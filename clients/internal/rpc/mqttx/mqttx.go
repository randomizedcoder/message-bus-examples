// Package mqttx is the MQTT (Mosquitto) request/reply binding for the RPC lab's
// routing envelope (§13): a GatewayService client that satisfies rpc.Client and
// a responder helper that backs an rpc.Handler, over MQTT. It is the broker twin
// of natsx/rabbitmqx — same rpc.Client / rpc.Handler seams, a different wire.
//
// MQTT is natively one-way publish/subscribe, so request/reply is synthesized
// (§13): the client publishes the marshaled envelope to a per-method request
// topic `rpc/request/<service>/<method>` and subscribes to its own response
// topic `rpc/response/<client-id>`; the responder subscribes to the request
// wildcard `rpc/request/+/+`, dispatches, and publishes the response to
// `rpc/response/<client-id>`. Replies arrive out of band, so the client uses
// rpc.Correlator to match a reply back to the waiting Call by request_id.
//
// The vendored paho is v1.5.0 (MQTT 3.1.1): there is NO per-message v5
// ResponseTopic/CorrelationData property, so both the reply route and the
// correlation id travel in the rpc.v1 envelope itself — the reply topic is
// derived from Request.client_id (stamped by Call), and the correlation is
// Request/Response.request_id. No proto change is needed.
//
// QoS is a standalone, selectable byte (0, 1, or 2) on both Dial and Serve and
// is NEVER folded into a mode: QoS 0/1/2 are not equivalent semantics (§13). A
// QoS-2 exactly-once guarantee holds only within a single broker; across the
// bridged 3-broker mesh (MQTT 3.1.1 bridges) it is not preserved end to end —
// sessionAffinity: ClientIP co-locates a client's request publish and its reply
// subscription on one broker, keeping the round trip broker-local.
//
// Like natsx/rabbitmqx it marshals the whole rpc.v1 envelope with proto and
// pulls in only paho + rpc.* — not the proto-bench codec/envelope/transport
// machinery.
package mqttx

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
)

const (
	// reqTopicPrefix and respTopicPrefix namespace every RPC-lab topic so they
	// never collide with the proto-bench telemetry topics (wl/<region>/…) on the
	// same broker.
	reqTopicPrefix  = "rpc/request"  // rpc/request/<service>/<method>
	respTopicPrefix = "rpc/response" // rpc/response/<client-id>
	// reqWildcard is the single-level (+) request wildcard a responder subscribes
	// to so one subscription serves every service.method.
	reqWildcard = "rpc/request/+/+"

	connectTimeout   = 10 * time.Second
	subscribeTimeout = 10 * time.Second
	disconnectMS     = 250
)

// clientSeq disambiguates client ids within one process: the pid alone is not
// unique when a program dials more than one mqttx.Client (benchmark, tests), and
// a shared client id would cross-wire the two clients' reply topics.
var clientSeq atomic.Uint64

// RequestTopic is the request topic for a service.method: a per-method topic
// lets the wire stay self-describing and mirrors natsx's per-method subject.
func RequestTopic(service, method string) string {
	return reqTopicPrefix + "/" + service + "/" + method
}

// ResponseTopic is the reply topic for a client id. The responder derives it
// from the request's client_id, so each client receives only its own replies.
func ResponseTopic(clientID string) string {
	return respTopicPrefix + "/" + clientID
}

// brokerURL accepts either a bare host:port or a full scheme URL (tcp://…,
// ssl://…) and returns something paho's AddBroker understands, so callers can
// pass the same -addr they would to a NodePort (e.g. 10.33.33.10:30883).
func brokerURL(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "tcp://" + addr
}

// connect dials a paho client at addr with a unique client id and the resilient
// reconnect policy the pub/sub CLIs use.
func connect(addr, clientID string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL(addr)).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		// Dispatch every incoming message on its own goroutine rather than on
		// paho's single router goroutine. With QoS 2, that router goroutine also
		// drives the inbound receive handshake (PUBREC/PUBREL/PUBCOMP); if a
		// handler runs on it and touches the client (e.g. the responder replying,
		// or a reused client's next flow), the protocol can no longer advance and
		// request/reply wedges after the first exchange. Ordering does not matter
		// here — each reply is correlated by request_id — so this is safe.
		SetOrderMatters(false)
	c := mqtt.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(connectTimeout) || tok.Error() != nil {
		return nil, fmt.Errorf("mqtt connect %s: %w", addr, tok.Error())
	}
	return c, nil
}

// ─── Client ──────────────────────────────────────────────────────────────────

// Client is a GatewayService MQTT client that satisfies rpc.Client. It owns one
// paho connection, subscribes to its own response topic, and correlates
// out-of-band replies by request_id.
type Client struct {
	c        mqtt.Client
	corr     *rpc.Correlator
	clientID string
	qos      byte
	caps     rpc.Capabilities
}

// Dial connects to a broker at addr (host:port or scheme URL), subscribes to a
// per-client response topic at qos, and returns a Client ready for Call. qos is
// the MQTT QoS (0, 1, or 2) used for both the request publish and the response
// subscription; QoS 0/1/2 are distinct semantics (§13), never conflated.
func Dial(addr string, qos byte) (*Client, error) {
	clientID := fmt.Sprintf("rpc-mqttx-%d-%d", os.Getpid(), clientSeq.Add(1))
	conn, err := connect(addr, clientID)
	if err != nil {
		return nil, err
	}
	c := &Client{
		c:        conn,
		corr:     rpc.NewCorrelator(),
		clientID: clientID,
		qos:      qos,
		caps: rpc.Capabilities{
			// Request/reply is synthesized from pub/sub + a correlator, and MQTT
			// gives no "no responder" signal — a request to an unserved method is
			// simply never answered, so the caller learns it only at timeout.
			NativeRequestReply: false,
			ServerDiscovery:    false,
			Streaming:          false,
			// QoS>=1 is at-least-once (the bridge may also duplicate, §13), so
			// idempotency matters; QoS 0 is at-most-once.
			AtLeastOnce: qos >= 1,
		},
	}
	// Subscribe to this client's response topic before any Call publishes, so no
	// reply can race ahead of the subscription.
	tok := conn.Subscribe(ResponseTopic(clientID), qos, func(_ mqtt.Client, m mqtt.Message) {
		c.deliver(m.Payload())
	})
	if !tok.WaitTimeout(subscribeTimeout) || tok.Error() != nil {
		conn.Disconnect(disconnectMS)
		return nil, fmt.Errorf("mqtt subscribe %s: %w", ResponseTopic(clientID), tok.Error())
	}
	return c, nil
}

// deliver routes one reply to the waiting Call by request_id. MQTT 3.1.1 carries
// no correlation metadata, so the id comes from the envelope's request_id.
func (c *Client) deliver(body []byte) {
	resp := &rpcv1.Response{}
	if err := proto.Unmarshal(body, resp); err != nil {
		return
	}
	c.corr.Deliver(resp.GetRequestId(), rpc.Result{Response: resp})
}

// Call stamps the reply-route client id onto req, publishes it to the request
// topic at the client's QoS, and waits for the correlated reply or ctx's
// deadline. A publish failure maps to ErrUnavailable → STATUS_UNAVAILABLE.
func (c *Client) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	// The responder has no v5 ResponseTopic to reply on, so it must learn the
	// reply route from the envelope: stamp this client's id before marshaling.
	req.ClientId = c.clientID
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	id := req.GetRequestId()
	ch, cancel := c.corr.Register(id)
	defer cancel()

	tok := c.c.Publish(RequestTopic(req.GetService(), req.GetMethod()), c.qos, false, data)
	// Wait for the publish to be accepted (PUBACK for QoS>0) before selecting on
	// the reply, so a broker rejection surfaces as UNAVAILABLE, not a timeout.
	select {
	case <-tok.Done():
		if tok.Error() != nil {
			return nil, fmt.Errorf("%w: publish: %v", rpc.ErrUnavailable, tok.Error())
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-ch:
		return res.Response, res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Capabilities reports what this MQTT QoS provides (§34).
func (c *Client) Capabilities() rpc.Capabilities { return c.caps }

// Close unblocks any outstanding Call, unsubscribes, and disconnects.
func (c *Client) Close() error {
	c.corr.Shutdown(fmt.Errorf("%w: client closed", rpc.ErrUnavailable))
	c.c.Unsubscribe(ResponseTopic(c.clientID))
	c.c.Disconnect(disconnectMS)
	return nil
}

// ─── Responder ─────────────────────────────────────────────────────────────

// Responder subscribes to the request wildcard, backs each request with an
// rpc.Handler, and publishes the response to the reply topic derived from the
// request's client_id. It is the MQTT twin of natsx.Serve / rabbitmqx.Serve.
type Responder struct {
	c   mqtt.Client
	qos byte
}

// Serve connects to addr and subscribes to reqWildcard at qos, dispatching each
// request to h. It returns once the subscription is established (delivery is
// asynchronous); Close stops it. qos governs both the request subscription and
// the reply publish.
func Serve(ctx context.Context, addr string, qos byte, h rpc.Handler) (*Responder, error) {
	clientID := fmt.Sprintf("rpc-mqttx-responder-%d-%d", os.Getpid(), clientSeq.Add(1))
	conn, err := connect(addr, clientID)
	if err != nil {
		return nil, err
	}
	r := &Responder{c: conn, qos: qos}
	// handle publishes the reply at QoS 2, which must NOT run on paho's message
	// pump goroutine (it owes that flow's PUBCOMP); SetOrderMatters(false) in
	// connect() guarantees each incoming message is dispatched on its own
	// goroutine, so replying from the callback here is safe.
	tok := conn.Subscribe(reqWildcard, qos, func(_ mqtt.Client, m mqtt.Message) {
		r.handle(ctx, m.Payload(), h)
	})
	if !tok.WaitTimeout(subscribeTimeout) || tok.Error() != nil {
		conn.Disconnect(disconnectMS)
		return nil, fmt.Errorf("mqtt subscribe %s: %w", reqWildcard, tok.Error())
	}
	return r, nil
}

// handle decodes one request, runs the handler, and publishes the response to
// the reply topic for the request's client_id. A handler error becomes an
// ErrorResponse so the caller always gets a Status; a delivery that cannot be
// decoded or that carries no client_id has nowhere to reply, so it is dropped
// (the caller sees a timeout).
func (r *Responder) handle(ctx context.Context, body []byte, h rpc.Handler) {
	req := &rpcv1.Request{}
	if err := proto.Unmarshal(body, req); err != nil {
		return
	}
	clientID := req.GetClientId()
	if clientID == "" {
		return
	}
	resp, err := h.Handle(ctx, req)
	if err != nil {
		resp = rpc.ErrorResponse(req, err)
	}
	out, err := proto.Marshal(resp)
	if err != nil {
		return
	}
	r.c.Publish(ResponseTopic(clientID), r.qos, false, out)
}

// Close unsubscribes and disconnects.
func (r *Responder) Close() error {
	r.c.Unsubscribe(reqWildcard)
	r.c.Disconnect(disconnectMS)
	return nil
}
