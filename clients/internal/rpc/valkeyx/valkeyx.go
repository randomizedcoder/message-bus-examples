// Package valkeyx is the Valkey (Redis-compatible) request/reply binding for the
// RPC lab's routing envelope: a GatewayService client that satisfies rpc.Client
// and a responder helper that backs an rpc.Handler, over Valkey. It is the broker
// twin of natsx/rabbitmqx/mqttx — same rpc.Client / rpc.Handler seams, a
// different wire — and offers two modes as a natsx-style pair:
//
//   - Pub/Sub (this file): the ephemeral path, conceptually closest to NATS Core.
//     The client PUBLISHes the marshaled envelope to a per-method request channel
//     and SUBSCRIBEs to its own reply channel; the responder PSUBSCRIBEs the
//     request pattern and PUBLISHes each reply to the caller's reply channel. A
//     publish to a channel with no subscriber is silently dropped (Valkey Pub/Sub
//     has no "no responder" signal), so an unserved request is only learned at the
//     caller's timeout.
//   - Streams (stream.go): the durable path, conceptually closest to JetStream —
//     XADD to a request stream consumed by a durable group, replies XADDed to a
//     per-client reply stream.
//
// Valkey Pub/Sub, like MQTT, carries no per-message reply-address or correlation
// metadata, so both travel in the rpc.v1 envelope itself: the reply channel is
// derived from Request.client_id (stamped by Call) and correlation is
// Request/Response.request_id, matched by rpc.Correlator. No proto change.
//
// Writes and PUBLISH must reach the primary, so the binding always connects via
// Sentinels (redis.NewFailoverClient): the round-robin client NodePort can land
// on a read-only replica. It marshals the whole envelope with proto and imports
// only go-redis + rpc.* — not the proto-bench codec/envelope/transport machinery.
package valkeyx

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
)

const (
	// masterName is the Sentinel-monitored primary's name; it matches the Valkey
	// manifests (sentinel monitor mymaster …).
	masterName = "mymaster"

	// reqChanPrefix / respChanPrefix namespace every RPC-lab Pub/Sub channel under
	// an rpc: prefix so they never collide with the workloads wl:<region>:* keys.
	reqChanPrefix  = "rpc:request"  // rpc:request:<service>:<method>
	respChanPrefix = "rpc:response" // rpc:response:<client-id>
	// reqPattern is the glob a responder PSUBSCRIBEs so one subscription serves
	// every service.method.
	reqPattern = "rpc:request:*"

	pingTimeout = 10 * time.Second
)

// clientSeq disambiguates client ids within one process: the pid alone is not
// unique when a program dials more than one Valkey client, and a shared client id
// would cross-wire the two clients' reply channels/streams.
var clientSeq atomic.Uint64

// RequestChannel is the Pub/Sub channel for a service.method. A per-method
// channel keeps the wire self-describing and mirrors natsx's per-method subject.
func RequestChannel(service, method string) string {
	return reqChanPrefix + ":" + service + ":" + method
}

// ResponseChannel is the reply channel for a client id. The responder derives it
// from the request's client_id, so each client receives only its own replies.
func ResponseChannel(clientID string) string {
	return respChanPrefix + ":" + clientID
}

// splitSentinels turns a comma-separated Sentinel list into the slice
// NewFailoverClient wants, trimming spaces and dropping empties.
func splitSentinels(addr string) []string {
	parts := strings.Split(addr, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// newClientID mints a process-unique id used as this client's reply-route
// segment; prefix distinguishes Pub/Sub from Streams clients in logs.
func newClientID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), clientSeq.Add(1))
}

// dial builds a Sentinel-backed client (always the primary, so writes/PUBLISH
// land) and verifies it is reachable with a PING.
func dial(sentinels, pass string) (*redis.Client, error) {
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:      masterName,
		SentinelAddrs:   splitSentinels(sentinels),
		Password:        pass,
		MaxRetries:      -1,
		MinRetryBackoff: 200 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("%w: valkey connect: %v", rpc.ErrUnavailable, err)
	}
	return rdb, nil
}

// ─── Client (Pub/Sub) ────────────────────────────────────────────────────────

// Client is a GatewayService Valkey Pub/Sub client that satisfies rpc.Client. It
// owns one connection pool, subscribes to its own reply channel, and correlates
// out-of-band replies by request_id.
type Client struct {
	rdb      *redis.Client
	sub      *redis.PubSub
	corr     *rpc.Correlator
	clientID string
	caps     rpc.Capabilities
}

// Dial connects to Valkey via the given Sentinels (comma-separated host:port
// list), subscribes to a per-client reply channel, and returns a Client ready for
// Call. pass is the primary's password (empty for none).
func Dial(sentinels, pass string) (*Client, error) {
	rdb, err := dial(sentinels, pass)
	if err != nil {
		return nil, err
	}
	clientID := newClientID("rpc-valkeyx")
	sub := rdb.Subscribe(context.Background(), ResponseChannel(clientID))
	// Block until the SUBSCRIBE is acknowledged, so no reply can race ahead of the
	// subscription (Receive returns the *Subscription confirmation first).
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		_ = rdb.Close()
		return nil, fmt.Errorf("%w: valkey subscribe: %v", rpc.ErrUnavailable, err)
	}
	c := &Client{
		rdb:      rdb,
		sub:      sub,
		corr:     rpc.NewCorrelator(),
		clientID: clientID,
		caps: rpc.Capabilities{
			// Request/reply is synthesized from Pub/Sub + a correlator; a publish to
			// a channel with no subscriber is dropped, so there is no discovery
			// signal and no durability — an unserved request times out.
			NativeRequestReply: false,
			ServerDiscovery:    false,
			DurableRequests:    false,
			Streaming:          false,
			AtLeastOnce:        false,
		},
	}
	go c.dispatch(sub.Channel())
	return c, nil
}

// dispatch routes each reply to the waiting Call by request_id.
func (c *Client) dispatch(ch <-chan *redis.Message) {
	for m := range ch {
		resp := &rpcv1.Response{}
		if err := proto.Unmarshal([]byte(m.Payload), resp); err != nil {
			continue
		}
		c.corr.Deliver(resp.GetRequestId(), rpc.Result{Response: resp})
	}
}

// Call stamps the reply-route client id onto req, publishes it to the request
// channel, and waits for the correlated reply or ctx's deadline. A publish
// failure maps to ErrUnavailable → STATUS_UNAVAILABLE.
func (c *Client) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	// The responder learns the reply channel from the envelope, so stamp this
	// client's id before marshaling.
	req.ClientId = c.clientID
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	id := req.GetRequestId()
	ch, cancel := c.corr.Register(id)
	defer cancel()

	if err := c.rdb.Publish(ctx, RequestChannel(req.GetService(), req.GetMethod()), data).Err(); err != nil {
		return nil, fmt.Errorf("%w: publish: %v", rpc.ErrUnavailable, err)
	}

	select {
	case res := <-ch:
		return res.Response, res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Capabilities reports what Valkey Pub/Sub provides (§34).
func (c *Client) Capabilities() rpc.Capabilities { return c.caps }

// Close unblocks any outstanding Call, then closes the subscription and pool.
func (c *Client) Close() error {
	c.corr.Shutdown(fmt.Errorf("%w: client closed", rpc.ErrUnavailable))
	if c.sub != nil {
		_ = c.sub.Close()
	}
	if c.rdb != nil {
		_ = c.rdb.Close()
	}
	return nil
}

// ─── Responder (Pub/Sub) ─────────────────────────────────────────────────────

// Responder PSUBSCRIBEs the request pattern, backs each request with an
// rpc.Handler, and publishes the response to the reply channel derived from the
// request's client_id. It is the Valkey twin of natsx.Serve / mqttx.Serve.
type Responder struct {
	rdb    *redis.Client
	sub    *redis.PubSub
	cancel context.CancelFunc
	done   chan struct{}
}

// Serve connects to Valkey via the given Sentinels and serves GatewayService
// requests on reqPattern, dispatching each to h. It returns once the
// subscription is acknowledged; Close stops it.
func Serve(ctx context.Context, sentinels, pass string, h rpc.Handler) (*Responder, error) {
	rdb, err := dial(sentinels, pass)
	if err != nil {
		return nil, err
	}
	psub := rdb.PSubscribe(context.Background(), reqPattern)
	rctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if _, err := psub.Receive(rctx); err != nil {
		_ = psub.Close()
		_ = rdb.Close()
		return nil, fmt.Errorf("%w: valkey psubscribe: %v", rpc.ErrUnavailable, err)
	}
	sctx, scancel := context.WithCancel(ctx)
	r := &Responder{rdb: rdb, sub: psub, cancel: scancel, done: make(chan struct{})}
	go r.loop(sctx, psub.Channel(), h)
	return r, nil
}

// loop dispatches each request on its own goroutine so a slow handler does not
// head-of-line-block the delivery channel; go-redis is safe for concurrent use.
func (r *Responder) loop(ctx context.Context, ch <-chan *redis.Message, h rpc.Handler) {
	defer close(r.done)
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			payload := []byte(m.Payload)
			go r.handle(ctx, payload, h)
		}
	}
}

// handle decodes one request, runs the handler, and publishes the response to
// the reply channel for the request's client_id. A delivery that cannot be
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
	_ = r.rdb.Publish(ctx, ResponseChannel(clientID), out).Err()
}

// Close stops the serve loop and closes the subscription and pool.
func (r *Responder) Close() error {
	r.cancel()
	<-r.done
	if r.sub != nil {
		_ = r.sub.Close()
	}
	if r.rdb != nil {
		_ = r.rdb.Close()
	}
	return nil
}
