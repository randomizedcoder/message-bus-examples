// Package natsx is the NATS Core request/reply binding for the RPC lab's routing
// envelope (§11): a GatewayService client that satisfies rpc.Client by turning
// each Call into a native NATS request, and a responder helper that backs an
// rpc.Handler by queue-subscribing for those requests. It is the transport twin
// of grpcx — same rpc.Client / rpc.Handler seams, a different wire.
//
// NATS Core needs no correlator: the reply travels back on the NATS-managed
// _INBOX subject, so the connection matches the response to the request for us.
// It also gives the RPC lab its most useful failure signal (§28): when no server
// is subscribed, nats-server answers immediately with a no-responders sentinel,
// so the caller learns the request cannot be serviced without waiting out the
// application timeout. mapErr turns that into rpc.ErrNoResponder → STATUS_UNAVAILABLE.
//
// The whole rpc.v1 Request/Response envelope is marshaled to bytes with proto;
// the inner payload Any is already packed by rpc.NewRequest. Both ends speak
// proto, so no Content-Type negotiation is needed here (codec sweeps come later).
package natsx

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	natstransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/nats"
)

// subjectPrefix namespaces every RPC-lab subject so it never collides with the
// proto-bench workloads subjects (wl.<region>.*) on the same broker.
const subjectPrefix = "rpc"

// SubjectWildcard is the token-wildcard a responder subscribes to so one
// subscription serves every service.method. The gateway/service dispatches by
// the envelope's service.method, mirroring the payload-blind gRPC router.
const SubjectWildcard = subjectPrefix + ".>"

// queueGroup load-balances requests across responders subscribed to the same
// subject (the design's horizontal-scale topology; one replica works too).
const queueGroup = "rpc-responders"

// Subject is the request subject for a service.method: rpc.<service>.<method>.
// A per-method subject lets NATS queue-group-balance backends independently and
// keeps the wire self-describing for tracing.
func Subject(service, method string) string {
	return subjectPrefix + "." + service + "." + method
}

// natsURL accepts either a bare host:port or a full nats://… URL and returns a
// URL nats.Connect understands, so callers can pass the same -addr they would to
// a NodePort (e.g. 10.33.33.10:30422).
func natsURL(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "nats://" + addr
}

// mapErr translates a NATS request error into the RPC lab's sentinels so
// rpc.StatusOf yields the right wire Status. The no-responders case is the §28
// fast-fail (→ STATUS_UNAVAILABLE); a NATS timeout is folded onto
// context.DeadlineExceeded (→ STATUS_TIMEOUT). Anything else — including a ctx
// deadline the caller already carries — passes through unchanged.
func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrNoResponders):
		return fmt.Errorf("%w: %v", rpc.ErrNoResponder, err)
	case errors.Is(err, nats.ErrTimeout):
		return fmt.Errorf("%w: %v", context.DeadlineExceeded, err)
	default:
		return err
	}
}

// ─── Client ──────────────────────────────────────────────────────────────────

// Client is a GatewayService NATS-Core client that satisfies rpc.Client.
type Client struct {
	nc   *nats.Conn
	caps rpc.Capabilities
}

// Dial connects to a NATS broker at addr (host:port or nats://…) with the same
// resilient reconnect policy the pub/sub CLIs use.
func Dial(addr string) (*Client, error) {
	nc, err := natstransport.Dial(natsURL(addr))
	if err != nil {
		return nil, err
	}
	return &Client{
		nc: nc,
		caps: rpc.Capabilities{
			NativeRequestReply: true,
			// NATS core reports "no responder" without waiting for the app
			// timeout (§28) — this is the discovery signal grpc lacks.
			ServerDiscovery: true,
			Streaming:       false,
			AtLeastOnce:     false,
		},
	}, nil
}

// Call issues req as a NATS request on its service.method subject and returns
// the decoded Response, respecting ctx's deadline. A no-responder or NATS
// timeout is mapped to an rpc sentinel (see mapErr) so the caller sees the same
// Status it would from any other transport.
func (c *Client) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	reply, err := c.nc.RequestWithContext(ctx, Subject(req.GetService(), req.GetMethod()), data)
	if err != nil {
		return nil, mapErr(err)
	}
	resp := &rpcv1.Response{}
	if err := proto.Unmarshal(reply.Data, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// Capabilities reports what NATS Core provides (§34).
func (c *Client) Capabilities() rpc.Capabilities { return c.caps }

// Close drains and closes the underlying connection.
func (c *Client) Close() error { c.nc.Close(); return nil }

// ─── Responder ─────────────────────────────────────────────────────────────

// Responder queue-subscribes for GatewayService requests and backs them with an
// rpc.Handler, replying on the NATS-supplied inbox. It is the NATS twin of
// grpcx.RegisterServer; a service or a gateway-B uses it to serve over NATS.
type Responder struct {
	nc  *nats.Conn
	sub *nats.Subscription
}

// Serve connects to addr and starts serving GatewayService requests on
// SubjectWildcard in queue group queueGroup, dispatching each to h. It returns
// once the subscription is established (delivery is asynchronous); Close stops
// it. group defaults to queueGroup when empty.
func Serve(ctx context.Context, addr string, h rpc.Handler) (*Responder, error) {
	nc, err := natstransport.Dial(natsURL(addr))
	if err != nil {
		return nil, err
	}
	r := &Responder{nc: nc}
	sub, err := nc.QueueSubscribe(SubjectWildcard, queueGroup, func(m *nats.Msg) {
		r.handle(ctx, m, h)
	})
	if err != nil {
		nc.Close()
		return nil, err
	}
	r.sub = sub
	return r, nil
}

// handle decodes one request, runs the handler, and publishes the encoded
// Response on the reply inbox. A handler error becomes an ErrorResponse so the
// caller always gets a Status; an undecodable delivery has no envelope to
// correlate, so it is dropped (the caller sees it as a timeout).
func (r *Responder) handle(ctx context.Context, m *nats.Msg, h rpc.Handler) {
	req := &rpcv1.Request{}
	if err := proto.Unmarshal(m.Data, req); err != nil {
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
	_ = m.Respond(out)
}

// Close unsubscribes and closes the connection.
func (r *Responder) Close() error {
	if r.sub != nil {
		_ = r.sub.Unsubscribe()
	}
	r.nc.Close()
	return nil
}
