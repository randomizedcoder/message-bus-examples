// JetStream durable RPC (§11 "NATS JetStream should be a separate test mode",
// §30). Where natsx.Client is ephemeral Core request/reply, this is the durable
// variant: the request is published into a replicated, file-backed JetStream
// stream (persisted and acked before Call proceeds), a durable pull consumer
// serves it, and if the responder crashes before it acks, JetStream redelivers
// — at-least-once, deduped to exactly-once *effect* by the idempotency cache
// (§29). The distinct semantics (persistence, consumer state, redelivery/replay)
// are exactly what §30 says not to hide behind one "NATS" number.
//
// The reply is correlated by request_id through rpc.Correlator and returned over
// a Core NATS inbox: the durability that matters for "did the operation happen"
// lives on the request + the idempotency cache, so the response leg stays cheap.
// This reuses the legacy JetStreamContext API (nc.JetStream()) that the vendored
// nats.go exposes, mirroring internal/transport/nats/jetstream.go — no new dep.
package natsx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	natstransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/nats"
)

const (
	// jsStreamName is the durable request stream; jsSubjectPrefix namespaces its
	// subjects apart from the Core path (rpc.*) and the workloads streams (wl.*).
	jsStreamName    = "RPC_REQUESTS"
	jsSubjectPrefix = "rpcjs"
	// jsReplicas keeps a copy on every JetStream server (the cluster runs 3), so a
	// persisted request survives one node loss with the Raft group still quorate.
	jsReplicas = 3
	// jsMaxAge bounds the stream so a benchmark's requests do not accumulate
	// forever; it is long enough that acked messages can still be replayed by a
	// fresh consumer within a run (the §30 replay property).
	jsMaxAge = 30 * time.Minute
	// jsDurable is the shared durable pull-consumer name: every responder replica
	// binds the same durable, so JetStream load-balances requests across them and
	// remembers ack state across restarts.
	jsDurable    = "RPC_RESPONDERS"
	jsAckWait    = 30 * time.Second
	jsFetchBatch = 256
	jsFetchWait  = time.Second
	// hdrReplyTo carries the client's Core NATS reply inbox on the persisted
	// request, so the responder knows where to send the (ephemeral) response.
	hdrReplyTo = "Rpc-Reply"
)

// JSSubjectWildcard is the stream/consumer subject filter; JSSubject is the
// per-service.method publish subject, so requests stay self-describing.
const JSSubjectWildcard = jsSubjectPrefix + ".>"

func JSSubject(service, method string) string {
	return jsSubjectPrefix + "." + service + "." + method
}

// ensureRPCStream idempotently creates the durable request stream. Like the
// workloads EnsureStream it is safe to call concurrently from every client and
// responder at startup: an AddStream that loses the create race is confirmed via
// StreamInfo rather than propagated.
func ensureRPCStream(js nats.JetStreamContext) error {
	if _, err := js.StreamInfo(jsStreamName); err == nil {
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      jsStreamName,
		Subjects:  []string{JSSubjectWildcard},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  jsReplicas,
		MaxAge:    jsMaxAge,
	})
	if err == nil {
		return nil
	}
	if _, e2 := js.StreamInfo(jsStreamName); e2 == nil {
		return nil
	}
	return err
}

// ─── JSClient ────────────────────────────────────────────────────────────────

// JSClient is the durable-JetStream twin of Client: it satisfies rpc.Client by
// publishing each request into the stream and correlating the reply (delivered
// over a Core NATS inbox) back by request_id.
type JSClient struct {
	nc    *nats.Conn
	js    nats.JetStreamContext
	inbox string
	sub   *nats.Subscription
	corr  *rpc.Correlator
	caps  rpc.Capabilities
}

// DialJetStream connects to addr, ensures the request stream, and subscribes to
// a unique Core inbox for replies.
func DialJetStream(addr string) (*JSClient, error) {
	nc, err := natstransport.Dial(natsURL(addr))
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}
	if err := ensureRPCStream(js); err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure %s stream: %w", jsStreamName, err)
	}
	c := &JSClient{
		nc:    nc,
		js:    js,
		inbox: nats.NewInbox(),
		corr:  rpc.NewCorrelator(),
		caps: rpc.Capabilities{
			NativeRequestReply: false, // request/reply is built over the stream + a correlator
			DurableRequests:    true,
			Replay:             true,
			AtLeastOnce:        true,
			Streaming:          false,
		},
	}
	sub, err := nc.Subscribe(c.inbox, c.onReply)
	if err != nil {
		nc.Close()
		return nil, err
	}
	c.sub = sub
	return c, nil
}

// onReply decodes a response from the inbox and hands it to the correlator; an
// undecodable or unmatched (late/orphan) reply is dropped.
func (c *JSClient) onReply(m *nats.Msg) {
	resp := &rpcv1.Response{}
	if err := proto.Unmarshal(m.Data, resp); err != nil {
		return
	}
	c.corr.Deliver(resp.GetRequestId(), rpc.Result{Response: resp})
}

// Call publishes req into the stream (blocking until JetStream acks it as
// persisted) and waits for the correlated response or ctx's deadline. A publish
// failure is mapped like the Core path; a deadline returns ctx.Err() (→ TIMEOUT).
func (c *JSClient) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	ch, cancel := c.corr.Register(req.GetRequestId())
	defer cancel()

	msg := nats.NewMsg(JSSubject(req.GetService(), req.GetMethod()))
	msg.Header.Set(hdrReplyTo, c.inbox)
	msg.Data = data
	if _, err := c.js.PublishMsg(msg, nats.Context(ctx)); err != nil {
		return nil, mapErr(err)
	}

	select {
	case res := <-ch:
		return res.Response, res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Capabilities reports what durable JetStream RPC provides (§34).
func (c *JSClient) Capabilities() rpc.Capabilities { return c.caps }

// Close unsubscribes, unblocks any outstanding Call, and closes the connection.
func (c *JSClient) Close() error {
	if c.sub != nil {
		_ = c.sub.Unsubscribe()
	}
	c.corr.Shutdown(errors.New("natsx: client closed"))
	c.nc.Close()
	return nil
}

// ─── JSResponder ─────────────────────────────────────────────────────────────

// JSResponder is a durable pull consumer that serves persisted requests with an
// rpc.Handler and publishes each response back over Core NATS to the request's
// reply inbox. It acks only after the reply is sent, so a crash mid-request
// leaves the message unacked and JetStream redelivers it (at-least-once).
type JSResponder struct {
	nc     *nats.Conn
	sub    *nats.Subscription
	cancel context.CancelFunc
	done   chan struct{}
}

// ServeJetStream connects to addr, ensures the stream, binds the durable pull
// consumer, and serves in the background until Close (or ctx) stops it.
func ServeJetStream(ctx context.Context, addr string, h rpc.Handler) (*JSResponder, error) {
	nc, err := natstransport.Dial(natsURL(addr))
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}
	if err := ensureRPCStream(js); err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure %s stream: %w", jsStreamName, err)
	}
	sub, err := js.PullSubscribe(JSSubjectWildcard, jsDurable,
		nats.BindStream(jsStreamName), nats.ManualAck(), nats.AckExplicit(), nats.AckWait(jsAckWait))
	if err != nil {
		nc.Close()
		return nil, err
	}
	rctx, cancel := context.WithCancel(ctx)
	r := &JSResponder{nc: nc, sub: sub, cancel: cancel, done: make(chan struct{})}
	go r.loop(rctx, h)
	return r, nil
}

func (r *JSResponder) loop(ctx context.Context, h rpc.Handler) {
	defer close(r.done)
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, err := r.sub.Fetch(jsFetchBatch, nats.MaxWait(jsFetchWait))
		if err != nil {
			// A quiet stream just times out with no messages — keep polling until
			// ctx is cancelled.
			if errors.Is(err, nats.ErrTimeout) || ctx.Err() != nil {
				continue
			}
			return
		}
		for _, m := range msgs {
			r.handle(ctx, m, h)
		}
	}
}

// handle decodes one delivery, runs the handler, publishes the response to the
// reply inbox, then acks. An undecodable delivery has no request to retry, so it
// is terminated (removed from redelivery) rather than left to redeliver forever.
func (r *JSResponder) handle(ctx context.Context, m *nats.Msg, h rpc.Handler) {
	req := &rpcv1.Request{}
	if err := proto.Unmarshal(m.Data, req); err != nil {
		_ = m.Term()
		return
	}
	resp, err := h.Handle(ctx, req)
	if err != nil {
		resp = rpc.ErrorResponse(req, err)
	}
	if replyTo := m.Header.Get(hdrReplyTo); replyTo != "" {
		if out, err := proto.Marshal(resp); err == nil {
			_ = r.nc.Publish(replyTo, out)
		}
	}
	_ = m.Ack()
}

// Close stops the serve loop, unsubscribes, and closes the connection.
func (r *JSResponder) Close() error {
	r.cancel()
	<-r.done
	if r.sub != nil {
		_ = r.sub.Unsubscribe()
	}
	r.nc.Close()
	return nil
}
