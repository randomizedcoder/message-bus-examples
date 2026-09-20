// Package natstransport is the NATS binding of the proto-bench transport
// interfaces (design §3.9, §7.5). P3 implements the request-reply tier-C flow:
// the driver's Requester sends a DeployRequest on `wl.<region>.deploy` and
// blocks for the reply; the agent's Responder serves that subject in queue
// group `agents`. The codec is carried in the `Content-Type` header so the
// responder decodes before it has seen the envelope.
//
// Buffer handling follows §7.5: nats.go copies the payload into its write
// buffer synchronously inside Publish/Request, so a pooled encode buffer is
// safe to return the moment the call returns; inbound `msg.Data` is owned by
// the *nats.Msg (already copied in processMsg), so we decode and drop it.
package natstransport

import (
	"context"
	"time"

	"buf.build/go/protovalidate"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// queueGroup load-balances Deploy requests across the region's agents (only one
// per region today, but the queue group is what makes it horizontally scalable
// and is the design's stated topology).
const queueGroup = "agents"

const contentType = "Content-Type"

// DeploySubject is the request-reply subject for a region (design §3.9).
func DeploySubject(region string) string { return "wl." + region + ".deploy" }

// Dial opens a NATS connection with the same resilient reconnect policy the
// pub/sub CLIs use, so a broker failover during a run reconnects rather than
// erroring the whole cell.
func Dial(url string) (*nats.Conn, error) {
	return nats.Connect(url,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
}

// ─── Requester (client) ─────────────────────────────────────────────────────

type requester struct {
	nc      *nats.Conn
	opts    transport.Options
	subject string
}

// NewRequester builds a request-reply client targeting region's deploy subject.
func NewRequester(nc *nats.Conn, opts transport.Options, region string) transport.Requester {
	return &requester{nc: nc, opts: opts, subject: DeploySubject(region)}
}

func (r *requester) Request(ctx context.Context, req, resp proto.Message) error {
	buf, err := codec.Encode(r.opts.Codec, r.opts.Pool, req)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(r.subject)
	msg.Header.Set(contentType, codec.ContentType(r.opts.Codec))
	msg.Data = *buf
	reply, err := r.nc.RequestMsgWithContext(ctx, msg)
	r.opts.Pool.Put(buf) // synchronously copied by publish (§7.5)
	if err != nil {
		return err
	}
	return r.opts.Codec.Unmarshal(reply.Data, resp)
}

func (r *requester) Close() error { r.nc.Close(); return nil }

// ─── Responder (server) ──────────────────────────────────────────────────────

type responder struct {
	nc          *nats.Conn
	opts        transport.Options
	responderID string
	subject     string
	newReq      func() proto.Message
	sub         *nats.Subscription
}

// NewResponder serves region's deploy subject. newReq builds a fresh request
// message to decode each delivery into (the subject fixes the type — design
// §3.9); h is the pure application logic (req → resp), the responder owns the
// decode / receive-stamp / validate / send-stamp / encode around it.
func NewResponder(nc *nats.Conn, opts transport.Options, responderID, region string, newReq func() proto.Message) transport.Responder {
	return &responder{nc: nc, opts: opts, responderID: responderID, subject: DeploySubject(region), newReq: newReq}
}

func (s *responder) Serve(ctx context.Context, h transport.Handler) error {
	sub, err := s.nc.QueueSubscribe(s.subject, queueGroup, func(m *nats.Msg) {
		s.reply(ctx, m, h)
	})
	if err != nil {
		return err
	}
	s.sub = sub
	<-ctx.Done()
	return nil
}

// reply decodes one delivery, runs the handler, and publishes the encoded
// response back on the NATS-supplied inbox (m.Respond).
func (s *responder) reply(ctx context.Context, m *nats.Msg, h transport.Handler) {
	cdc := codec.ByContentType(m.Header.Get(contentType))
	req := s.newReq()
	if err := cdc.Unmarshal(m.Data, req); err != nil {
		return // a decode failure has no envelope to correlate; drop (counted as a timeout on the driver)
	}
	if env := envelope.Of(req); env != nil {
		envelope.StampReceive(env, s.responderID, m.Data, s.opts.Integrity)
	}
	if s.opts.Validate {
		if err := protovalidate.Validate(req); err != nil {
			return
		}
	}
	resp, err := h(ctx, req)
	if err != nil {
		return
	}
	if env := envelope.Of(resp); env != nil {
		envelope.StampSend(env)
	}
	out, err := codec.Encode(cdc, s.opts.Pool, resp)
	if err != nil {
		return
	}
	_ = m.Respond(*out)
	s.opts.Pool.Put(out) // Respond copied synchronously (§7.5)
}

func (s *responder) Close() error {
	if s.sub != nil {
		_ = s.sub.Unsubscribe()
	}
	return nil
}
