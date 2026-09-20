// JetStream is the NATS durable, at-least-once telemetry binding (design §2.3
// tier B, §3.9). The driver's Publisher publishes TelemetrySample on
// `wl.<region>.telemetry`; JetStream persists it to the R3, file-backed stream
// `WL_TELEMETRY` and returns a PubAck (the message is replicated before the
// publish returns). Each region-agent runs a durable pull consumer filtered to
// its own region, acks every delivery explicitly, and counts redeliveries
// (NumDelivered > 1) — the tier-B correctness signal: loss must be ~0, and
// duplicates/redeliveries are reported, not hidden (design §8.4).
//
// The legacy JetStreamContext API (nc.JetStream()) is used because that is what
// nats.go 1.39.1 vendors under the root package; the newer jetstream/ package is
// not vendored. Buffer handling follows §7.5: nats.go copies the payload into
// its write buffer synchronously inside PublishMsg, so a pooled encode buffer is
// safe to return the moment the call returns; inbound msg.Data is owned by the
// *nats.Msg, so we decode and drop it.
package natstransport

import (
	"context"
	"errors"
	"time"

	"buf.build/go/protovalidate"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// StreamName is the JetStream stream carrying telemetry + usage for every region
// (design §3.9). One stream, subject-filtered per region by the consumers.
const StreamName = "WL_TELEMETRY"

// streamReplicas is the stream's replication factor. The cluster runs 3 JetStream
// servers, so R3 keeps a copy on every server (survives one node loss with the
// Raft group still quorate) — the tier-B "acked and replicated before ack"
// guarantee (design §2.3).
const streamReplicas = 3

// Fetch tuning for the pull consumer: pull up to fetchBatch messages per call,
// waiting at most fetchWait for the first one, so a quiet region wakes promptly
// on ctx cancellation instead of blocking a whole batch window.
const (
	fetchBatch = 256
	fetchWait  = time.Second
)

// TelemetrySubject is the JetStream publish/consume subject for a region.
func TelemetrySubject(region string) string { return "wl." + region + ".telemetry" }

// durableName is the per-region durable pull-consumer name (design §3.9: a
// durable pull consumer per agent). Kept DNS/JetStream-safe by using the region
// verbatim (regions are already `^[a-z]{2}-[a-z]+-[0-9]$`).
func durableName(region string) string { return "AGENT_" + region }

// JetStream opens a JetStreamContext on nc. It does not create the stream; call
// EnsureStream once from a server before publishing/consuming.
func JetStream(nc *nats.Conn) (nats.JetStreamContext, error) { return nc.JetStream() }

// EnsureStream idempotently creates the WL_TELEMETRY stream (R3, file-backed,
// subjects wl.*.telemetry + wl.*.usage). It is safe to call concurrently from
// every region-agent at startup: an already-existing stream is treated as
// success (AddStream races lose to "stream name already in use", which we then
// confirm via StreamInfo rather than propagate).
func EnsureStream(js nats.JetStreamContext) error {
	if _, err := js.StreamInfo(StreamName); err == nil {
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      StreamName,
		Subjects:  []string{"wl.*.telemetry", "wl.*.usage"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  streamReplicas,
	})
	if err == nil {
		return nil
	}
	// Lost a create race with another agent: the stream now exists → success.
	if _, e2 := js.StreamInfo(StreamName); e2 == nil {
		return nil
	}
	return err
}

// ─── Publisher (client, durable one-way) ─────────────────────────────────────

type jsPublisher struct {
	js      nats.JetStreamContext
	opts    transport.Options
	subject string
}

// NewPublisher builds a JetStream telemetry publisher for region. Each Publish
// blocks until JetStream acks the message as persisted (design §2.3 tier B).
func NewPublisher(js nats.JetStreamContext, opts transport.Options, region string) transport.Publisher {
	return &jsPublisher{js: js, opts: opts, subject: TelemetrySubject(region)}
}

func (p *jsPublisher) Publish(ctx context.Context, m proto.Message) (transport.Release, error) {
	buf, err := codec.Encode(p.opts.Codec, p.opts.Pool, m)
	if err != nil {
		return nil, err
	}
	msg := nats.NewMsg(p.subject)
	msg.Header.Set(contentType, codec.ContentType(p.opts.Codec))
	msg.Data = *buf
	_, err = p.js.PublishMsg(msg, nats.Context(ctx))
	p.opts.Pool.Put(buf) // nats.go copied the payload synchronously (§7.5)
	return func() {}, err
}

func (p *jsPublisher) Close() error { return nil } // the *nats.Conn is owned by the caller

// ─── Consumer (server, durable pull) ─────────────────────────────────────────

// jsMsg is one JetStream delivery. Codec comes from the Content-Type header so
// the consumer decodes before it has seen the envelope; NumDelivered exposes the
// redelivery count so the agent can report tier-B redeliveries (design §8.4).
type jsMsg struct{ m *nats.Msg }

func (j *jsMsg) Bytes() []byte      { return j.m.Data }
func (j *jsMsg) Ack() error         { return j.m.Ack() }
func (j *jsMsg) Codec() codec.Codec { return codec.ByContentType(j.m.Header.Get(contentType)) }

// NumDelivered reports how many times JetStream has delivered this message; 1 on
// the first delivery, >1 on a redelivery. A missing/garbled metadata reply is
// treated as a first delivery so a metadata hiccup never inflates the count.
func (j *jsMsg) NumDelivered() uint64 {
	md, err := j.m.Metadata()
	if err != nil || md.NumDelivered == 0 {
		return 1
	}
	return md.NumDelivered
}

// Redelivered reports whether m is a JetStream redelivery (NumDelivered > 1). It
// is false for any Msg that is not a JetStream delivery, so the agent's consume
// loop can call it uniformly.
func Redelivered(m transport.Msg) bool {
	if jm, ok := m.(interface{ NumDelivered() uint64 }); ok {
		return jm.NumDelivered() > 1
	}
	return false
}

type jsConsumer struct {
	js      nats.JetStreamContext
	opts    transport.Options
	region  string
	subject string
	sub     *nats.Subscription
}

// NewConsumer builds a durable pull consumer for region's telemetry subject. The
// caller must have run EnsureStream first.
func NewConsumer(js nats.JetStreamContext, opts transport.Options, region string) transport.Consumer {
	return &jsConsumer{js: js, opts: opts, region: region, subject: TelemetrySubject(region)}
}

func (c *jsConsumer) Consume(ctx context.Context, fn func(transport.Msg) error) error {
	sub, err := c.js.PullSubscribe(c.subject, durableName(c.region),
		nats.BindStream(StreamName), nats.ManualAck(), nats.AckExplicit())
	if err != nil {
		return err
	}
	c.sub = sub
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := sub.Fetch(fetchBatch, nats.MaxWait(fetchWait))
		if err != nil {
			// A quiet region just times out with no messages — keep polling.
			if errors.Is(err, nats.ErrTimeout) || ctx.Err() != nil {
				continue
			}
			return err
		}
		for _, m := range msgs {
			_ = fn(&jsMsg{m: m})
		}
	}
}

func (c *jsConsumer) Close() error {
	if c.sub != nil {
		return c.sub.Unsubscribe()
	}
	return nil
}

// Decode turns a delivered JetStream Msg into a request message, receive-stamps
// it, and optionally validates — the one-way analogue of a Responder's decode
// half (there is no reply to send). It mirrors the MQTT consumer helper so the
// agent's telemetry consume loop is transport-neutral.
func Decode(m transport.Msg, newMsg func() proto.Message, responderID string, opts transport.Options) (proto.Message, error) {
	req := newMsg()
	if err := m.Codec().Unmarshal(m.Bytes(), req); err != nil {
		return nil, err
	}
	if env := envelope.Of(req); env != nil {
		envelope.StampReceive(env, responderID, m.Bytes(), opts.Integrity)
	}
	if opts.Validate {
		if err := protovalidate.Validate(req); err != nil {
			return req, err
		}
	}
	return req, nil
}
