// Logs is the NATS JetStream fan-out binding for the logs plane (design §2.2,
// §3.9). A regional log source publishes LogChunk messages on
// `wl.<region>.logs.<workload_id>`; JetStream persists them to the stream
// WL_LOGS, and every subscriber attaches its OWN ephemeral push consumer to the
// subject — so N subscribers each receive a full copy of the stream (the
// fan-out). This is the mirror image of the telemetry binding (jetstream.go),
// where a single durable pull consumer per region load-balances one copy.
//
// The stream is bounded (short MaxAge + a byte cap) because log chunks are large
// (the `max` fixture is ~960 KiB) and transient: it shares the JetStream file
// store with WL_TELEMETRY, so it self-trims rather than growing unbounded.
// Buffer handling follows §7.5 — nats.go copies the payload synchronously inside
// PublishMsg, so the pooled encode buffer is returned the moment it returns.
package natstransport

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// LogsStreamName is the JetStream stream carrying the log fan-out for every
// region (design §3.9). One stream, subject-filtered per workload.
const LogsStreamName = "WL_LOGS"

// Log-stream bounds: R3 file-backed like WL_TELEMETRY, but self-trimming — log
// chunks are large and transient, and the stream shares the JetStream file
// store, so it caps total bytes and ages entries out quickly.
const (
	logsReplicas = 3
	logsMaxAge   = 10 * time.Minute
	logsMaxBytes = 512 << 20 // 512 MiB per replica
)

// LogsSubject is the JetStream publish/subscribe subject for one workload's logs
// in a region (design §3.9: wl.<region>.logs.<workload_id>).
func LogsSubject(region, workloadID string) string {
	return "wl." + region + ".logs." + workloadID
}

// EnsureLogsStream idempotently creates the WL_LOGS stream (R3, file-backed,
// subjects wl.*.logs.*, self-trimming). Safe to call concurrently from many
// publishers: an already-existing stream is treated as success (mirrors
// EnsureStream).
func EnsureLogsStream(js nats.JetStreamContext) error {
	if _, err := js.StreamInfo(LogsStreamName); err == nil {
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      LogsStreamName,
		Subjects:  []string{"wl.*.logs.*"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  logsReplicas,
		MaxAge:    logsMaxAge,
		MaxBytes:  logsMaxBytes,
	})
	if err == nil {
		return nil
	}
	// Lost a create race with another publisher: the stream now exists → success.
	if _, e2 := js.StreamInfo(LogsStreamName); e2 == nil {
		return nil
	}
	return err
}

// ─── Publisher (log source, durable one-way) ──────────────────────────────────

type logPublisher struct {
	js      nats.JetStreamContext
	opts    transport.Options
	subject string
}

// NewLogPublisher builds a fan-out log publisher for region/workloadID. Each
// Publish blocks until JetStream acks the LogChunk as persisted; JetStream then
// fans it out to every attached subscriber.
func NewLogPublisher(js nats.JetStreamContext, opts transport.Options, region, workloadID string) transport.Publisher {
	return &logPublisher{js: js, opts: opts, subject: LogsSubject(region, workloadID)}
}

func (p *logPublisher) Publish(ctx context.Context, m proto.Message) (transport.Release, error) {
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

func (p *logPublisher) Close() error { return nil } // the *nats.Conn is owned by the caller

// ─── Subscriber (customer, ephemeral push, fan-out) ───────────────────────────

// LogSubscription is one subscriber's ephemeral push consumer. Closing it
// unsubscribes and removes the ephemeral consumer server-side.
type LogSubscription struct{ sub *nats.Subscription }

func (s *LogSubscription) Close() error {
	if s.sub == nil {
		return nil
	}
	return s.sub.Unsubscribe()
}

// SubscribeLogs attaches an ephemeral push consumer to region/workloadID's log
// subject and invokes fn for each LogChunk delivered after the subscription is
// created (DeliverNew, so a subscriber sees only chunks published once it is
// live — the clean fan-out window). Each call creates an independent consumer,
// so K subscribers each receive every published chunk (the fan-out, design
// §3.9). The caller must have run EnsureLogsStream first.
//
// AckNone: fan-out delivery is the measurement; there is no per-message ack and
// no redelivery bookkeeping (contrast the durable telemetry pull consumer). The
// subscription's pending limits are lifted so a bounded fan-out run is never
// dropped as a slow consumer.
func SubscribeLogs(js nats.JetStreamContext, region, workloadID string, fn func(transport.Msg)) (*LogSubscription, error) {
	sub, err := js.Subscribe(LogsSubject(region, workloadID), func(m *nats.Msg) {
		fn(&jsMsg{m: m})
	}, nats.BindStream(LogsStreamName), nats.DeliverNew(), nats.AckNone())
	if err != nil {
		return nil, err
	}
	_ = sub.SetPendingLimits(-1, -1) // measure true fan-out delivery, no slow-consumer drops
	return &LogSubscription{sub: sub}, nil
}
