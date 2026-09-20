// Stream is the Valkey durable, at-least-once telemetry binding (design §2.3
// tier B, §3.9). The driver's Publisher XADDs TelemetrySample to
// `wl:<region>:telemetry` (MAXLEN ~ trimmed); each region-agent consumes its own
// region's stream through the consumer group `agents`, XACKs every delivery, and
// counts redeliveries — a message delivered to this consumer but not yet acked
// (its Pending-Entries-List backlog after a crash/restart) is re-read and counted
// as redelivered, the tier-B correctness signal (design §8.4).
//
// Buffer handling (§7.5): go-redis copies the []byte arg into its write buffer
// during the synchronous XAdd, so a pooled encode buffer is safe to return the
// moment XAdd returns; the delivered value is a fresh string, so we decode and
// drop it.
package valkeytransport

import (
	"context"
	"errors"
	"strings"

	"buf.build/go/protovalidate"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// TelemetryStream is the durable telemetry stream for a region (design §3.9).
func TelemetryStream(region string) string { return "wl:" + region + ":telemetry" }

// ─── Publisher (client, durable one-way) ─────────────────────────────────────

type streamPublisher struct {
	rdb    *redis.Client
	opts   transport.Options
	stream string
}

// NewPublisher builds a Valkey stream telemetry publisher for region. Each
// Publish XADDs one entry (returned once the primary has written it). The caller
// owns rdb.
func NewPublisher(rdb *redis.Client, opts transport.Options, region string) transport.Publisher {
	return &streamPublisher{rdb: rdb, opts: opts, stream: TelemetryStream(region)}
}

func (p *streamPublisher) Publish(ctx context.Context, m proto.Message) (transport.Release, error) {
	buf, err := codec.Encode(p.opts.Codec, p.opts.Pool, m)
	if err != nil {
		return nil, err
	}
	err = p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream, MaxLen: maxLen, Approx: true,
		Values: map[string]any{"ct": codec.ContentType(p.opts.Codec), "data": *buf},
	}).Err()
	p.opts.Pool.Put(buf) // synchronously written by XAdd (§7.5)
	return func() {}, err
}

func (p *streamPublisher) Close() error { return nil } // the *redis.Client is owned by the caller

// ─── Consumer (server, consumer group + XACK) ─────────────────────────────────

// streamMsg is one stream delivery. Codec comes from the `ct` field so the
// consumer decodes before it has seen the envelope; Redelivered is true for a
// message re-read from this consumer's pending list (design §8.4).
type streamMsg struct {
	rdb            *redis.Client
	stream, id, ct string
	data           []byte
	redelivered    bool
}

func (m *streamMsg) Bytes() []byte { return m.data }
func (m *streamMsg) Ack() error {
	return m.rdb.XAck(context.Background(), m.stream, group, m.id).Err()
}
func (m *streamMsg) Codec() codec.Codec { return codec.ByContentType(m.ct) }
func (m *streamMsg) Redelivered() bool  { return m.redelivered }

// Redelivered reports whether m is a re-read (pending) stream delivery. It is
// false for any Msg that is not a stream delivery, so the agent's consume loop
// can call it uniformly.
func Redelivered(m transport.Msg) bool {
	if sm, ok := m.(interface{ Redelivered() bool }); ok {
		return sm.Redelivered()
	}
	return false
}

type streamConsumer struct {
	rdb      *redis.Client
	opts     transport.Options
	stream   string
	consumer string
}

// NewConsumer creates the consumer group on region's telemetry stream (idempotent,
// starting at "$" so only entries published after the group exists are consumed —
// the agent creates the group at startup, before any driver run) and returns a
// consumer that XACKs each delivery. consumerID names this consumer in the group.
func NewConsumer(rdb *redis.Client, opts transport.Options, region, consumerID string) (transport.Consumer, error) {
	stream := TelemetryStream(region)
	err := rdb.XGroupCreateMkStream(context.Background(), stream, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, err
	}
	return &streamConsumer{rdb: rdb, opts: opts, stream: stream, consumer: consumerID}, nil
}

func (c *streamConsumer) Consume(ctx context.Context, fn func(transport.Msg) error) error {
	// First drain this consumer's pending backlog (id "0" — messages delivered
	// before a crash/restart but never acked); those are redeliveries. Once the
	// backlog is empty, switch to ">" for new messages.
	pending := true
	for {
		if ctx.Err() != nil {
			return nil
		}
		id := ">"
		if pending {
			id = "0"
		}
		res, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: c.consumer,
			Streams: []string{c.stream, id}, Count: 64, Block: blockTime,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				pending = false // no (more) pending / block timeout → read new next
				continue
			}
			continue
		}
		total := 0
		for _, st := range res {
			for _, m := range st.Messages {
				data, _ := m.Values["data"].(string)
				ct, _ := m.Values["ct"].(string)
				_ = fn(&streamMsg{rdb: c.rdb, stream: c.stream, id: m.ID, ct: ct, data: []byte(data), redelivered: pending})
				total++
			}
		}
		if pending && total == 0 {
			pending = false // pending backlog drained → read new messages next
		}
	}
}

func (c *streamConsumer) Close() error { return nil }

// Decode turns a delivered stream Msg into a request message, receive-stamps it,
// and optionally validates — the one-way analogue of a Responder's decode half.
// Mirrors the MQTT/JetStream/quorum consumer helpers so the agent's telemetry
// consume loop is transport-neutral.
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
