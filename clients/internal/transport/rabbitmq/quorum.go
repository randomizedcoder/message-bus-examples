// Quorum is the RabbitMQ durable, at-least-once telemetry binding (design §2.3
// tier B, §3.9). The driver's Publisher publishes TelemetrySample to the durable
// quorum queue `wl.telemetry.<region>` (x-queue-type: quorum) with persistent
// delivery and publisher confirms — Publish blocks until the broker confirms the
// message committed to the quorum (Raft-replicated across the RabbitMQ nodes),
// the tier-B "acked and replicated before ack" guarantee. Each region-agent
// consumes its own region's queue with manual ack, acks every delivery, and
// counts redeliveries (the AMQP `redelivered` flag) — the tier-B correctness
// signal (design §8.4).
//
// Buffer handling (§7.5): amqp091 writes the body frames synchronously inside
// PublishWithDeferredConfirmWithContext under the channel send mutex, so a pooled
// encode buffer is safe to return the moment the call returns (before the confirm
// arrives); Delivery.Body is owned by the delivery, so we decode and drop it.
package rmqtransport

import (
	"context"
	"fmt"

	"buf.build/go/protovalidate"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// TelemetryQueue is the durable quorum queue for a region's telemetry (design
// §3.9). Published to via the default exchange keyed by the queue name.
func TelemetryQueue(region string) string { return "wl.telemetry." + region }

// declareQuorumQueue declares region's telemetry queue as a durable quorum queue.
// It is idempotent (redeclare with identical args is a no-op), so both the
// publisher and the consumer call it and a restarting agent re-attaches.
func declareQuorumQueue(ch *amqp.Channel, region string) error {
	_, err := ch.QueueDeclare(TelemetryQueue(region), true, false, false, false,
		amqp.Table{"x-queue-type": "quorum"})
	return err
}

// ─── Publisher (client, durable one-way) ─────────────────────────────────────

type quorumPublisher struct {
	ch    *amqp.Channel
	opts  transport.Options
	queue string
}

// NewPublisher opens a confirm-mode channel, declares the region's quorum queue,
// and returns a durable telemetry publisher. Each Publish blocks until the broker
// confirms the message committed (design §2.3 tier B). The caller owns conn.
func NewPublisher(conn *amqp.Connection, opts transport.Options, region string) (transport.Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := declareQuorumQueue(ch, region); err != nil {
		_ = ch.Close()
		return nil, err
	}
	if err := ch.Confirm(false); err != nil { // publisher confirms
		_ = ch.Close()
		return nil, err
	}
	return &quorumPublisher{ch: ch, opts: opts, queue: TelemetryQueue(region)}, nil
}

func (p *quorumPublisher) Publish(ctx context.Context, m proto.Message) (transport.Release, error) {
	buf, err := codec.Encode(p.opts.Codec, p.opts.Pool, m)
	if err != nil {
		return nil, err
	}
	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", p.queue, false, false, amqp.Publishing{
		ContentType:  codec.ContentType(p.opts.Codec),
		DeliveryMode: amqp.Persistent,
		Body:         *buf,
	})
	p.opts.Pool.Put(buf) // written synchronously by the publish call (§7.5)
	if err != nil {
		return func() {}, err
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return func() {}, err
	}
	if !acked {
		return func() {}, fmt.Errorf("rabbitmq: publish nacked (queue %s)", p.queue)
	}
	return func() {}, nil
}

func (p *quorumPublisher) Close() error { return p.ch.Close() }

// ─── Consumer (server, manual ack) ────────────────────────────────────────────

// qMsg is one quorum-queue delivery. Codec comes from the AMQP content_type so
// the consumer decodes before it has seen the envelope; Redelivered exposes the
// AMQP redelivered flag so the agent can report tier-B redeliveries (§8.4).
type qMsg struct{ d amqp.Delivery }

func (m *qMsg) Bytes() []byte      { return m.d.Body }
func (m *qMsg) Ack() error         { return m.d.Ack(false) }
func (m *qMsg) Codec() codec.Codec { return codec.ByContentType(m.d.ContentType) }
func (m *qMsg) Redelivered() bool  { return m.d.Redelivered }

// Redelivered reports whether m is an AMQP redelivery. It is false for any Msg
// that is not a quorum-queue delivery, so the agent's consume loop can call it
// uniformly.
func Redelivered(m transport.Msg) bool {
	if qm, ok := m.(interface{ Redelivered() bool }); ok {
		return qm.Redelivered()
	}
	return false
}

type quorumConsumer struct {
	ch     *amqp.Channel
	opts   transport.Options
	region string
}

// NewConsumer opens a channel, declares region's quorum queue, and returns a
// manual-ack telemetry consumer. The caller owns conn.
func NewConsumer(conn *amqp.Connection, opts transport.Options, region string) (transport.Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := declareQuorumQueue(ch, region); err != nil {
		_ = ch.Close()
		return nil, err
	}
	return &quorumConsumer{ch: ch, opts: opts, region: region}, nil
}

func (c *quorumConsumer) Consume(ctx context.Context, fn func(transport.Msg) error) error {
	deliveries, err := c.ch.Consume(TelemetryQueue(c.region), "", false, false, false, false, nil) // manual ack
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			_ = fn(&qMsg{d: d})
		}
	}
}

func (c *quorumConsumer) Close() error { return c.ch.Close() }

// Decode turns a delivered quorum Msg into a request message, receive-stamps it,
// and optionally validates — the one-way analogue of a Responder's decode half
// (there is no reply to send). Mirrors the MQTT/JetStream consumer helpers so the
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
