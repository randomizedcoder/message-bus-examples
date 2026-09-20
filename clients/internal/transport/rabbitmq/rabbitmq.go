// Package rmqtransport is the RabbitMQ binding of the proto-bench transport
// interfaces (design §3.9, §7.5). P3 implements the RPC tier-C flow: the driver
// publishes a DeployRequest to the direct exchange `wl` with routing key
// `deploy.<region>`, `reply_to` = RabbitMQ direct reply-to
// (`amq.rabbitmq.reply-to`) and `correlation_id` per call; the agent's Responder
// binds `deploy.<region>`, handles it, and publishes the reply back on the
// default exchange keyed by the delivery's reply_to.
//
// Buffer handling (§7.5): amqp091 writes the body frames synchronously inside
// PublishWithContext under the channel send mutex, so a pooled encode buffer is
// safe to return the moment the call returns; `Delivery.Body` is owned by the
// delivery, so we decode and drop it.
package rmqtransport

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"buf.build/go/protovalidate"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

const (
	exchange      = "wl"
	directReplyTo = "amq.rabbitmq.reply-to"
)

// RoutingKey is the direct-exchange key for a region's deploy RPC (design §3.9).
func RoutingKey(region string) string { return "deploy." + region }

// Dial opens an AMQP connection. url is amqp://user:pass@host:port/.
func Dial(url string) (*amqp.Connection, error) { return amqp.Dial(url) }

// declareExchange declares the shared direct exchange; idempotent, so both ends
// call it.
func declareExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil)
}

// ─── Requester (client) ─────────────────────────────────────────────────────

type requester struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	opts transport.Options
	key  string

	seq     atomic.Uint64
	mu      sync.Mutex
	pending map[string]chan amqp.Delivery
}

// NewRequester opens a channel, declares the exchange, and starts consuming the
// RabbitMQ direct reply-to pseudo-queue; replies are dispatched to the waiting
// call by correlation_id, so one Requester multiplexes concurrent calls.
func NewRequester(conn *amqp.Connection, opts transport.Options, region string) (transport.Requester, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := declareExchange(ch); err != nil {
		_ = ch.Close()
		return nil, err
	}
	r := &requester{conn: conn, ch: ch, opts: opts, key: RoutingKey(region), pending: map[string]chan amqp.Delivery{}}
	// Direct reply-to: consume the pseudo-queue with auto-ack (required for it).
	deliveries, err := ch.Consume(directReplyTo, "", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}
	go r.dispatch(deliveries)
	return r, nil
}

func (r *requester) dispatch(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		r.mu.Lock()
		waiter := r.pending[d.CorrelationId]
		delete(r.pending, d.CorrelationId)
		r.mu.Unlock()
		if waiter != nil {
			waiter <- d
		}
	}
}

func (r *requester) Request(ctx context.Context, req, resp proto.Message) error {
	corr := strconv.FormatUint(r.seq.Add(1), 36)
	ch := make(chan amqp.Delivery, 1)
	r.mu.Lock()
	r.pending[corr] = ch
	r.mu.Unlock()

	buf, err := codec.Encode(r.opts.Codec, r.opts.Pool, req)
	if err != nil {
		r.forget(corr)
		return err
	}
	err = r.ch.PublishWithContext(ctx, exchange, r.key, false, false, amqp.Publishing{
		ContentType:   codec.ContentType(r.opts.Codec),
		CorrelationId: corr,
		ReplyTo:       directReplyTo,
		Body:          *buf,
	})
	r.opts.Pool.Put(buf) // synchronously written by PublishWithContext (§7.5)
	if err != nil {
		r.forget(corr)
		return err
	}
	select {
	case <-ctx.Done():
		r.forget(corr)
		return ctx.Err()
	case d := <-ch:
		return r.opts.Codec.Unmarshal(d.Body, resp)
	}
}

func (r *requester) forget(corr string) {
	r.mu.Lock()
	delete(r.pending, corr)
	r.mu.Unlock()
}

func (r *requester) Close() error { return r.ch.Close() }

// ─── Responder (server) ──────────────────────────────────────────────────────

type responder struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	opts        transport.Options
	responderID string
	region      string
	newReq      func() proto.Message
}

// NewResponder opens a channel, declares the exchange, and binds a queue for the
// region's deploy routing key. newReq builds a fresh request per delivery.
func NewResponder(conn *amqp.Connection, opts transport.Options, responderID, region string, newReq func() proto.Message) (transport.Responder, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := declareExchange(ch); err != nil {
		_ = ch.Close()
		return nil, err
	}
	// Durable, non-auto-delete: RabbitMQ 4.2 refuses transient non-exclusive
	// queues by default (the `transient_nonexcl_queues` deprecation). Redeclare
	// is idempotent, so a restarting agent just re-attaches to the same queue.
	q, err := ch.QueueDeclare(fmt.Sprintf("wl.deploy.%s", region), true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}
	if err := ch.QueueBind(q.Name, RoutingKey(region), exchange, false, nil); err != nil {
		_ = ch.Close()
		return nil, err
	}
	return &responder{conn: conn, ch: ch, opts: opts, responderID: responderID, region: region, newReq: newReq}, nil
}

func (s *responder) Serve(ctx context.Context, h transport.Handler) error {
	q := fmt.Sprintf("wl.deploy.%s", s.region)
	deliveries, err := s.ch.Consume(q, "", true, false, false, false, nil) // auto-ack: RPC needs no persistence
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
			s.reply(ctx, d, h)
		}
	}
}

func (s *responder) reply(ctx context.Context, d amqp.Delivery, h transport.Handler) {
	cdc := codec.ByContentType(d.ContentType)
	req := s.newReq()
	if err := cdc.Unmarshal(d.Body, req); err != nil {
		return
	}
	if env := envelope.Of(req); env != nil {
		envelope.StampReceive(env, s.responderID, d.Body, s.opts.Integrity)
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
	// Reply on the default exchange, routing key = the client's reply_to.
	_ = s.ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, amqp.Publishing{
		ContentType:   d.ContentType,
		CorrelationId: d.CorrelationId,
		Body:          *out,
	})
	s.opts.Pool.Put(out)
}

func (s *responder) Close() error { return s.ch.Close() }
