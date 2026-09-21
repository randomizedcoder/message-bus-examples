// Package rabbitmqx is the RabbitMQ request/reply binding for the RPC lab's
// routing envelope (§12): a GatewayService client that satisfies rpc.Client and
// a responder helper that backs an rpc.Handler, over AMQP. It is the broker
// twin of natsx — same rpc.Client / rpc.Handler seams, a different wire.
//
// RabbitMQ RPC maps naturally onto a request queue + a reply destination + a
// correlation id (§12): the client publishes the marshaled envelope to a request
// queue with Publishing.CorrelationId = request_id and Publishing.ReplyTo = its
// reply destination; the responder dispatches and publishes the response to that
// reply destination echoing the correlation id. Replies arrive out of band, so —
// unlike NATS Core's _INBOX — the client uses rpc.Correlator to match a reply
// back to the waiting Call by request_id.
//
// Two reply destinations are offered (§12): a classic per-client reply queue
// (ModeReplyQueue) and RabbitMQ Direct Reply-To (ModeDirect), the latter
// conceptually close to NATS inbox request/reply — a useful side-by-side. The
// responder is identical for both; only the client's reply destination differs.
//
// Like natsx it marshals the whole rpc.v1 envelope with proto and pulls in only
// amqp091 + rpc.* — not the proto-bench codec/envelope/transport machinery.
package rabbitmqx

import (
	"context"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
)

const (
	// RequestQueue is the single durable queue every request is published to; the
	// responder dispatches by the envelope's service.method, so the gateway stays
	// payload-blind (one queue serves every service).
	RequestQueue = "rpc.requests"
	// directReplyTo is RabbitMQ's Direct Reply-To pseudo-queue: a broker-side
	// name the client consumes with autoAck and names as its ReplyTo, with no
	// queue to declare or clean up.
	directReplyTo = "amq.rabbitmq.reply-to"
	contentType   = "application/x-protobuf"
)

// Mode selects the client's reply destination (§12).
type Mode int

const (
	// ModeReplyQueue declares a server-named, exclusive, auto-delete reply queue
	// per client and correlates replies on it by request_id.
	ModeReplyQueue Mode = iota
	// ModeDirect uses RabbitMQ Direct Reply-To — the lowest-overhead reply path,
	// conceptually closest to NATS inbox request/reply.
	ModeDirect
)

// URL builds an AMQP URL from addr. A value already carrying a scheme is used
// verbatim (so a full amqp://user:pass@host/vhost passes through); otherwise
// addr is a host:port and user/pass are woven into amqp://user:pass@host:port/.
// RabbitMQ, unlike NATS, requires credentials.
func URL(addr, user, pass string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return fmt.Sprintf("amqp://%s:%s@%s/", user, pass, addr)
}

// ─── Client ──────────────────────────────────────────────────────────────────

// Client is a GatewayService AMQP client that satisfies rpc.Client. It owns one
// connection + channel and correlates out-of-band replies by request_id.
type Client struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	corr    *rpc.Correlator
	replyTo string
	caps    rpc.Capabilities
}

// Dial connects to url, sets up the reply destination for mode, and starts
// consuming replies. PublishWithContext serializes on the channel's own mutex,
// so concurrent Call is safe on the single channel.
func Dial(url string, mode Mode) (*Client, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &Client{
		conn: conn,
		ch:   ch,
		corr: rpc.NewCorrelator(),
		caps: rpc.Capabilities{
			NativeRequestReply: false, // request/reply is synthesized from a queue + correlator
			Streaming:          false,
			AtLeastOnce:        false,
		},
	}

	var deliveries <-chan amqp.Delivery
	switch mode {
	case ModeDirect:
		// Direct Reply-To: consume the pseudo-queue with autoAck (required); no declare.
		c.replyTo = directReplyTo
		deliveries, err = ch.Consume(directReplyTo, "", true, false, false, false, nil)
	default:
		// Classic reply queue: server-named, transient, exclusive, auto-delete —
		// private to this client and cleaned up when the connection closes.
		q, e := ch.QueueDeclare("", false, true, true, false, nil)
		if e != nil {
			ch.Close()
			conn.Close()
			return nil, e
		}
		c.replyTo = q.Name
		deliveries, err = ch.Consume(q.Name, "", true, false, false, false, nil)
	}
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	go c.dispatch(deliveries)
	return c, nil
}

// dispatch routes each reply to the waiting Call by correlation id (== request_id).
func (c *Client) dispatch(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		resp := &rpcv1.Response{}
		if err := proto.Unmarshal(d.Body, resp); err != nil {
			continue
		}
		c.corr.Deliver(d.CorrelationId, rpc.Result{Response: resp})
	}
}

// Call publishes req to the request queue and waits for the correlated reply or
// ctx's deadline. A publish failure maps to ErrUnavailable → STATUS_UNAVAILABLE.
func (c *Client) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	id := req.GetRequestId()
	ch, cancel := c.corr.Register(id)
	defer cancel()

	pub := amqp.Publishing{
		ContentType:   contentType,
		CorrelationId: id,
		ReplyTo:       c.replyTo,
		Body:          data,
	}
	if err := c.ch.PublishWithContext(ctx, "", RequestQueue, false, false, pub); err != nil {
		return nil, fmt.Errorf("%w: publish: %v", rpc.ErrUnavailable, err)
	}

	select {
	case res := <-ch:
		return res.Response, res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Capabilities reports what this RabbitMQ mode provides (§34).
func (c *Client) Capabilities() rpc.Capabilities { return c.caps }

// Close unblocks any outstanding Call, then closes the channel and connection.
func (c *Client) Close() error {
	c.corr.Shutdown(fmt.Errorf("%w: client closed", rpc.ErrUnavailable))
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	return nil
}

// ─── Responder ─────────────────────────────────────────────────────────────

// Responder consumes the request queue, backs each request with an rpc.Handler,
// and publishes the response to the request's ReplyTo echoing its CorrelationId.
// It serves both client modes: the reply destination is whatever the client put
// on the request, so Direct Reply-To and a classic reply queue look identical here.
type Responder struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	cancel context.CancelFunc
	done   chan struct{}
}

// Serve connects to url, declares the (durable) request queue, and serves it in
// the background until Close (or ctx) stops it. Deliveries are auto-acked: an RPC
// request needs no broker persistence once it has been handed to the handler.
func Serve(ctx context.Context, url string, h rpc.Handler) (*Responder, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	// Durable, non-exclusive, not auto-deleted: RabbitMQ 4.2 refuses a transient
	// non-exclusive queue by default, and the queue outlives any one responder.
	if _, err := ch.QueueDeclare(RequestQueue, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	deliveries, err := ch.Consume(RequestQueue, "", true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	rctx, cancel := context.WithCancel(ctx)
	r := &Responder{conn: conn, ch: ch, cancel: cancel, done: make(chan struct{})}
	go r.loop(rctx, deliveries, h)
	return r, nil
}

func (r *Responder) loop(ctx context.Context, deliveries <-chan amqp.Delivery, h rpc.Handler) {
	defer close(r.done)
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			r.handle(ctx, d, h)
		}
	}
}

// handle decodes one request, runs the handler, and publishes the response to
// the reply destination on the default exchange. An undecodable delivery has no
// envelope to correlate, so it is dropped (the caller sees a timeout).
func (r *Responder) handle(ctx context.Context, d amqp.Delivery, h rpc.Handler) {
	req := &rpcv1.Request{}
	if err := proto.Unmarshal(d.Body, req); err != nil {
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
	pub := amqp.Publishing{
		ContentType:   contentType,
		CorrelationId: d.CorrelationId,
		Body:          out,
	}
	_ = r.ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, pub)
}

// Close stops the serve loop and closes the channel and connection.
func (r *Responder) Close() error {
	r.cancel()
	<-r.done
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
	return nil
}
