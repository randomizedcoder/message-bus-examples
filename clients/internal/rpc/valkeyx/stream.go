// Streams mode (durable) for the Valkey RPC binding, the twin of NATS JetStream
// in this lab: requests are XADDed to a shared request stream consumed by a
// durable consumer group, and each reply is XADDed to the caller's own reply
// stream. Unlike Pub/Sub mode, an in-flight request survives the responder being
// momentarily absent (it stays in the stream until a consumer reads and acks it),
// and redeliveries after a crash are drained from the consumer's pending list —
// so the idempotency cache (§29) matters here.
//
// Reply routing and correlation, as in Pub/Sub mode, ride in the envelope: the
// reply stream is derived from Request.client_id and the match is by request_id.
package valkeyx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
)

const (
	// reqStream is the single durable stream every request is XADDed to; the
	// responder group dispatches by the envelope's service.method, so one stream
	// serves every service. respStreamPrefix names each client's private reply
	// stream. Stream keys live in the keyspace, distinct from Pub/Sub channels.
	reqStream        = "rpc:stream:requests"
	respStreamPrefix = "rpc:stream:response" // rpc:stream:response:<client-id>
	// group is the durable consumer group responders join to load-balance and to
	// retain a pending-entries list for redelivery after a crash.
	group = "rpc-responders"

	// streamMaxLen caps each stream so an unbounded RPC run cannot grow Valkey
	// memory without bound; Approx (~) lets the server trim efficiently.
	streamMaxLen = 10000
	// blockDur is how long a blocking read waits for new entries before looping.
	blockDur = 5 * time.Second
	// readBatch caps how many stream entries one blocking read returns.
	readBatch = 64
)

// respStreamName is the reply stream for a client id.
func respStreamName(clientID string) string {
	return respStreamPrefix + ":" + clientID
}

// ─── StreamClient ────────────────────────────────────────────────────────────

// StreamClient is a GatewayService Valkey Streams client that satisfies
// rpc.Client. It XADDs requests to the shared request stream and reads its own
// reply stream in the background, correlating replies by request_id.
type StreamClient struct {
	rdb        *redis.Client
	corr       *rpc.Correlator
	clientID   string
	respStream string
	cancel     context.CancelFunc
	done       chan struct{}
	caps       rpc.Capabilities
}

// DialStream connects to Valkey via the given Sentinels and starts reading this
// client's reply stream. pass is the primary's password (empty for none).
func DialStream(sentinels, pass string) (*StreamClient, error) {
	rdb, err := dial(sentinels, pass)
	if err != nil {
		return nil, err
	}
	clientID := newClientID("rpc-valkeyx-stream")
	ctx, cancel := context.WithCancel(context.Background())
	c := &StreamClient{
		rdb:        rdb,
		corr:       rpc.NewCorrelator(),
		clientID:   clientID,
		respStream: respStreamName(clientID),
		cancel:     cancel,
		done:       make(chan struct{}),
		caps: rpc.Capabilities{
			NativeRequestReply: false,
			DurableRequests:    true,
			Replay:             true,
			AtLeastOnce:        true,
			Streaming:          false,
		},
	}
	go c.readReplies(ctx)
	return c, nil
}

// readReplies XREADs this client's private reply stream from the beginning (the
// stream is fresh per unique client id, so "0" reads every reply in order with no
// gap) and delivers each to the waiting Call by request_id.
func (c *StreamClient) readReplies(ctx context.Context) {
	defer close(c.done)
	lastID := "0"
	for {
		if ctx.Err() != nil {
			return
		}
		res, err := c.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{c.respStream, lastID},
			Count:   readBatch,
			Block:   blockDur,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // block timeout, no new replies
			}
			if ctx.Err() != nil {
				return
			}
			// Transient error (failover in progress, etc.): back off briefly.
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		for _, st := range res {
			for _, m := range st.Messages {
				lastID = m.ID
				data, _ := m.Values["data"].(string)
				resp := &rpcv1.Response{}
				if err := proto.Unmarshal([]byte(data), resp); err != nil {
					continue
				}
				c.corr.Deliver(resp.GetRequestId(), rpc.Result{Response: resp})
			}
		}
	}
}

// Call stamps the reply-route client id onto req, XADDs it to the request stream,
// and waits for the correlated reply or ctx's deadline. An XADD failure maps to
// ErrUnavailable → STATUS_UNAVAILABLE.
func (c *StreamClient) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	req.ClientId = c.clientID
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	id := req.GetRequestId()
	ch, cancel := c.corr.Register(id)
	defer cancel()

	if err := c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: reqStream,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: map[string]any{"data": data},
	}).Err(); err != nil {
		return nil, fmt.Errorf("%w: xadd: %v", rpc.ErrUnavailable, err)
	}

	select {
	case res := <-ch:
		return res.Response, res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Capabilities reports what Valkey Streams provide (§34).
func (c *StreamClient) Capabilities() rpc.Capabilities { return c.caps }

// Close stops the reply reader, unblocks any outstanding Call, deletes this
// client's now-unneeded reply stream, and closes the pool.
func (c *StreamClient) Close() error {
	c.cancel()
	<-c.done
	c.corr.Shutdown(fmt.Errorf("%w: client closed", rpc.ErrUnavailable))
	// The private reply stream is bounded to this client's lifetime; drop it so
	// abandoned reply streams do not accumulate in Valkey.
	delCtx, delCancel := context.WithTimeout(context.Background(), pingTimeout)
	_ = c.rdb.Del(delCtx, c.respStream).Err()
	delCancel()
	if c.rdb != nil {
		_ = c.rdb.Close()
	}
	return nil
}

// ─── StreamResponder ─────────────────────────────────────────────────────────

// StreamResponder consumes the request stream through a durable consumer group,
// backs each request with an rpc.Handler, and XADDs the response to the caller's
// reply stream. It is the Valkey-Streams twin of natsx.ServeJetStream.
type StreamResponder struct {
	rdb      *redis.Client
	consumer string
	cancel   context.CancelFunc
	done     chan struct{}
}

// ServeStream connects to Valkey via the given Sentinels, ensures the request
// stream and consumer group exist, and serves the group in the background until
// Close (or ctx). New messages only ("$"): a fresh responder does not replay
// requests whose callers have long since timed out.
func ServeStream(ctx context.Context, sentinels, pass string, h rpc.Handler) (*StreamResponder, error) {
	rdb, err := dial(sentinels, pass)
	if err != nil {
		return nil, err
	}
	// Idempotent create: MkStream makes the stream if absent; BUSYGROUP means the
	// group already exists (another responder created it), which is fine.
	if err := rdb.XGroupCreateMkStream(ctx, reqStream, group, "$").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		_ = rdb.Close()
		return nil, fmt.Errorf("%w: valkey xgroup create: %v", rpc.ErrUnavailable, err)
	}
	sctx, cancel := context.WithCancel(ctx)
	r := &StreamResponder{
		rdb:      rdb,
		consumer: newClientID("rpc-responder"),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go r.loop(sctx, h)
	return r, nil
}

// loop first drains this consumer's pending-entries list (redeliveries left
// unacked by a previous crash, read from id "0") and then switches to new
// messages (">"), dispatching each on its own goroutine.
func (r *StreamResponder) loop(ctx context.Context, h rpc.Handler) {
	defer close(r.done)
	pending := true
	lastPending := "0"
	for {
		if ctx.Err() != nil {
			return
		}
		readID := ">"
		if pending {
			readID = lastPending
		}
		res, err := r.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: r.consumer,
			Streams:  []string{reqStream, readID},
			Count:    readBatch,
			Block:    blockDur,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				pending = false // no more pending; block for new below
				continue
			}
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		got := 0
		for _, st := range res {
			for _, m := range st.Messages {
				got++
				if pending {
					lastPending = m.ID
				}
				msg := m
				go r.handle(ctx, msg, h)
			}
		}
		if pending && got == 0 {
			pending = false
		}
	}
}

// handle decodes one request, runs the handler, XADDs the response to the
// caller's reply stream, and acks the request. A poison (undecodable) message is
// acked too, so it is not redelivered forever.
func (r *StreamResponder) handle(ctx context.Context, m redis.XMessage, h rpc.Handler) {
	defer func() { _ = r.rdb.XAck(ctx, reqStream, group, m.ID).Err() }()
	data, _ := m.Values["data"].(string)
	req := &rpcv1.Request{}
	if err := proto.Unmarshal([]byte(data), req); err != nil {
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
	_ = r.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: respStreamName(clientID),
		MaxLen: streamMaxLen,
		Approx: true,
		Values: map[string]any{"data": out},
	}).Err()
}

// Close stops the serve loop and closes the pool. The request stream and group
// are durable and shared, so they are left in place.
func (r *StreamResponder) Close() error {
	r.cancel()
	<-r.done
	if r.rdb != nil {
		_ = r.rdb.Close()
	}
	return nil
}
