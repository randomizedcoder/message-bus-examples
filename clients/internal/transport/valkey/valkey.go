// Package valkeytransport is the Valkey (Redis) Streams binding of the
// proto-bench transport interfaces (design §3.9, §7.5). P3 implements the
// stream RPC tier-C flow: the driver XADDs a DeployRequest to
// `wl:<region>:deploy` (a consumer group `agents` load-balances it) and blocks
// reading its per-client reply stream `wl:reply:<client_id>`; the agent's
// Responder XREADGROUPs the deploy stream, handles it, XADDs the reply, and
// XACKs. `MAXLEN ~` keeps the streams bounded.
//
// Buffer handling (§7.5): go-redis copies the []byte arg into its write buffer
// during the synchronous XAdd, so a pooled encode buffer is safe to return the
// moment XAdd returns (but NOT inside a Pipeline until Exec).
package valkeytransport

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"buf.build/go/protovalidate"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

const (
	group     = "agents"
	maxLen    = 10000 // ~ approximate trim, keeps the streams bounded
	blockTime = 5 * time.Second
)

// DeployStream is the request stream for a region (design §3.9).
func DeployStream(region string) string { return "wl:" + region + ":deploy" }

// ReplyStream is a client's private reply stream.
func ReplyStream(clientID string) string { return "wl:reply:" + clientID }

// ─── Requester (client) ─────────────────────────────────────────────────────

type requester struct {
	rdb      *redis.Client
	opts     transport.Options
	stream   string
	reply    string
	clientID string

	seq     atomic.Uint64
	mu      sync.Mutex
	pending map[string]chan []byte
	cancel  context.CancelFunc
}

// NewRequester targets region's deploy stream and starts a reader goroutine on
// this client's reply stream; replies are dispatched by correlation id.
func NewRequester(rdb *redis.Client, opts transport.Options, region, clientID string) transport.Requester {
	ctx, cancel := context.WithCancel(context.Background())
	r := &requester{
		rdb: rdb, opts: opts, stream: DeployStream(region),
		reply: ReplyStream(clientID), clientID: clientID,
		pending: map[string]chan []byte{}, cancel: cancel,
	}
	go r.readReplies(ctx)
	return r
}

func (r *requester) readReplies(ctx context.Context) {
	lastID := "$"
	for ctx.Err() == nil {
		res, err := r.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{r.reply, lastID}, Block: blockTime,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue
			}
			continue
		}
		for _, st := range res {
			for _, m := range st.Messages {
				lastID = m.ID
				corr, _ := m.Values["corr"].(string)
				data, _ := m.Values["data"].(string)
				r.mu.Lock()
				w := r.pending[corr]
				delete(r.pending, corr)
				r.mu.Unlock()
				if w != nil {
					w <- []byte(data)
				}
			}
		}
	}
}

func (r *requester) Request(ctx context.Context, req, resp proto.Message) error {
	corr := strconv.FormatUint(r.seq.Add(1), 36)
	ch := make(chan []byte, 1)
	r.mu.Lock()
	r.pending[corr] = ch
	r.mu.Unlock()

	buf, err := codec.Encode(r.opts.Codec, r.opts.Pool, req)
	if err != nil {
		r.forget(corr)
		return err
	}
	err = r.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: r.stream, MaxLen: maxLen, Approx: true,
		Values: map[string]any{
			"ct": codec.ContentType(r.opts.Codec), "corr": corr,
			"reply": r.reply, "data": *buf,
		},
	}).Err()
	r.opts.Pool.Put(buf) // synchronously written by XAdd (§7.5)
	if err != nil {
		r.forget(corr)
		return err
	}
	select {
	case <-ctx.Done():
		r.forget(corr)
		return ctx.Err()
	case data := <-ch:
		return r.opts.Codec.Unmarshal(data, resp)
	}
}

func (r *requester) forget(corr string) {
	r.mu.Lock()
	delete(r.pending, corr)
	r.mu.Unlock()
}

func (r *requester) Close() error {
	r.cancel()
	return nil
}

// ─── Responder (server) ──────────────────────────────────────────────────────

type responder struct {
	rdb         *redis.Client
	opts        transport.Options
	responderID string
	stream      string
	newReq      func() proto.Message
}

// NewResponder creates the consumer group on region's deploy stream (idempotent)
// and serves it. newReq builds a fresh request per delivery.
func NewResponder(rdb *redis.Client, opts transport.Options, responderID, region string, newReq func() proto.Message) (transport.Responder, error) {
	stream := DeployStream(region)
	err := rdb.XGroupCreateMkStream(context.Background(), stream, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, err
	}
	return &responder{rdb: rdb, opts: opts, responderID: responderID, stream: stream, newReq: newReq}, nil
}

func (s *responder) Serve(ctx context.Context, h transport.Handler) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: s.responderID,
			Streams: []string{s.stream, ">"}, Count: 64, Block: blockTime,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue
			}
			continue
		}
		for _, st := range res {
			for _, m := range st.Messages {
				s.reply(ctx, m, h)
				s.rdb.XAck(ctx, s.stream, group, m.ID)
			}
		}
	}
}

func (s *responder) reply(ctx context.Context, m redis.XMessage, h transport.Handler) {
	ct, _ := m.Values["ct"].(string)
	corr, _ := m.Values["corr"].(string)
	replyStream, _ := m.Values["reply"].(string)
	data, _ := m.Values["data"].(string)
	if replyStream == "" {
		return
	}
	cdc := codec.ByContentType(ct)
	req := s.newReq()
	raw := []byte(data)
	if err := cdc.Unmarshal(raw, req); err != nil {
		return
	}
	if env := envelope.Of(req); env != nil {
		envelope.StampReceive(env, s.responderID, raw, s.opts.Integrity)
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
	s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: replyStream, MaxLen: maxLen, Approx: true,
		Values: map[string]any{"ct": ct, "corr": corr, "data": *out},
	})
	s.opts.Pool.Put(out) // synchronously written by XAdd (§7.5)
}

func (s *responder) Close() error { return nil }
