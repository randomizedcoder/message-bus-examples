package rpc

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// RetryPolicy configures a RetryClient. MaxAttempts is the total number of
// attempts including the first; a value <= 1 disables retrying (one attempt).
// Backoff is the fixed delay inserted before each retry (attempt 2 onward); a
// non-positive Backoff retries immediately.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

// RetryClient decorates a Client with §29 safe retries: a transient failure — a
// lost response, a broker blip, gateway-B briefly unreachable — is re-sent under
// a *new request_id* but the *same idempotency_key*. A service with an
// idempotency cache therefore executes the logical operation once and replays the
// original result on the retry, so request/reply does not silently become
// at-least-once execution (§29 keeps request_id = one attempt, idempotency_key =
// one logical operation strictly separate).
//
// Only genuinely transient outcomes are retried: a timeout or an UNAVAILABLE
// status (transport error or wire status). An application-level non-OK
// (NOT_FOUND, INVALID_ARGUMENT, INTERNAL) is returned as-is — retrying cannot
// change a deterministic rejection.
//
// Retrying is only safe because of the idempotency_key, so RetryClient mints one
// when the request carries none: every retried operation is then deduplicable
// even if the caller set no key. The caller's Request is never mutated; each
// attempt is issued against a clone.
type RetryClient struct {
	inner   Client
	policy  RetryPolicy
	retries atomic.Uint64
}

// NewRetryClient wraps inner with policy. A policy with MaxAttempts < 1 is raised
// to 1, so the client always issues at least one attempt (and still fills an
// idempotency_key, keeping downstream dedup meaningful even without retries).
func NewRetryClient(inner Client, policy RetryPolicy) *RetryClient {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	return &RetryClient{inner: inner, policy: policy}
}

// Call issues req, retrying transient failures per the policy. Each attempt after
// the first carries a fresh request_id; the idempotency_key is held stable across
// all attempts (minted here when req has none). See RetryClient.
func (c *RetryClient) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	key := req.GetIdempotencyKey()
	if key == "" {
		key = uuid.NewString()
	}
	var resp *rpcv1.Response
	var err error
	for attempt := 1; attempt <= c.policy.MaxAttempts; attempt++ {
		r, _ := proto.Clone(req).(*rpcv1.Request)
		r.IdempotencyKey = key
		if attempt > 1 {
			r.RequestId = uuid.NewString() // §29: each attempt is a distinct request_id
		}
		resp, err = c.inner.Call(ctx, r)
		if !retryable(err, resp) || attempt == c.policy.MaxAttempts {
			return resp, err
		}
		c.retries.Add(1)
		if !sleepCtx(ctx, c.policy.Backoff) {
			return resp, err // ctx expired during backoff; surface the last outcome
		}
	}
	return resp, err
}

// Retries is the cumulative number of retry attempts this client has issued
// (total attempts minus one per Call that retried). The benchmark driver reads
// the delta across a cell to report retries alongside idempotent replays.
func (c *RetryClient) Retries() uint64 { return c.retries.Load() }

// Capabilities and Close delegate to the wrapped client so RetryClient is a
// drop-in Client.
func (c *RetryClient) Capabilities() Capabilities { return c.inner.Capabilities() }

// Close releases the wrapped client.
func (c *RetryClient) Close() error { return c.inner.Close() }

// retryable reports whether an outcome is worth retrying: a transient transport
// error (timeout / unavailable) or a Response carrying a transient status. A nil
// error with an OK response, and every application-level non-OK status, are not
// retried.
func retryable(err error, resp *rpcv1.Response) bool {
	if err != nil {
		switch StatusOf(err) {
		case rpcv1.Status_STATUS_TIMEOUT, rpcv1.Status_STATUS_UNAVAILABLE:
			return true
		default:
			return false
		}
	}
	switch resp.GetStatus() {
	case rpcv1.Status_STATUS_TIMEOUT, rpcv1.Status_STATUS_UNAVAILABLE:
		return true
	default:
		return false
	}
}

// sleepCtx waits for d unless ctx expires first. It returns false if ctx expired
// (the caller should stop). A non-positive d does not wait but still reports
// whether ctx is already done.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
