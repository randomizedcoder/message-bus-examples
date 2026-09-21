package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// scriptedClient is a fake Client that returns a pre-programmed outcome per
// attempt (the last outcome repeats for any further attempts) and records every
// Request it was handed, so a test can assert both the final result and how the
// retry loop rewrote request_id / idempotency_key across attempts.
type scriptedClient struct {
	outcomes []outcome
	seen     []*rpcv1.Request
}

type outcome struct {
	resp *rpcv1.Response
	err  error
}

func (c *scriptedClient) Call(_ context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	i := len(c.seen)
	c.seen = append(c.seen, req)
	if i >= len(c.outcomes) {
		i = len(c.outcomes) - 1
	}
	return c.outcomes[i].resp, c.outcomes[i].err
}

func (c *scriptedClient) Capabilities() Capabilities { return Capabilities{} }
func (c *scriptedClient) Close() error               { return nil }

func respOK() *rpcv1.Response               { return &rpcv1.Response{Status: rpcv1.Status_STATUS_OK} }
func status(s rpcv1.Status) *rpcv1.Response { return &rpcv1.Response{Status: s} }

func TestRetryClientCall(t *testing.T) {
	timeoutErr := Errorf(rpcv1.Status_STATUS_TIMEOUT, "deadline")
	internalErr := errors.New("boom") // StatusOf → STATUS_INTERNAL, not retryable

	tests := []struct {
		description string
		outcomes    []outcome
		maxAttempts int
		wantCalls   int          // attempts actually issued to the inner client
		wantRetries uint64       // retry counter delta
		wantStatus  rpcv1.Status // final response status (STATUS_UNSPECIFIED if err path)
		wantErr     bool
	}{
		{
			description: "OK on the first attempt issues one call and no retry",
			outcomes:    []outcome{{resp: respOK()}},
			maxAttempts: 3, wantCalls: 1, wantRetries: 0, wantStatus: rpcv1.Status_STATUS_OK,
		},
		{
			description: "a transport timeout then OK is one retry",
			outcomes:    []outcome{{err: timeoutErr}, {resp: respOK()}},
			maxAttempts: 3, wantCalls: 2, wantRetries: 1, wantStatus: rpcv1.Status_STATUS_OK,
		},
		{
			description: "an UNAVAILABLE status response then OK is one retry",
			outcomes:    []outcome{{resp: status(rpcv1.Status_STATUS_UNAVAILABLE)}, {resp: respOK()}},
			maxAttempts: 3, wantCalls: 2, wantRetries: 1, wantStatus: rpcv1.Status_STATUS_OK,
		},
		{
			description: "exhausting the budget returns the last transient failure",
			outcomes:    []outcome{{err: timeoutErr}},
			maxAttempts: 2, wantCalls: 2, wantRetries: 1, wantErr: true,
		},
		{
			description: "an application non-OK status is returned without retrying",
			outcomes:    []outcome{{resp: status(rpcv1.Status_STATUS_NOT_FOUND)}, {resp: respOK()}},
			maxAttempts: 3, wantCalls: 1, wantRetries: 0, wantStatus: rpcv1.Status_STATUS_NOT_FOUND,
		},
		{
			description: "a non-transient transport error is returned without retrying",
			outcomes:    []outcome{{err: internalErr}, {resp: respOK()}},
			maxAttempts: 3, wantCalls: 1, wantRetries: 0, wantErr: true,
		},
		{
			description: "MaxAttempts<=1 disables retry even on a timeout",
			outcomes:    []outcome{{err: timeoutErr}},
			maxAttempts: 1, wantCalls: 1, wantRetries: 0, wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			fake := &scriptedClient{outcomes: tt.outcomes}
			rc := NewRetryClient(fake, RetryPolicy{MaxAttempts: tt.maxAttempts})
			req, err := NewRequest("echo", "Echo", &rpcv1.Request{}, time.Second)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			resp, err := rc.Call(context.Background(), req)

			if (err != nil) != tt.wantErr {
				t.Fatalf("Call err = %v, wantErr = %v", err, tt.wantErr)
			}
			if len(fake.seen) != tt.wantCalls {
				t.Errorf("issued %d calls, want %d", len(fake.seen), tt.wantCalls)
			}
			if got := rc.Retries(); got != tt.wantRetries {
				t.Errorf("Retries() = %d, want %d", got, tt.wantRetries)
			}
			if !tt.wantErr && resp.GetStatus() != tt.wantStatus {
				t.Errorf("status = %v, want %v", resp.GetStatus(), tt.wantStatus)
			}
		})
	}
}

func TestRetryClientIdempotencyKeyStableAcrossAttempts(t *testing.T) {
	tests := []struct {
		description string
		inKey       string // idempotency_key the caller set (empty = none)
	}{
		{description: "a caller-supplied key is reused on every attempt", inKey: "create-order-123"},
		{description: "a missing key is minted once and held stable", inKey: ""},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			// Two transient failures then OK ⇒ three attempts.
			fake := &scriptedClient{outcomes: []outcome{
				{err: Errorf(rpcv1.Status_STATUS_TIMEOUT, "t")},
				{err: Errorf(rpcv1.Status_STATUS_UNAVAILABLE, "u")},
				{resp: respOK()},
			}}
			rc := NewRetryClient(fake, RetryPolicy{MaxAttempts: 3})
			req, err := NewRequest("echo", "Echo", &rpcv1.Request{}, time.Second)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			req.IdempotencyKey = tt.inKey
			if _, err := rc.Call(context.Background(), req); err != nil {
				t.Fatalf("Call: %v", err)
			}
			if len(fake.seen) != 3 {
				t.Fatalf("issued %d attempts, want 3", len(fake.seen))
			}

			// idempotency_key: identical, non-empty, on all three attempts.
			key := fake.seen[0].GetIdempotencyKey()
			if key == "" {
				t.Fatalf("first attempt has no idempotency_key")
			}
			if tt.inKey != "" && key != tt.inKey {
				t.Errorf("idempotency_key = %q, want caller's %q", key, tt.inKey)
			}
			for i, r := range fake.seen {
				if r.GetIdempotencyKey() != key {
					t.Errorf("attempt %d idempotency_key = %q, want %q", i+1, r.GetIdempotencyKey(), key)
				}
			}

			// request_id: distinct across the three attempts (§29 — one per attempt).
			ids := map[string]int{}
			for _, r := range fake.seen {
				ids[r.GetRequestId()]++
			}
			if len(ids) != 3 {
				t.Errorf("got %d distinct request_ids across 3 attempts, want 3 (%v)", len(ids), ids)
			}
		})
	}
}

func TestRetryClientStopsWhenContextExpiresDuringBackoff(t *testing.T) {
	fake := &scriptedClient{outcomes: []outcome{{err: Errorf(rpcv1.Status_STATUS_TIMEOUT, "t")}}}
	rc := NewRetryClient(fake, RetryPolicy{MaxAttempts: 5, Backoff: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already expired

	req, err := NewRequest("echo", "Echo", &rpcv1.Request{}, time.Second)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	done := make(chan struct{})
	go func() {
		_, _ = rc.Call(ctx, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Call blocked in backoff instead of honoring the cancelled context")
	}
	// One attempt failed transiently, then the cancelled ctx aborted the backoff
	// before a second attempt was issued.
	if len(fake.seen) != 1 {
		t.Errorf("issued %d attempts, want 1 (backoff must abort on ctx cancel)", len(fake.seen))
	}
}
