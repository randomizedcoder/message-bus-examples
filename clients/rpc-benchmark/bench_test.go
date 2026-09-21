package main

import (
	"context"
	"testing"
	"time"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
)

// fakeClient is an in-process benchClient: it sleeps delay then returns the
// configured outcome, so the driver's loops are exercised without a network.
type fakeClient struct {
	delay  time.Duration
	status rpcv1.Status // returned when err == nil
	err    error        // when non-nil, Call returns it
}

func (f fakeClient) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &rpcv1.Response{RequestId: req.GetRequestId(), Status: f.status}, nil
}

func newReqFn() func() *rpcv1.Request {
	payload := &rpcv1.Request{} // any proto message works as an opaque payload here
	return func() *rpcv1.Request {
		r, err := rpc.NewRequest("customer", "Lookup", payload, time.Second)
		if err != nil {
			panic(err)
		}
		return r
	}
}

func TestBenchConfigValidate(t *testing.T) {
	tests := []struct {
		description string
		cfg         benchConfig
		wantErr     bool
	}{
		{
			description: "valid closed config",
			cfg:         benchConfig{mode: modeClosed, requests: 100, concurrency: 4, timeout: time.Second},
			wantErr:     false,
		},
		{
			description: "valid open config",
			cfg:         benchConfig{mode: modeOpen, rate: 100, duration: time.Second, timeout: time.Second},
			wantErr:     false,
		},
		{
			description: "closed without requests is invalid",
			cfg:         benchConfig{mode: modeClosed, requests: 0, concurrency: 4, timeout: time.Second},
			wantErr:     true,
		},
		{
			description: "closed without concurrency is invalid",
			cfg:         benchConfig{mode: modeClosed, requests: 100, concurrency: 0, timeout: time.Second},
			wantErr:     true,
		},
		{
			description: "open without rate is invalid",
			cfg:         benchConfig{mode: modeOpen, rate: 0, duration: time.Second, timeout: time.Second},
			wantErr:     true,
		},
		{
			description: "open without duration is invalid",
			cfg:         benchConfig{mode: modeOpen, rate: 100, duration: 0, timeout: time.Second},
			wantErr:     true,
		},
		{
			description: "non-positive timeout is invalid",
			cfg:         benchConfig{mode: modeClosed, requests: 100, concurrency: 4, timeout: 0},
			wantErr:     true,
		},
		{
			description: "unknown mode is invalid",
			cfg:         benchConfig{mode: "sweep", timeout: time.Second},
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			err := tt.cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate() err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestRunBenchClosed(t *testing.T) {
	tests := []struct {
		description  string
		client       fakeClient
		wantOK       int
		wantErrs     bool
		wantTimeouts bool
		wantNonOK    bool
	}{
		{
			description: "all-OK client records every request as a success",
			client:      fakeClient{status: rpcv1.Status_STATUS_OK},
			wantOK:      200,
		},
		{
			description:  "transport-timeout error is counted as a timeout, not a latency",
			client:       fakeClient{err: context.DeadlineExceeded},
			wantOK:       0,
			wantErrs:     true,
			wantTimeouts: true,
		},
		{
			description: "opaque transport error is counted as an error",
			client:      fakeClient{err: rpc.ErrUnavailable},
			wantOK:      0,
			wantErrs:    true,
		},
		{
			description: "non-OK response (NOT_FOUND) is counted as non-ok",
			client:      fakeClient{status: rpcv1.Status_STATUS_NOT_FOUND},
			wantOK:      0,
			wantErrs:    true,
			wantNonOK:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			cfg := benchConfig{mode: modeClosed, requests: 200, concurrency: 8, timeout: time.Second}
			res := runBench(context.Background(), tt.client, newReqFn(), cfg, nil)
			if res.Summary.Count != tt.wantOK {
				t.Errorf("ok count = %d, want %d", res.Summary.Count, tt.wantOK)
			}
			if (res.Summary.Errors > 0) != tt.wantErrs {
				t.Errorf("errors = %d, wantErrs = %v", res.Summary.Errors, tt.wantErrs)
			}
			if (res.Timeouts > 0) != tt.wantTimeouts {
				t.Errorf("timeouts = %d, wantTimeouts = %v", res.Timeouts, tt.wantTimeouts)
			}
			if (res.NonOK > 0) != tt.wantNonOK {
				t.Errorf("nonOK = %d, wantNonOK = %v", res.NonOK, tt.wantNonOK)
			}
			// The whole budget is always accounted for (ok + errors == requests).
			if got := res.Summary.Count + res.Summary.Errors; got != cfg.requests {
				t.Errorf("accounted %d, want %d", got, cfg.requests)
			}
		})
	}
}

func TestRunBenchOpen(t *testing.T) {
	cfg := benchConfig{mode: modeOpen, rate: 500, duration: 200 * time.Millisecond, timeout: time.Second}
	client := fakeClient{delay: time.Millisecond, status: rpcv1.Status_STATUS_OK}
	res := runBench(context.Background(), client, newReqFn(), cfg, nil)

	if res.Summary.Count == 0 {
		t.Fatalf("open-loop produced no samples")
	}
	// ~500/s for 200ms ≈ 100 requests; allow a wide band for scheduling jitter.
	if res.Summary.Count < 20 {
		t.Errorf("open-loop count = %d, want a meaningful number (>=20)", res.Summary.Count)
	}
	// The open loop CO-corrects, so a corrected series must exist.
	if !res.CorrectedOK {
		t.Errorf("open-loop did not record a coordinated-omission-corrected series")
	}
}

func TestRunBenchOpenCancel(t *testing.T) {
	// A cancelled context ends an open-loop run promptly rather than sending for
	// the full duration.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := benchConfig{mode: modeOpen, rate: 1000, duration: 10 * time.Second, timeout: time.Second}
	client := fakeClient{status: rpcv1.Status_STATUS_OK}

	done := make(chan result, 1)
	go func() { done <- runBench(ctx, client, newReqFn(), cfg, nil) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("open-loop did not honour context cancellation")
	}
}
