package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/rpcmetrics"
)

// Run modes. closed is a closed loop — `concurrency` workers, each sends the
// next request only after its reply arrives, draining a shared request budget;
// it measures service latency under a fixed concurrency. open is an open loop —
// requests are offered at a fixed rate for a fixed duration regardless of when
// replies arrive, and each sample is coordinated-omission-corrected against its
// intended send instant (§25), so a stall widens the tail honestly.
const (
	modeClosed = "closed"
	modeOpen   = "open"
)

// benchClient is the slice of rpc.Client the driver needs; a real grpcx.Client
// satisfies it, and tests supply an in-process fake so the loop is exercised
// without a network.
type benchClient interface {
	Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error)
}

// benchConfig is one measurement cell.
type benchConfig struct {
	mode        string        // closed | open
	requests    int           // closed: total request budget
	concurrency int           // closed: number of workers
	rate        float64       // open: offered requests/sec
	duration    time.Duration // open: wall-clock budget
	timeout     time.Duration // per-call timeout
}

func (c benchConfig) validate() error {
	if c.timeout <= 0 {
		return fmt.Errorf("timeout must be > 0")
	}
	switch c.mode {
	case modeClosed:
		if c.requests <= 0 {
			return fmt.Errorf("mode=closed needs -requests > 0")
		}
		if c.concurrency <= 0 {
			return fmt.Errorf("mode=closed needs -concurrency > 0")
		}
	case modeOpen:
		if c.rate <= 0 {
			return fmt.Errorf("mode=open needs -rate > 0 (offered req/s)")
		}
		if c.duration <= 0 {
			return fmt.Errorf("mode=open needs -duration > 0")
		}
	default:
		return fmt.Errorf("unknown -mode %q (closed|open)", c.mode)
	}
	return nil
}

// result is what a cell reports.
type result struct {
	Summary      harness.Summary
	CorrectedP99 time.Duration
	CorrectedOK  bool
	Timeouts     int64
	NonOK        int64
	// Retries is the number of retry attempts the RetryClient issued during the
	// cell (0 when -retries is unset); Replays is the number of OK responses the
	// service served from its idempotency cache (§29 duplicate logical operations).
	Retries int64
	Replays int64
	Elapsed time.Duration
	// hdr is the raw accumulator, retained so run.json can render the p95 and the
	// full .hgrm distribution the reduced Summary does not carry.
	hdr *harness.HDR
}

// classify records one call outcome into hdr, returning whether it was a success
// (an OK response within the deadline). It also bumps the timeout / non-OK
// counters the printed report separates out, and mirrors the outcome to sink
// (the rpc_* Prometheus metrics; nil when -metrics-addr is unset).
func classify(hdr *harness.HDR, rtt time.Duration, resp *rpcv1.Response, err error, timeouts, nonOK, replays *int64, corrected bool, expected time.Duration, sink *metricsSink) bool {
	switch {
	case err != nil:
		if isTimeout(err) {
			atomic.AddInt64(timeouts, 1)
			hdr.AddErrorKind("timeout")
			sink.record(rpcmetrics.ResultTimeout, nil, rtt)
		} else {
			hdr.AddErrorKind("error")
			sink.record(rpcmetrics.ResultError, nil, rtt)
		}
		return false
	case resp.GetStatus() != rpcv1.Status_STATUS_OK:
		if resp.GetStatus() == rpcv1.Status_STATUS_TIMEOUT {
			atomic.AddInt64(timeouts, 1)
			hdr.AddErrorKind("timeout")
			sink.record(rpcmetrics.ResultTimeout, resp, rtt)
		} else {
			atomic.AddInt64(nonOK, 1)
			hdr.AddErrorKind("non-ok")
			sink.record(rpcmetrics.ResultNonOK, resp, rtt)
		}
		return false
	default:
		if corrected {
			hdr.RecordCorrected(rtt, expected)
		} else {
			hdr.Record(rtt)
		}
		if isReplay(resp) {
			atomic.AddInt64(replays, 1)
		}
		sink.record(rpcmetrics.ResultOK, resp, rtt)
		return true
	}
}

func isTimeout(err error) bool {
	return rpc.StatusOf(err) == rpcv1.Status_STATUS_TIMEOUT
}

// runBench drives one cell. newReq builds a fresh Request per call (a new
// request_id each time) so the server sees distinct logical operations.
func runBench(ctx context.Context, client benchClient, newReq func() *rpcv1.Request, cfg benchConfig, sink *metricsSink) result {
	hdr := harness.NewHDR()
	var timeouts, nonOK, replays int64
	var elapsed time.Duration

	// A RetryClient counts its retries cumulatively; snapshot the delta over this
	// cell so run.json reports per-cell retries (§29).
	rc, _ := client.(retryCounter)
	var retriesBefore uint64
	if rc != nil {
		retriesBefore = rc.Retries()
	}

	switch cfg.mode {
	case modeClosed:
		elapsed = runClosed(ctx, client, newReq, cfg, hdr, &timeouts, &nonOK, &replays, sink)
	case modeOpen:
		elapsed = runOpen(ctx, client, newReq, cfg, hdr, &timeouts, &nonOK, &replays, sink)
	}

	res := result{
		Summary:  hdr.Summarize(elapsed),
		Timeouts: timeouts,
		NonOK:    nonOK,
		Replays:  replays,
		Elapsed:  elapsed,
		hdr:      hdr,
	}
	if rc != nil {
		res.Retries = int64(rc.Retries() - retriesBefore)
	}
	res.CorrectedP99, res.CorrectedOK = hdr.CorrectedP99()
	return res
}

// retryCounter is the optional interface a wrapped client implements to report
// its cumulative retry count (satisfied by *rpc.RetryClient).
type retryCounter interface {
	Retries() uint64
}

// runClosed spawns cfg.concurrency workers draining a shared budget of
// cfg.requests, each issuing a timed unary Call and blocking for its reply.
func runClosed(ctx context.Context, client benchClient, newReq func() *rpcv1.Request, cfg benchConfig, hdr *harness.HDR, timeouts, nonOK, replays *int64, sink *metricsSink) time.Duration {
	var remaining atomic.Int64
	remaining.Store(int64(cfg.requests))

	var mu sync.Mutex // serialises hdr.Record (HDR is not concurrency-safe)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < cfg.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				if ctx.Err() != nil {
					return
				}
				rtt, resp, err := oneCall(ctx, client, newReq(), cfg.timeout)
				mu.Lock()
				classify(hdr, rtt, resp, err, timeouts, nonOK, replays, false, 0, sink)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return time.Since(start)
}

// runOpen offers requests at cfg.rate for cfg.duration, capping outstanding work
// at a pool sized from the rate so a stalled server cannot spawn unbounded
// goroutines, and CO-corrects each sample against its intended send instant.
func runOpen(ctx context.Context, client benchClient, newReq func() *rpcv1.Request, cfg benchConfig, hdr *harness.HDR, timeouts, nonOK, replays *int64, sink *metricsSink) time.Duration {
	pool := int(cfg.rate/10) + 1
	if pool > 4096 {
		pool = 4096
	}
	free := make(chan struct{}, pool)
	for i := 0; i < pool; i++ {
		free <- struct{}{}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	interval := time.Duration(float64(time.Second) / cfg.rate)
	start := time.Now()
	deadline := start.Add(cfg.duration)

	for i := 0; ; i++ {
		intended := start.Add(time.Duration(i) * interval)
		if intended.After(deadline) {
			break
		}
		if d := time.Until(intended); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				goto done
			}
		}
		select {
		case <-free:
		case <-ctx.Done():
			goto done
		}
		wg.Add(1)
		go func(intended time.Time) {
			defer wg.Done()
			expected := time.Since(intended) // how late the send already is
			rtt, resp, err := oneCall(ctx, client, newReq(), cfg.timeout)
			mu.Lock()
			classify(hdr, rtt+expected, resp, err, timeouts, nonOK, replays, true, rtt+expected, sink)
			mu.Unlock()
			free <- struct{}{}
		}(intended)
	}
done:
	wg.Wait()
	return time.Since(start)
}

// oneCall issues a single unary Call bounded by timeout and returns its RTT.
func oneCall(ctx context.Context, client benchClient, req *rpcv1.Request, timeout time.Duration) (time.Duration, *rpcv1.Response, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t0 := time.Now()
	resp, err := client.Call(cctx, req)
	return time.Since(t0), resp, err
}
