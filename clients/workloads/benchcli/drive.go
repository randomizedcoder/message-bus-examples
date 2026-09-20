package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// Run modes (design §8.2). latency and windowed are closed loops (each worker
// waits for its reply before sending the next); openloop is an open loop that
// sends at fixed intended instants regardless of replies, which is what makes
// it coordinated-omission-free. saturation/coldstart/fault land in a later
// slice (they need a rate ramp, a per-sample dial factory, and harness pod-kill
// orchestration respectively).
const (
	modeLatency  = "latency"
	modeWindowed = "windowed"
	modeOpenLoop = "openloop"
)

// defaultOpenInflight caps outstanding open-loop requests when -inflight is 0.
// Open-loop keeps sending at its intended instants regardless of replies; the
// cap bounds goroutines and cloned buffers and turns server overload into
// honest back-pressure — a full worker pool stalls the sender, which then
// shows up as late_sends rather than as unbounded memory growth.
const defaultOpenInflight = 1024

// lateThreshold is how late a send may begin, relative to its intended instant,
// before it counts toward late_sends (design §8.2).
const lateThreshold = time.Millisecond

// driveConfig is the run-mode configuration for one cell, parsed from the
// transport subcommand's flags.
type driveConfig struct {
	mode     string        // latency | windowed | openloop
	n        int           // message budget (closed modes)
	inflight int           // concurrent workers (closed) / max outstanding (open)
	rate     float64       // offered msg/s (open modes)
	duration time.Duration // wall-clock budget (open modes)
	timeout  time.Duration // per-request timeout
	runID    string
	fixture  string
}

// validate checks the flag combination and normalises inflight. It rejects the
// combinations that would silently measure the wrong thing (e.g. -inflight>1 in
// latency mode, which is really the windowed experiment).
func (c *driveConfig) validate() error {
	switch c.mode {
	case modeLatency:
		if c.inflight == 0 {
			c.inflight = 1
		}
		if c.inflight != 1 {
			return fmt.Errorf("mode=latency is closed-loop inflight=1 (got -inflight=%d); use -mode=windowed for concurrency", c.inflight)
		}
		if c.n <= 0 {
			return fmt.Errorf("mode=latency needs -n > 0")
		}
	case modeWindowed:
		if c.inflight < 2 {
			return fmt.Errorf("mode=windowed needs -inflight >= 2 (design sweeps 16|64|256); use -mode=latency for inflight=1")
		}
		if c.n <= 0 {
			return fmt.Errorf("mode=windowed needs -n > 0")
		}
	case modeOpenLoop:
		if c.rate <= 0 {
			return fmt.Errorf("mode=openloop needs -rate > 0 (offered msg/s)")
		}
		if c.duration <= 0 {
			return fmt.Errorf("mode=openloop needs -duration > 0")
		}
		if c.inflight <= 0 {
			c.inflight = defaultOpenInflight
		}
	default:
		return fmt.Errorf("unknown -mode %q (latency|windowed|openloop)", c.mode)
	}
	return nil
}

// reqCell is what the run-mode driver needs from a request/reply transport: a
// concurrency-safe Requester, a request prototype to clone per worker, a
// fresh-response constructor, the codec/transport enums for envelope stamping,
// and the metrics instruments + cell. gRPC and every request/reply bus build
// one of these and share the identical driver.
type reqCell struct {
	requester transport.Requester
	reqProto  proto.Message // request prototype, cloned per worker
	newResp   func() proto.Message
	cenum     workloadsv1.Codec
	tenum     workloadsv1.Transport
	inst      *harness.Instruments
	cell      harness.Cell
}

// job is one worker's private request/response pair, so many workers can drive
// a single concurrency-safe Requester without sharing mutable proto messages.
type job struct {
	req  proto.Message
	resp proto.Message
	env  *workloadsv1.Envelope
}

func (rc *reqCell) newJob() *job {
	req := proto.Clone(rc.reqProto)
	return &job{req: req, resp: rc.newResp(), env: req.(hasEnvelope).GetEnvelope()}
}

// driver holds the shared, mutex-guarded measurement state for one cell. A
// single HDR guarded by a mutex is deliberate: recording is a sub-microsecond
// operation next to a network round-trip, so lock contention is negligible,
// and one shared histogram avoids the per-worker histograms that would cost
// hundreds of MB at inflight=256 or the open-loop pool size.
type driver struct {
	rc        *reqCell
	cfg       driveConfig
	mu        sync.Mutex
	hdr       *harness.HDR
	lateSends int64
}

// drive runs the configured mode against rc and returns the finished cell
// measurement for printing + emission.
func drive(ctx context.Context, rc *reqCell, cfg driveConfig, wireReq int64) (cellResult, error) {
	if err := cfg.validate(); err != nil {
		return cellResult{}, err
	}
	d := &driver{rc: rc, cfg: cfg, hdr: harness.NewHDR()}
	rc.inst.SetActiveCell(ctx, rc.cell, true)
	defer rc.inst.SetActiveCell(ctx, rc.cell, false)
	var elapsed time.Duration
	if cfg.mode == modeOpenLoop {
		elapsed = d.runOpen(ctx)
	} else {
		elapsed = d.runClosed(ctx)
	}
	return cellResult{
		hdr: d.hdr, elapsed: elapsed, wireReq: wireReq,
		inflight: cfg.inflight, rate: cfg.rate, lateSends: atomic.LoadInt64(&d.lateSends),
	}, nil
}

// runClosed drives inflight workers that each send, wait for the reply, then
// send again until the shared sequence counter reaches n. inflight=1 is the
// latency floor; inflight>1 is the windowed concurrency sweep.
func (d *driver) runClosed(ctx context.Context) time.Duration {
	var seq int64 = -1
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < d.cfg.inflight; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j := d.rc.newJob()
			for {
				i := atomic.AddInt64(&seq, 1)
				if i >= int64(d.cfg.n) {
					return
				}
				t0 := time.Now()
				d.do(ctx, j, uint64(i), t0, 0)
			}
		}()
	}
	wg.Wait()
	return time.Since(start)
}

// runOpen sends at intended instants start+i/rate, dispatching each request to
// a bounded pool of reusable workers. Latency is measured from the intended
// instant (not the actual send), and a send that cannot begin within
// lateThreshold of its instant — because the pool is drained by a slow server —
// counts toward late_sends. The CO-corrected series back-fills the samples a
// stalled sender missed at the offered interval (design §8.2).
func (d *driver) runOpen(ctx context.Context) time.Duration {
	interval := time.Duration(float64(time.Second) / d.cfg.rate)
	if interval < 1 {
		interval = 1
	}
	free := make(chan *job, d.cfg.inflight)
	for i := 0; i < d.cfg.inflight; i++ {
		free <- d.rc.newJob()
	}
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(d.cfg.duration)
	for seq := uint64(0); ; seq++ {
		intended := start.Add(time.Duration(seq) * interval)
		if !intended.Before(deadline) {
			break
		}
		if wait := time.Until(intended); wait > 0 {
			time.Sleep(wait)
		}
		// Blocking here means the offered load exceeds what the pool can
		// absorb: the sender falls behind its schedule, which is exactly what
		// late_sends and the CO correction are meant to expose.
		j := <-free
		if time.Since(intended) > lateThreshold {
			atomic.AddInt64(&d.lateSends, 1)
		}
		wg.Add(1)
		go func(j *job, seq uint64, from time.Time) {
			defer wg.Done()
			d.do(ctx, j, seq, from, interval)
			free <- j
		}(j, seq, intended)
	}
	wg.Wait()
	return time.Since(start)
}

// do issues one request for sequence seq using the worker's private job and
// records the outcome. from is the instant RTT is measured against (the send
// instant for closed loops, the intended instant for open-loop). expected>0
// additionally records the coordinated-omission-corrected series.
func (d *driver) do(ctx context.Context, j *job, seq uint64, from time.Time, expected time.Duration) {
	rc := d.rc
	if err := envelope.Fill(j.env, d.cfg.runID, seq, rc.cenum, rc.tenum, d.cfg.fixture); err != nil {
		d.record(0, "encode", expected)
		return
	}
	proto.Reset(j.resp)
	rctx, cancel := context.WithTimeout(ctx, d.cfg.timeout)
	err := rc.requester.Request(rctx, j.req, j.resp)
	cancel()
	rtt := time.Since(from)
	rc.inst.Message(ctx, rc.cell, harness.ResultSent)
	if err != nil {
		rc.inst.Error(ctx, rc.cell, "transport")
		d.record(0, "transport", expected)
		return
	}
	if re := envelope.Of(j.resp); re == nil || !bytes.Equal(re.GetMessageId(), j.env.GetMessageId()) {
		rc.inst.Error(ctx, rc.cell, "corrupt")
		d.record(0, "corrupt", expected)
		return
	}
	rc.inst.Message(ctx, rc.cell, harness.ResultReceived)
	rc.inst.RTT(ctx, rc.cell, rtt)
	d.record(rtt, "", expected)
}

// record folds one outcome into the shared histogram under the lock. kind==""
// is a success (rtt is recorded, CO-corrected when expected>0); otherwise it is
// an error of that kind and no latency is recorded.
func (d *driver) record(rtt time.Duration, kind string, expected time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case kind != "":
		d.hdr.AddErrorKind(kind)
	case expected > 0:
		d.hdr.RecordCorrected(rtt, expected)
	default:
		d.hdr.Record(rtt)
	}
}
