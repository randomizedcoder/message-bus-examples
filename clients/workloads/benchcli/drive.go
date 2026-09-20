package main

import (
	"bytes"
	"context"
	"fmt"
	"math"
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
// it coordinated-omission-free. saturation is a ramp of open-loop passes that
// finds the max sustainable rate (the knee). coldstart samples the first-request
// latency of fresh connections. fault is an open-loop pass that holds a fixed
// rate straight through a broker/agent disruption (a pod kill, driven alongside
// by nix/chaos-scripts.nix): it never aborts on an error — every request that
// gets no valid reply is counted as a missing sequence, so the integrity
// counters, not latency, are the result (design §8.2, §8.4).
const (
	modeLatency    = "latency"
	modeWindowed   = "windowed"
	modeOpenLoop   = "openloop"
	modeSaturation = "saturation"
	modeColdstart  = "coldstart"
	modeFault      = "fault"
)

// defaultColdstartConns is how many fresh connections coldstart establishes when
// -conns is 0; each contributes one first-request-latency sample (design §8.2).
const defaultColdstartConns = 20

// defaultOpenInflight caps outstanding open-loop requests when -inflight is 0.
// Open-loop keeps sending at its intended instants regardless of replies; the
// cap bounds goroutines and cloned buffers and turns server overload into
// honest back-pressure — a full worker pool stalls the sender, which then
// shows up as late_sends rather than as unbounded memory growth.
const defaultOpenInflight = 1024

// lateThreshold is how late a send may begin, relative to its intended instant,
// before it counts toward late_sends (design §8.2).
const lateThreshold = time.Millisecond

// Saturation ramp parameters (design §8.2): the offered rate doubles every
// step until the p99 rises past saturationP99Mult × floor or the late-send
// fraction exceeds saturationLateFrac; the last sustainable rate is the knee.
// maxRampSteps bounds the ramp so an implausibly fast target still terminates
// (late_sends fires long before this on any real transport).
const (
	defaultStepDuration = 10 * time.Second
	saturationP99Mult   = 10
	saturationLateFrac  = 0.01
	maxRampSteps        = 24
)

// driveConfig is the run-mode configuration for one cell, parsed from the
// transport subcommand's flags.
type driveConfig struct {
	mode     string        // latency | windowed | openloop | saturation | coldstart
	n        int           // message budget (closed modes)
	inflight int           // concurrent workers (closed) / max outstanding (open)
	rate     float64       // offered msg/s (open modes); base rate for saturation
	duration time.Duration // wall-clock budget (openloop)
	step     time.Duration // per-ramp-step window (saturation)
	floor    time.Duration // reference p99 for the saturation ceiling (0 = measure)
	conns    int           // fresh connections (coldstart)
	timeout  time.Duration // per-request timeout
	runID    string
	fixture  string
}

// validate checks the flag combination and normalises inflight/step. It rejects
// the combinations that would silently measure the wrong thing (e.g. -inflight>1
// in latency mode, which is really the windowed experiment).
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
	case modeSaturation:
		if c.rate <= 0 {
			return fmt.Errorf("mode=saturation needs -rate > 0 (the base rate the ramp doubles from)")
		}
		if c.step <= 0 {
			c.step = defaultStepDuration
		}
		if c.floor < 0 {
			return fmt.Errorf("mode=saturation needs -floor >= 0 (0 = measure the floor from the first ramp step)")
		}
		if c.inflight <= 0 {
			c.inflight = defaultOpenInflight
		}
	case modeColdstart:
		if c.conns <= 0 {
			c.conns = defaultColdstartConns
		}
		if c.inflight == 0 {
			c.inflight = 1
		}
		if c.inflight != 1 {
			return fmt.Errorf("mode=coldstart sends one request per fresh connection (inflight=1, got -inflight=%d)", c.inflight)
		}
	case modeFault:
		// fault is open-loop at a fixed rate (design §8.2), so it needs the same
		// -rate/-duration as openloop; the window is meant to span a pod kill.
		if c.rate <= 0 {
			return fmt.Errorf("mode=fault needs -rate > 0 (offered msg/s held through the disruption)")
		}
		if c.duration <= 0 {
			return fmt.Errorf("mode=fault needs -duration > 0 (the window spanning the pod kill)")
		}
		if c.inflight <= 0 {
			c.inflight = defaultOpenInflight
		}
	default:
		return fmt.Errorf("unknown -mode %q (latency|windowed|openloop|saturation|coldstart|fault)", c.mode)
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
	dial      coldDial      // fresh-connection factory (coldstart only; nil otherwise)
	reqProto  proto.Message // request prototype, cloned per worker
	newResp   func() proto.Message
	cenum     workloadsv1.Codec
	tenum     workloadsv1.Transport
	inst      *harness.Instruments
	cell      harness.Cell
}

// coldDial establishes a fresh connection and returns a ready Requester plus a
// teardown that closes the underlying connection (not just the Requester). Only
// coldstart uses it — to measure first-request latency on a cold path (design
// §8.2); the other modes reuse reqCell.requester, so it is nil for them.
type coldDial func() (transport.Requester, func(), error)

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

// series is the mutex-guarded measurement state for one measurement pass. A
// single HDR guarded by a mutex is deliberate: recording is a sub-microsecond
// operation next to a network round-trip, so lock contention is negligible, and
// one shared histogram avoids the per-worker histograms that would cost hundreds
// of MB at inflight=256 or the open-loop pool size. late is written only by the
// open-loop sender goroutine (read after its workers drain), so it needs no lock.
type series struct {
	mu   sync.Mutex
	hdr  *harness.HDR
	late int64
}

func newSeries() *series { return &series{hdr: harness.NewHDR()} }

// record folds one outcome into the pass histogram under the lock. kind==""
// is a success (rtt is recorded, CO-corrected when expected>0); otherwise it is
// an error of that kind and no latency is recorded.
func (s *series) record(rtt time.Duration, kind string, expected time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case kind != "":
		s.hdr.AddErrorKind(kind)
	case expected > 0:
		s.hdr.RecordCorrected(rtt, expected)
	default:
		s.hdr.Record(rtt)
	}
}

// sent is the number of dispatched requests folded into the pass: successes plus
// errors (each do() records exactly one outcome). It is the denominator for the
// late-send fraction.
func (s *series) sent() int64 { return s.hdr.Count() + int64(s.hdr.NumErrors()) }

// satResult carries the saturation ramp's verdict for the print path (it is not
// serialised: the emitted record reports the knee as the cell rate like any
// open-loop cell). floor is the reference p99, knee the last sustainable rate
// (0 if the base rate already saturates), stopRate the rate the ramp stopped at,
// and reason why ("p99" | "late" | "cap").
type satResult struct {
	floor    time.Duration
	knee     float64
	stopRate float64
	reason   string
}

// driver holds the immutable per-cell wiring; each measurement pass gets its own
// series so a saturation ramp can score each step independently.
type driver struct {
	rc  *reqCell
	cfg driveConfig
}

// drive runs the configured mode against rc and returns the finished cell
// measurement for printing + emission.
func drive(ctx context.Context, rc *reqCell, cfg driveConfig, wireReq int64) (cellResult, error) {
	if err := cfg.validate(); err != nil {
		return cellResult{}, err
	}
	d := &driver{rc: rc, cfg: cfg}
	rc.inst.SetActiveCell(ctx, rc.cell, true)
	defer rc.inst.SetActiveCell(ctx, rc.cell, false)

	switch cfg.mode {
	case modeColdstart:
		s := newSeries()
		elapsed, err := d.runColdstart(ctx, s)
		if err != nil {
			return cellResult{}, err
		}
		return cellResult{
			hdr: s.hdr, elapsed: elapsed, wireReq: wireReq, inflight: 1,
		}, nil
	case modeSaturation:
		elapsed, s, sr := d.runSaturation(ctx)
		return cellResult{
			hdr: s.hdr, elapsed: elapsed, wireReq: wireReq,
			inflight: cfg.inflight, rate: sr.knee, lateSends: s.late, sat: sr,
		}, nil
	case modeOpenLoop:
		s := newSeries()
		elapsed := d.openPass(ctx, cfg.rate, cfg.duration, s)
		return cellResult{
			hdr: s.hdr, elapsed: elapsed, wireReq: wireReq,
			inflight: cfg.inflight, rate: cfg.rate, lateSends: s.late,
		}, nil
	case modeFault:
		// The same open-loop engine as openloop, but the errors it tolerates are
		// the point: every request that gets no valid reply during the disruption
		// is surfaced as a missing sequence (the fault integrity counter, design
		// §8.4). duplicate/reordered/redelivered are streaming-tier counters not
		// observable in unary request/reply — one reply per request — so they stay
		// zero here and blank in the report.
		s := newSeries()
		elapsed := d.openPass(ctx, cfg.rate, cfg.duration, s)
		return cellResult{
			hdr: s.hdr, elapsed: elapsed, wireReq: wireReq,
			inflight: cfg.inflight, rate: cfg.rate, lateSends: s.late,
			missing: int64(s.hdr.NumErrors()),
		}, nil
	default: // latency, windowed
		s := newSeries()
		elapsed := d.runClosed(ctx, s)
		return cellResult{
			hdr: s.hdr, elapsed: elapsed, wireReq: wireReq,
			inflight: cfg.inflight, rate: cfg.rate, lateSends: s.late,
		}, nil
	}
}

// runClosed drives inflight workers that each send, wait for the reply, then
// send again until the shared sequence counter reaches n. inflight=1 is the
// latency floor; inflight>1 is the windowed concurrency sweep.
func (d *driver) runClosed(ctx context.Context, s *series) time.Duration {
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
				d.do(ctx, s, j, uint64(i), t0, 0)
			}
		}()
	}
	wg.Wait()
	return time.Since(start)
}

// openPass sends at intended instants start+i/rate for the given duration,
// dispatching each request to a bounded pool of reusable workers. Latency is
// measured from the intended instant (not the actual send), and a send that
// cannot begin within lateThreshold of its instant — because the pool is drained
// by a slow server — counts toward s.late. The CO-corrected series back-fills the
// samples a stalled sender missed at the offered interval (design §8.2).
func (d *driver) openPass(ctx context.Context, rate float64, duration time.Duration, s *series) time.Duration {
	interval := time.Duration(float64(time.Second) / rate)
	if interval < 1 {
		interval = 1
	}
	free := make(chan *job, d.cfg.inflight)
	for i := 0; i < d.cfg.inflight; i++ {
		free <- d.rc.newJob()
	}
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(duration)
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
			s.late++
		}
		wg.Add(1)
		go func(j *job, seq uint64, from time.Time) {
			defer wg.Done()
			d.do(ctx, s, j, seq, from, interval)
			free <- j
		}(j, seq, intended)
	}
	wg.Wait()
	return time.Since(start)
}

// stepSustainable reports whether one saturation ramp step holds: its p99 must
// stay within saturationP99Mult × floor (the p99 bound is skipped when floor is
// 0, i.e. not yet measured) and its late-send fraction within saturationLateFrac.
// reason names the first breached bound ("p99" | "late"), empty when sustainable.
// Both bounds are strict (a step exactly at the ceiling still holds), and p99 is
// checked first so it wins when a saturated step trips both.
func stepSustainable(p99, floor time.Duration, lateFrac float64) (ok bool, reason string) {
	if floor > 0 && p99 > saturationP99Mult*floor {
		return false, "p99"
	}
	if lateFrac > saturationLateFrac {
		return false, "late"
	}
	return true, ""
}

// openPassFn measures one ramp step at the given rate and returns its wall time.
// Production uses driver.openPass; tests inject a deterministic stand-in so the
// ramp decision logic can be exercised without relying on sub-millisecond
// wall-clock timing (which is not reproducible under `go test` load — the real
// driver is taskset-pinned, design §8.3).
type openPassFn func(ctx context.Context, rate float64, duration time.Duration, s *series) time.Duration

// runSaturation ramps the offered rate — doubling every step — over open-loop
// passes until stepSustainable reports a step no longer holds. The knee is the
// last sustainable rate, which is what is reported (not the failing peak, design
// §8.2). When -floor is 0 the floor is taken from the first, lowest-load step.
func (d *driver) runSaturation(ctx context.Context) (time.Duration, *series, *satResult) {
	return d.ramp(ctx, d.openPass)
}

// ramp is the timing-independent ramp loop; pass measures each step. It returns
// the total ramp wall time, the series of the reported step (the knee, or the
// failing first step when even the base rate saturates), and the ramp verdict.
func (d *driver) ramp(ctx context.Context, pass openPassFn) (time.Duration, *series, *satResult) {
	floor := d.cfg.floor
	sr := &satResult{floor: floor}
	var (
		total  time.Duration
		kneeS  *series // last sustainable step
		lastS  *series // most recent step, sustainable or not
		capHit = true  // ramp exhausted maxRampSteps without a failing step
	)
	for i := 0; i < maxRampSteps; i++ {
		rate := d.cfg.rate * math.Pow(2, float64(i))
		s := newSeries()
		total += pass(ctx, rate, d.cfg.step, s)
		lastS = s

		p99 := s.hdr.Summarize(0).P99
		if floor <= 0 && i == 0 {
			floor = p99 // the unloaded first step defines the floor
			sr.floor = floor
		}
		var lateFrac float64
		if sent := s.sent(); sent > 0 {
			lateFrac = float64(s.late) / float64(sent)
		}

		if ok, reason := stepSustainable(p99, floor, lateFrac); !ok {
			sr.stopRate = rate
			sr.reason = reason
			capHit = false
			break
		}
		sr.knee = rate
		kneeS = s
	}
	if capHit {
		// Every step up to the cap was sustainable: report the last as the knee.
		sr.reason = "cap"
		sr.stopRate = sr.knee
	}
	if kneeS == nil {
		// Even the base rate saturated: report the failing first step so the
		// record still carries its distribution, with knee left at 0.
		kneeS = lastS
	}
	return total, kneeS, sr
}

// runColdstart establishes cfg.conns fresh connections in turn and records each
// one's first-request latency, so the distribution is over cold paths (gRPC
// stream setup, NATS reply subscription, AMQP consumer, Valkey group — design
// §8.2). The connect itself is not timed (a failed dial is a "connect" error);
// only the first request's RTT is the sample, so it is comparable to the latency
// floor. Connections are opened one at a time to avoid a dial storm and to keep
// each sample a genuine cold first request rather than a warmed pool.
func (d *driver) runColdstart(ctx context.Context, s *series) (time.Duration, error) {
	if d.rc.dial == nil {
		return 0, fmt.Errorf("mode=coldstart is not supported by this transport (no per-connection dialer)")
	}
	start := time.Now()
	for i := 0; i < d.cfg.conns; i++ {
		r, teardown, err := d.rc.dial()
		if err != nil {
			s.record(0, "connect", 0)
			continue
		}
		j := d.rc.newJob()
		d.doOn(ctx, r, s, j, uint64(i), time.Now(), 0)
		teardown()
	}
	return time.Since(start), nil
}

// do issues one request for sequence seq over the cell's shared Requester.
func (d *driver) do(ctx context.Context, s *series, j *job, seq uint64, from time.Time, expected time.Duration) {
	d.doOn(ctx, d.rc.requester, s, j, seq, from, expected)
}

// doOn issues one request for sequence seq over r using the worker's private job
// and records the outcome into s. from is the instant RTT is measured against
// (the send instant for closed loops, the intended instant for open-loop).
// expected>0 additionally records the coordinated-omission-corrected series.
// coldstart passes a fresh per-connection Requester as r; the other modes pass
// the cell's shared one via do.
func (d *driver) doOn(ctx context.Context, r transport.Requester, s *series, j *job, seq uint64, from time.Time, expected time.Duration) {
	rc := d.rc
	if err := envelope.Fill(j.env, d.cfg.runID, seq, rc.cenum, rc.tenum, d.cfg.fixture); err != nil {
		s.record(0, "encode", expected)
		return
	}
	proto.Reset(j.resp)
	rctx, cancel := context.WithTimeout(ctx, d.cfg.timeout)
	err := r.Request(rctx, j.req, j.resp)
	cancel()
	rtt := time.Since(from)
	rc.inst.Message(ctx, rc.cell, harness.ResultSent)
	if err != nil {
		rc.inst.Error(ctx, rc.cell, "transport")
		s.record(0, "transport", expected)
		return
	}
	if re := envelope.Of(j.resp); re == nil || !bytes.Equal(re.GetMessageId(), j.env.GetMessageId()) {
		rc.inst.Error(ctx, rc.cell, "corrupt")
		s.record(0, "corrupt", expected)
		return
	}
	rc.inst.Message(ctx, rc.cell, harness.ResultReceived)
	rc.inst.RTT(ctx, rc.cell, rtt)
	s.record(rtt, "", expected)
}
