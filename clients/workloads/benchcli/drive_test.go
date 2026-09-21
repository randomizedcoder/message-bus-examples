package main

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// echoRequester is an in-process transport.Requester that echoes the request's
// message_id into the response envelope (so the driver's correctness check
// passes), optionally after a delay and optionally failing every failEvery-th
// call. It is concurrency-safe, like the real Requesters the driver targets.
type echoRequester struct {
	delay     time.Duration
	failEvery int64
	calls     atomic.Int64
	peak      atomic.Int64 // highest observed concurrency
	inflight  atomic.Int64
}

func (e *echoRequester) Request(ctx context.Context, req, resp proto.Message) error {
	cur := e.inflight.Add(1)
	for {
		p := e.peak.Load()
		if cur <= p || e.peak.CompareAndSwap(p, cur) {
			break
		}
	}
	defer e.inflight.Add(-1)
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	n := e.calls.Add(1)
	if e.failEvery > 0 && n%e.failEvery == 0 {
		return errors.New("injected failure")
	}
	reqEnv := envelope.Of(req)
	resp.(*workloadsv1.DeployResponse).Envelope = &workloadsv1.Envelope{
		MessageId: append([]byte(nil), reqEnv.GetMessageId()...),
	}
	return nil
}

func (e *echoRequester) Close() error { return nil }

// newTestCell builds a reqCell wired to req, with a real instrument set on an
// off (no-serve) provider so the record paths run.
func newTestCell(t *testing.T, req *echoRequester, mode string) *reqCell {
	t.Helper()
	mp, _, err := metrics.NewProvider("")
	if err != nil {
		t.Fatalf("metrics provider: %v", err)
	}
	inst, err := harness.New(mp.Meter("test"))
	if err != nil {
		t.Fatalf("instruments: %v", err)
	}
	return &reqCell{
		requester: req,
		reqProto:  &workloadsv1.DeployRequest{Envelope: &workloadsv1.Envelope{}},
		newResp:   func() proto.Message { return &workloadsv1.DeployResponse{} },
		cenum:     workloadsv1.Codec_CODEC_PROTO,
		tenum:     workloadsv1.Transport_TRANSPORT_GRPC_UNARY,
		inst:      inst,
		cell:      harness.Cell{Transport: "grpc_unary", Codec: "proto", Fixture: "medium", Pool: "all", GC: "default", Mode: mode, Role: "client", Tier: "rpc"},
	}
}

func TestDriveConfigValidate(t *testing.T) {
	tests := []struct {
		description  string
		cfg          driveConfig
		wantErr      bool
		wantInflight int // expected normalised inflight when no error
	}{
		{"latency inflight=1 is valid", driveConfig{mode: modeLatency, n: 10, inflight: 1}, false, 1},
		{"latency inflight=0 defaults to 1", driveConfig{mode: modeLatency, n: 10, inflight: 0}, false, 1},
		{"latency with inflight>1 is rejected", driveConfig{mode: modeLatency, n: 10, inflight: 4}, true, 0},
		{"latency needs n>0", driveConfig{mode: modeLatency, n: 0, inflight: 1}, true, 0},
		{"windowed needs inflight>=2", driveConfig{mode: modeWindowed, n: 10, inflight: 1}, true, 0},
		{"windowed inflight=16 is valid", driveConfig{mode: modeWindowed, n: 10, inflight: 16}, false, 16},
		{"openloop needs rate>0", driveConfig{mode: modeOpenLoop, rate: 0, duration: time.Second}, true, 0},
		{"openloop needs duration>0", driveConfig{mode: modeOpenLoop, rate: 100, duration: 0}, true, 0},
		{"openloop defaults the inflight cap", driveConfig{mode: modeOpenLoop, rate: 100, duration: time.Second, inflight: 0}, false, defaultOpenInflight},
		{"openloop honours an explicit cap", driveConfig{mode: modeOpenLoop, rate: 100, duration: time.Second, inflight: 32}, false, 32},
		{"saturation needs rate>0", driveConfig{mode: modeSaturation, rate: 0, step: time.Second}, true, 0},
		{"saturation defaults the inflight cap", driveConfig{mode: modeSaturation, rate: 100, step: time.Second, inflight: 0}, false, defaultOpenInflight},
		{"saturation honours an explicit cap", driveConfig{mode: modeSaturation, rate: 100, step: time.Second, inflight: 8}, false, 8},
		{"saturation rejects a negative floor", driveConfig{mode: modeSaturation, rate: 100, step: time.Second, floor: -1}, true, 0},
		{"coldstart inflight=0 defaults to 1", driveConfig{mode: modeColdstart, conns: 20, inflight: 0}, false, 1},
		{"coldstart rejects inflight>1", driveConfig{mode: modeColdstart, conns: 20, inflight: 4}, true, 0},
		{"fault needs rate>0", driveConfig{mode: modeFault, rate: 0, duration: time.Second}, true, 0},
		{"fault needs duration>0", driveConfig{mode: modeFault, rate: 100, duration: 0}, true, 0},
		{"fault defaults the inflight cap", driveConfig{mode: modeFault, rate: 100, duration: time.Second, inflight: 0}, false, defaultOpenInflight},
		{"fault honours an explicit cap", driveConfig{mode: modeFault, rate: 100, duration: time.Second, inflight: 16}, false, 16},
		{"unknown mode is rejected", driveConfig{mode: "bogus", n: 10, inflight: 1}, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			cfg := tt.cfg
			err := cfg.validate()
			if tt.wantErr != (err != nil) {
				t.Fatalf("validate() err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && cfg.inflight != tt.wantInflight {
				t.Errorf("normalised inflight = %d, want %d", cfg.inflight, tt.wantInflight)
			}
		})
	}
}

func TestDriveClosed(t *testing.T) {
	tests := []struct {
		description string
		cfg         driveConfig
		failEvery   int64
		wantCount   int64
		wantErrs    int
	}{
		{"latency sends exactly n", driveConfig{mode: modeLatency, n: 200, inflight: 1, timeout: time.Second}, 0, 200, 0},
		{"windowed sends exactly n across workers", driveConfig{mode: modeWindowed, n: 500, inflight: 16, timeout: time.Second}, 0, 500, 0},
		{"errors are counted, not recorded as latency", driveConfig{mode: modeLatency, n: 100, inflight: 1, timeout: time.Second}, 10, 90, 10},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			er := &echoRequester{failEvery: tt.failEvery}
			rc := newTestCell(t, er, tt.cfg.mode)
			res, err := drive(context.Background(), rc, tt.cfg, 100)
			if err != nil {
				t.Fatalf("drive: %v", err)
			}
			if got := res.hdr.Count(); got != tt.wantCount {
				t.Errorf("recorded latency count = %d, want %d", got, tt.wantCount)
			}
			if got := res.hdr.NumErrors(); got != tt.wantErrs {
				t.Errorf("errors = %d, want %d", got, tt.wantErrs)
			}
			// Every sequence number 0..n-1 must have been issued exactly once.
			if er.calls.Load() != int64(tt.cfg.n) {
				t.Errorf("requester calls = %d, want %d", er.calls.Load(), tt.cfg.n)
			}
		})
	}
}

func TestDriveWindowedIsConcurrent(t *testing.T) {
	// A per-request delay forces overlap: 8 workers over a slow requester must
	// reach a concurrency > 1, or the loop is not actually windowed.
	er := &echoRequester{delay: 500 * time.Microsecond}
	cfg := driveConfig{mode: modeWindowed, n: 400, inflight: 8, timeout: time.Second}
	rc := newTestCell(t, er, cfg.mode)
	if _, err := drive(context.Background(), rc, cfg, 100); err != nil {
		t.Fatalf("drive: %v", err)
	}
	if peak := er.peak.Load(); peak < 2 {
		t.Errorf("observed peak concurrency = %d, want > 1 (windowed should overlap)", peak)
	}
}

func TestDriveOpenLoop(t *testing.T) {
	// A fast requester keeps up with the offered rate: samples land in the
	// CO-corrected series and late_sends stays low.
	er := &echoRequester{}
	cfg := driveConfig{mode: modeOpenLoop, rate: 2000, duration: 150 * time.Millisecond, timeout: time.Second}
	rc := newTestCell(t, er, cfg.mode)
	res, err := drive(context.Background(), rc, cfg, 100)
	if err != nil {
		t.Fatalf("drive: %v", err)
	}
	if res.rate != 2000 {
		t.Errorf("result rate = %v, want 2000", res.rate)
	}
	if res.hdr.Count() < 100 {
		t.Errorf("recorded %d samples, want a substantial fraction of ~300", res.hdr.Count())
	}
	if _, ok := res.hdr.CorrectedP99(); !ok {
		t.Error("open-loop should populate the coordinated-omission-corrected series")
	}
}

func TestDriveOpenLoopLateSends(t *testing.T) {
	// A requester far slower than the offered interval drains the tiny worker
	// pool, so the sender falls behind its schedule → late_sends accrue.
	er := &echoRequester{delay: 5 * time.Millisecond}
	cfg := driveConfig{mode: modeOpenLoop, rate: 5000, duration: 200 * time.Millisecond, inflight: 2, timeout: time.Second}
	rc := newTestCell(t, er, cfg.mode)
	res, err := drive(context.Background(), rc, cfg, 100)
	if err != nil {
		t.Fatalf("drive: %v", err)
	}
	if res.lateSends == 0 {
		t.Error("an overloaded open-loop run should report late_sends > 0")
	}
}

func TestDriveFault(t *testing.T) {
	// fault is an open-loop pass that never aborts: every request that gets no
	// valid reply during the window is folded in as an error and surfaced as a
	// missing sequence, so the reported missing count equals the pass's error
	// count. A clean window reports missing 0.
	tests := []struct {
		description string
		failEvery   int64
		wantMissing bool
	}{
		{"a clean fault window records no missing sequences", 0, false},
		{"failures during the window are counted as missing, not fatal", 5, true},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			er := &echoRequester{failEvery: tt.failEvery}
			cfg := driveConfig{mode: modeFault, rate: 2000, duration: 150 * time.Millisecond, timeout: time.Second}
			rc := newTestCell(t, er, cfg.mode)
			res, err := drive(context.Background(), rc, cfg, 100)
			if err != nil {
				t.Fatalf("drive: %v", err)
			}
			if res.missing != int64(res.hdr.NumErrors()) {
				t.Errorf("missing = %d, want it to equal the error count %d", res.missing, res.hdr.NumErrors())
			}
			if (res.missing > 0) != tt.wantMissing {
				t.Errorf("missing = %d, wantMissing = %v", res.missing, tt.wantMissing)
			}
			if res.rate != 2000 {
				t.Errorf("reported rate = %v, want 2000 (fault holds a fixed rate)", res.rate)
			}
			// The summary must carry the count into the run.json integrity column.
			sum := summaryFromHDR(rc.cell, res)
			if sum.Missing != res.missing {
				t.Errorf("summary missing = %d, want %d", sum.Missing, res.missing)
			}
		})
	}
}

func TestStepSustainable(t *testing.T) {
	const floor = time.Millisecond // ceiling is 10× → 10ms
	tests := []struct {
		description string
		p99         time.Duration
		floor       time.Duration
		lateFrac    float64
		wantOK      bool
		wantReason  string
	}{
		{"healthy step holds", 5 * time.Millisecond, floor, 0, true, ""},
		{"p99 exactly at the 10× ceiling still holds", 10 * time.Millisecond, floor, 0, true, ""},
		{"p99 just past the ceiling fails on p99", 10*time.Millisecond + 1, floor, 0, false, "p99"},
		{"late fraction exactly at 1% still holds", time.Millisecond, floor, saturationLateFrac, true, ""},
		{"late fraction just past 1% fails on late", time.Millisecond, floor, saturationLateFrac + 1e-9, false, "late"},
		{"unmeasured floor skips the p99 bound", time.Second, 0, 0, true, ""},
		{"unmeasured floor still enforces the late bound", time.Second, 0, 0.5, false, "late"},
		{"p99 wins when a saturated step trips both bounds", 50 * time.Millisecond, floor, 0.5, false, "p99"},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			ok, reason := stepSustainable(tt.p99, tt.floor, tt.lateFrac)
			if ok != tt.wantOK || reason != tt.wantReason {
				t.Errorf("stepSustainable(%s, %s, %v) = (%v, %q), want (%v, %q)",
					tt.p99, tt.floor, tt.lateFrac, ok, reason, tt.wantOK, tt.wantReason)
			}
		})
	}
}

// fakePass returns an openPassFn that fills each step's series deterministically
// from the offered rate: samples recorded at the returned p99, and the returned
// late count marked late. It lets the ramp logic be tested without wall-clock
// timing (see the comment on openPassFn).
func fakePass(fn func(rate float64) (p99 time.Duration, late, samples int64)) openPassFn {
	return func(_ context.Context, rate float64, _ time.Duration, s *series) time.Duration {
		p99, late, samples := fn(rate)
		for i := int64(0); i < samples; i++ {
			s.record(p99, "", 0)
		}
		s.late = late
		return 0
	}
}

func TestRamp(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		description string
		cfg         driveConfig
		pass        openPassFn
		wantKnee    float64
		wantStop    float64
		wantReason  string
		wantFloor   time.Duration
	}{
		{
			// floor measured at the first step (1ms → 10ms ceiling); steps stay
			// healthy through 4000, then 8000 saturates on p99.
			description: "ramp finds a knee, then a higher step trips p99",
			cfg:         driveConfig{rate: 1000, floor: 0},
			pass: fakePass(func(rate float64) (time.Duration, int64, int64) {
				if rate <= 4000 {
					return 1 * ms, 0, 1000
				}
				return 50 * ms, 0, 1000
			}),
			wantKnee: 4000, wantStop: 8000, wantReason: "p99", wantFloor: 1 * ms,
		},
		{
			// The base step's late fraction already exceeds 1%: no rate sustains,
			// so the knee is 0 and the failing first step is reported.
			description: "base rate already saturates on late → knee 0",
			cfg:         driveConfig{rate: 1000, floor: 0},
			pass: fakePass(func(rate float64) (time.Duration, int64, int64) {
				return 1 * ms, 500, 1000 // 50% late
			}),
			wantKnee: 0, wantStop: 1000, wantReason: "late", wantFloor: 1 * ms,
		},
		{
			// An explicit 1ms floor (ceiling 10ms) is not overwritten by the first
			// step's 50ms p99, which trips the ramp immediately.
			description: "explicit floor bounds the ceiling and is preserved",
			cfg:         driveConfig{rate: 1000, floor: 1 * ms},
			pass: fakePass(func(rate float64) (time.Duration, int64, int64) {
				return 50 * ms, 0, 1000
			}),
			wantKnee: 0, wantStop: 1000, wantReason: "p99", wantFloor: 1 * ms,
		},
		{
			// The measured floor comes from the first step (2ms → 20ms ceiling)
			// and is not overwritten by later steps: 2000's 30ms p99 trips it.
			description: "measured floor is fixed by the first step",
			cfg:         driveConfig{rate: 1000, floor: 0},
			pass: fakePass(func(rate float64) (time.Duration, int64, int64) {
				if rate == 1000 {
					return 2 * ms, 0, 1000
				}
				return 30 * ms, 0, 1000
			}),
			wantKnee: 1000, wantStop: 2000, wantReason: "p99", wantFloor: 2 * ms,
		},
		{
			// Every step up to the ramp cap is healthy: the ramp reports "cap"
			// with the top rate as the knee.
			description: "ramp cap reached without saturating",
			cfg:         driveConfig{rate: 1000, floor: 1 * ms},
			pass: fakePass(func(rate float64) (time.Duration, int64, int64) {
				return 1 * ms, 0, 500
			}),
			wantKnee: 1000 * math.Pow(2, maxRampSteps-1), wantStop: 1000 * math.Pow(2, maxRampSteps-1), wantReason: "cap", wantFloor: 1 * ms,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			d := &driver{cfg: tt.cfg}
			_, kneeS, sr := d.ramp(context.Background(), tt.pass)
			if sr.knee != tt.wantKnee {
				t.Errorf("knee = %.0f, want %.0f", sr.knee, tt.wantKnee)
			}
			if sr.stopRate != tt.wantStop {
				t.Errorf("stopRate = %.0f, want %.0f", sr.stopRate, tt.wantStop)
			}
			if sr.reason != tt.wantReason {
				t.Errorf("reason = %q, want %q", sr.reason, tt.wantReason)
			}
			// An explicit floor is preserved verbatim; a measured floor comes back
			// as an HDR bucket value, so it is only exact to the histogram's 3
			// significant figures.
			if tt.cfg.floor > 0 {
				if sr.floor != tt.wantFloor {
					t.Errorf("explicit floor = %s, want it preserved as %s", sr.floor, tt.wantFloor)
				}
			} else if delta := sr.floor - tt.wantFloor; delta < -tt.wantFloor/100 || delta > tt.wantFloor/100 {
				t.Errorf("measured floor = %s, want ≈ %s (±1%%)", sr.floor, tt.wantFloor)
			}
			if kneeS == nil || kneeS.hdr.Count() == 0 {
				t.Error("the reported step must carry a non-empty histogram")
			}
		})
	}
}

func TestRunColdstart(t *testing.T) {
	tests := []struct {
		description   string
		conns         int
		dialFailEvery int64
		wantSamples   int64
		wantConnErr   int
		wantDials     int64
		wantTeardowns int64
	}{
		{"dials conns times and records each first request", 20, 0, 20, 0, 20, 20},
		{"conns 0 defaults to 20 fresh connections", 0, 0, 20, 0, 20, 20},
		{"a failed dial is a connect error, not a sample and not torn down", 10, 3, 7, 3, 10, 7},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			er := &echoRequester{}
			var dials, teardowns atomic.Int64
			rc := newTestCell(t, er, modeColdstart)
			rc.dial = func() (transport.Requester, func(), error) {
				n := dials.Add(1)
				if tt.dialFailEvery > 0 && n%tt.dialFailEvery == 0 {
					return nil, nil, errors.New("dial failed")
				}
				return er, func() { teardowns.Add(1) }, nil
			}
			cfg := driveConfig{mode: modeColdstart, conns: tt.conns, timeout: time.Second}
			res, err := drive(context.Background(), rc, cfg, 100)
			if err != nil {
				t.Fatalf("drive: %v", err)
			}
			if got := res.hdr.Count(); got != tt.wantSamples {
				t.Errorf("recorded samples = %d, want %d", got, tt.wantSamples)
			}
			if got := res.hdr.NumErrors(); got != tt.wantConnErr {
				t.Errorf("connect errors = %d, want %d", got, tt.wantConnErr)
			}
			if got := dials.Load(); got != tt.wantDials {
				t.Errorf("dials = %d, want %d", got, tt.wantDials)
			}
			if got := teardowns.Load(); got != tt.wantTeardowns {
				t.Errorf("teardowns = %d, want %d (a failed dial must not be torn down)", got, tt.wantTeardowns)
			}
			if res.inflight != 1 {
				t.Errorf("reported inflight = %d, want 1", res.inflight)
			}
		})
	}
}

func TestRunColdstartNoDialer(t *testing.T) {
	// A transport that cannot re-dial (dial == nil) must reject coldstart rather
	// than silently measure the shared warm connection.
	er := &echoRequester{}
	rc := newTestCell(t, er, modeColdstart) // newTestCell leaves rc.dial nil
	cfg := driveConfig{mode: modeColdstart, conns: 5, timeout: time.Second}
	if _, err := drive(context.Background(), rc, cfg, 100); err == nil {
		t.Fatal("coldstart without a dialer should error")
	}
}

// TestDriveSaturationSmoke runs the real open-loop ramp once at a trivial load,
// asserting only the timing-independent invariants (a stop reason is recorded
// and the reported rate is the knee). The knee value itself depends on the
// host's scheduler precision, so it is exercised deterministically in TestRamp.
func TestDriveSaturationSmoke(t *testing.T) {
	er := &echoRequester{}
	cfg := driveConfig{mode: modeSaturation, rate: 1000, step: 40 * time.Millisecond, inflight: 8, timeout: time.Second}
	rc := newTestCell(t, er, cfg.mode)
	res, err := drive(context.Background(), rc, cfg, 100)
	if err != nil {
		t.Fatalf("drive: %v", err)
	}
	if res.sat == nil {
		t.Fatal("saturation result missing")
	}
	if res.sat.reason == "" {
		t.Error("a finished ramp must record a stop reason")
	}
	if res.rate != res.sat.knee {
		t.Errorf("reported rate = %.0f, want the knee %.0f", res.rate, res.sat.knee)
	}
}

// TestDriverGCProfile: the window GC summary reads the right pauses out of the
// MemStats ring, computes p99 over them, and derives the cycle rate — and
// reports zeros when no GC ran during the pass.
func TestDriverGCProfile(t *testing.T) {
	var ring [256]uint64
	for i := 0; i < 256; i++ {
		ring[i] = uint64((i + 1) * 1000) // index i → (i+1) µs, so the ring is easy to read
	}
	tests := []struct {
		description   string
		gc0, gc1      uint32
		elapsed       time.Duration
		wantP99       time.Duration
		wantCyclesPer float64
	}{
		{"no GC ran (gc0==gc1)", 7, 7, time.Second, 0, 0},
		{"single GC, p99 is that one pause", 0, 1, time.Second, 1 * time.Microsecond, 1},
		{"five GCs, p99 is the largest", 0, 5, time.Second, 5 * time.Microsecond, 5},
		{"five GCs over 2s halves the rate", 0, 5, 2 * time.Second, 5 * time.Microsecond, 2.5},
		{"zero elapsed leaves rate 0", 0, 3, 0, 3 * time.Microsecond, 0},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			p99, cps := driverGCProfile(tc.gc0, tc.gc1, ring, tc.elapsed)
			if p99 != tc.wantP99 {
				t.Errorf("p99 = %v, want %v", p99, tc.wantP99)
			}
			if cps != tc.wantCyclesPer {
				t.Errorf("cycles/s = %v, want %v", cps, tc.wantCyclesPer)
			}
		})
	}
}
