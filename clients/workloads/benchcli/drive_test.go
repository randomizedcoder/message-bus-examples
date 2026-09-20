package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
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
