package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/runrecord"
)

func TestCellID(t *testing.T) {
	tests := []struct {
		description string
		cell        harness.Cell
		want        string
	}{
		{
			"full identity joins in run.json order",
			harness.Cell{Transport: "grpc_unary", Codec: "proto", Fixture: "medium", Pool: "all", GC: "default", Mode: "latency"},
			"grpc_unary/proto/medium/all/default/latency",
		},
		{
			"bus cell",
			harness.Cell{Transport: "nats_request_reply", Codec: "vtproto", Fixture: "large", Pool: "none", GC: "limit", Mode: "openloop"},
			"nats_request_reply/vtproto/large/none/limit/openloop",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := cellID(tt.cell); got != tt.want {
				t.Errorf("cellID = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSummaryFromHDR(t *testing.T) {
	cell := harness.Cell{Transport: "grpc_unary", Codec: "proto", Fixture: "medium", Pool: "all", GC: "default", Mode: "latency", Tier: "rpc"}
	h := harness.NewHDR()
	for i := 0; i < 1000; i++ {
		h.Record(time.Millisecond)
	}
	h.AddErrorKind("transport")
	s := summaryFromHDR(cell, h, time.Second, 652, 1, 0)

	tests := []struct {
		description string
		got         float64
		wantLo      float64
		wantHi      float64
	}{
		{"msgs excludes errors", float64(s.Msgs), 1000, 1000},
		{"throughput is count/elapsed", s.ThroughputMsgS, 1000, 1000},
		{"p50 ≈ 1000µs", s.RTTP50US, 990, 1010},
		{"p99 ≈ 1000µs", s.RTTP99US, 990, 1015},
		{"MiB/s uses the request wire size", s.ThroughputMiBS, 0.6, 0.65},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if tt.got < tt.wantLo || tt.got > tt.wantHi {
				t.Errorf("%s = %v, want within [%v, %v]", tt.description, tt.got, tt.wantLo, tt.wantHi)
			}
		})
	}
	if s.Errors["transport"] != 1 {
		t.Errorf("errors = %v, want transport:1", s.Errors)
	}
	if s.Tier != "rpc" || s.WireBytesReq != 652 {
		t.Errorf("identity not carried: tier=%q wireReq=%d", s.Tier, s.WireBytesReq)
	}
}

func TestEmitCell(t *testing.T) {
	dir := t.TempDir()
	cell := harness.Cell{Transport: "grpc_unary", Codec: "proto", Fixture: "medium", Pool: "all", GC: "default", Mode: "latency", Tier: "rpc"}
	h := harness.NewHDR()
	for i := 0; i < 500; i++ {
		h.RecordCorrected(time.Millisecond, time.Millisecond)
	}

	tests := []struct {
		description string
		opts        emitOptions
		wantFiles   []string
		noFiles     bool
	}{
		{"no -out/-hgrm writes nothing", emitOptions{}, nil, true},
		{"only -hgrm writes the histogram", emitOptions{hgrm: filepath.Join(dir, "a.hgrm")}, []string{"a.hgrm"}, false},
		{"both write record + histogram", emitOptions{out: filepath.Join(dir, "b.json"), hgrm: filepath.Join(dir, "b.hgrm"), repeat: 2}, []string{"b.json", "b.hgrm"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if err := emitCell(tt.opts, cell, h, time.Second, 652, 1, 0); err != nil {
				t.Fatalf("emitCell: %v", err)
			}
			for _, f := range tt.wantFiles {
				if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
					t.Errorf("expected %s: %v", f, err)
				}
			}
		})
	}

	// The record from the "both" case must round-trip as a runrecord.Cell with
	// its id, repeat, and hgrm path.
	b, err := os.ReadFile(filepath.Join(dir, "b.json"))
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	var rc runrecord.Cell
	if err := json.Unmarshal(b, &rc); err != nil {
		t.Fatalf("unmarshal record: %v", err)
	}
	if rc.ID != "grpc_unary/proto/medium/all/default/latency" {
		t.Errorf("id = %q", rc.ID)
	}
	if rc.Repeat != 2 {
		t.Errorf("repeat = %d, want 2", rc.Repeat)
	}
	if rc.HGRM != filepath.Join(dir, "b.hgrm") {
		t.Errorf("hgrm = %q", rc.HGRM)
	}
	if rc.Summary.RTTCoCorrectedP99US <= 0 {
		t.Errorf("expected a CO-corrected p99 in the emitted record, got %v", rc.Summary.RTTCoCorrectedP99US)
	}
}
