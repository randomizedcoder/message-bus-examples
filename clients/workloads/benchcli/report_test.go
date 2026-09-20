package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/runrecord"
)

// sampleRun builds a small but representative run: two repeats of a stable gRPC
// latency cell, two repeats of an unstable one, an open-loop NATS cell, plus a
// clock and corpus header — enough to exercise every renderer branch.
func sampleRun() *runrecord.Run {
	mk := func(id, transport, tier, mode, pool string, p50, p99, tput, allocs float64) runrecord.Cell {
		return runrecord.Cell{ID: id, Summary: runrecord.Summary{
			Tier: tier, Transport: transport, Codec: "proto", Fixture: "medium",
			Pool: pool, GC: "default", Mode: mode,
			Msgs: 10000, ThroughputMsgS: tput,
			RTTP50US: p50, RTTP90US: p99 * 0.8, RTTP99US: p99, RTTP999US: p99 * 1.5, RTTMaxUS: p99 * 3,
			DriverAllocsPerMsg: allocs,
		}}
	}
	stable0 := mk("grpc_unary/proto/medium/all/default/latency", "grpc_unary", "rpc", "latency", "all", 200, 400, 5000, 1)
	stable1 := stable0
	stable1.Repeat = 1
	stable1.Summary.RTTP99US = 410
	// none-pool variant for the scorecard alloc comparison.
	noneP := mk("grpc_unary/proto/medium/none/default/latency", "grpc_unary", "rpc", "latency", "none", 210, 420, 4800, 6)

	unstable0 := mk("nats_rr/proto/medium/all/default/latency", "nats_request_reply", "at_least_once", "latency", "all", 800, 1000, 1200, 3)
	unstable1 := unstable0
	unstable1.Repeat = 1
	unstable1.Summary.RTTP99US = 3000 // huge spread -> unstable

	open := mk("nats_rr/proto/medium/all/default/openloop", "nats_request_reply", "at_least_once", "openloop", "all", 900, 1100, 2000, 3)
	open.Summary.Rate = 2000
	open.Summary.LateSends = 4

	return &runrecord.Run{
		RunID:    "01JTESTRUN",
		Git:      runrecord.Git{Rev: "abc1234", Dirty: true},
		Versions: map[string]string{"go": "1.26.7", "grpc": "v1.83.2"},
		Clock: map[string]runrecord.Clock{
			"us-west-2": {OffsetNs: 1500, UncertaintyNs: 4100, Source: "chrony-phc", Synced: true},
		},
		Corpus: runrecord.Corpus{Seed: 42, Fixtures: map[string]runrecord.FixtureSizes{
			"medium": {ProtoBytes: 652, ProtoJSONBytes: 1710, VTProtoBytes: 652},
		}},
		Matrix: runrecord.Matrix{OrderSeed: 7, Repeats: 2},
		Cells:  []runrecord.Cell{stable0, stable1, noneP, unstable0, unstable1, open},
	}
}

func TestRenderTSV(t *testing.T) {
	tsv := renderTSV(sampleRun())
	lines := strings.Split(strings.TrimRight(tsv, "\n"), "\n")
	tests := []struct {
		description string
		check       func(t *testing.T)
	}{
		{"one header + one row per cell", func(t *testing.T) {
			if len(lines) != 1+6 {
				t.Fatalf("lines = %d, want 7", len(lines))
			}
		}},
		{"header carries identity and metric columns", func(t *testing.T) {
			head := lines[0]
			for _, want := range []string{"id", "repeat", "tier", "rtt_p99_us", "throughput_msg_s", "hgrm", "grafana"} {
				if !strings.Contains(head, want) {
					t.Errorf("header missing %q", want)
				}
			}
		}},
		{"rows are tab-separated with a stable column count", func(t *testing.T) {
			n := len(strings.Split(lines[0], "\t"))
			for i, l := range lines[1:] {
				if got := len(strings.Split(l, "\t")); got != n {
					t.Errorf("row %d has %d columns, want %d", i, got, n)
				}
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.description, tt.check)
	}
}

func TestRenderMarkdown(t *testing.T) {
	md := renderMarkdown(sampleRun())
	tests := []struct {
		description string
		wantSubstr  string
	}{
		{"run id in the title", "01JTESTRUN"},
		{"dirty git tree noted", "(dirty)"},
		{"clock section present", "## Clock"},
		{"clock offset in microseconds", "1.5"}, // 1500ns -> 1.5µs
		{"corpus section present", "## Corpus (seed 42)"},
		{"rpc tier heading", "## Tier: rpc"},
		{"at_least_once tier heading", "## Tier: at_least_once"},
		{"latency mode heading", "### Mode: latency"},
		{"openloop mode heading", "### Mode: openloop"},
		{"unstable cell flagged", "⚠ unstable"},
		{"open-loop table shows late sends knob", "late"},
		{"scorecard present", "## Scorecard"},
		{"scorecard alloc comparison none→all", "6 → 1 allocs/msg"},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if !strings.Contains(md, tt.wantSubstr) {
				t.Errorf("markdown missing %q\n---\n%s", tt.wantSubstr, md)
			}
		})
	}
}

// TestRenderMarkdownTierOrder asserts rpc precedes at_least_once regardless of
// cell order, so the report reads in delivery-semantics order (design §2.3).
func TestRenderMarkdownTierOrder(t *testing.T) {
	md := renderMarkdown(sampleRun())
	rpc := strings.Index(md, "## Tier: rpc")
	alo := strings.Index(md, "## Tier: at_least_once")
	if rpc < 0 || alo < 0 || rpc > alo {
		t.Errorf("tier order wrong: rpc at %d, at_least_once at %d", rpc, alo)
	}
}

func TestRunReportWritesFiles(t *testing.T) {
	dir := t.TempDir()
	runPath := filepath.Join(dir, "run.json")
	if err := runrecord.Save(runPath, sampleRun()); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := runReport([]string{"-run", runPath}); err != nil {
		t.Fatalf("runReport: %v", err)
	}
	for _, name := range []string{"results.tsv", "results.md"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Errorf("expected %s: %v", name, err)
			continue
		}
		if len(b) == 0 {
			t.Errorf("%s is empty", name)
		}
	}
}

func TestRunReportRequiresRun(t *testing.T) {
	if err := runReport(nil); err == nil {
		t.Error("expected an error when -run is omitted")
	}
}

func TestRunRecordRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "run.json")
	in := sampleRun()
	if err := runrecord.Save(p, in); err != nil {
		t.Fatalf("save: %v", err)
	}
	out, err := runrecord.Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if out.RunID != in.RunID || len(out.Cells) != len(in.Cells) {
		t.Errorf("round-trip mismatch: run_id %q cells %d", out.RunID, len(out.Cells))
	}
	if out.Cells[0].Summary.RTTP99US != in.Cells[0].Summary.RTTP99US {
		t.Errorf("cell summary not preserved")
	}
}
