package harness

import (
	"strings"
	"testing"
	"time"
)

func TestHDRSummarize(t *testing.T) {
	tests := []struct {
		description string
		samples     []time.Duration
		elapsed     time.Duration
		wantCount   int
		wantP50Lo   time.Duration // p50 is within [lo, hi] (HDR quantises)
		wantP50Hi   time.Duration
		wantTput    float64
	}{
		{
			description: "empty histogram yields a zero summary",
			samples:     nil, elapsed: time.Second,
			wantCount: 0, wantP50Lo: 0, wantP50Hi: 0, wantTput: 0,
		},
		{
			description: "uniform 1ms samples: p50 ≈ 1ms",
			samples:     dur(1000, time.Millisecond),
			elapsed:     time.Second,
			wantCount:   1000,
			wantP50Lo:   990 * time.Microsecond, wantP50Hi: 1010 * time.Microsecond,
			wantTput: 1000,
		},
		{
			description: "throughput is count over elapsed",
			samples:     dur(200, time.Millisecond),
			elapsed:     2 * time.Second,
			wantCount:   200,
			wantP50Lo:   990 * time.Microsecond, wantP50Hi: 1010 * time.Microsecond,
			wantTput: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			h := NewHDR()
			for _, d := range tt.samples {
				h.Record(d)
			}
			s := h.Summarize(tt.elapsed)
			if s.Count != tt.wantCount {
				t.Errorf("count = %d, want %d", s.Count, tt.wantCount)
			}
			if tt.wantCount > 0 && (s.P50 < tt.wantP50Lo || s.P50 > tt.wantP50Hi) {
				t.Errorf("p50 = %v, want within [%v, %v]", s.P50, tt.wantP50Lo, tt.wantP50Hi)
			}
			if s.ThroughputPerSec != tt.wantTput {
				t.Errorf("throughput = %v, want %v", s.ThroughputPerSec, tt.wantTput)
			}
		})
	}
}

func TestHDRClamp(t *testing.T) {
	tests := []struct {
		description string
		sample      time.Duration
		wantMaxLE   time.Duration
	}{
		{"a sub-nanosecond sample is still counted", 0, time.Microsecond},
		{"a 2-minute outlier is clamped to ~60s, not beyond", 2 * time.Minute, 61 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			h := NewHDR()
			h.Record(tt.sample)
			s := h.Summarize(time.Second)
			if s.Count != 1 {
				t.Fatalf("count = %d, want 1", s.Count)
			}
			if s.Max > tt.wantMaxLE {
				t.Errorf("max = %v, want ≤ %v", s.Max, tt.wantMaxLE)
			}
		})
	}
}

func TestHDRErrorKinds(t *testing.T) {
	h := NewHDR()
	h.AddErrorKind("transport")
	h.AddErrorKind("transport")
	h.AddErrorKind("corrupt")
	h.AddError() // generic "error"
	if h.NumErrors() != 4 {
		t.Errorf("NumErrors = %d, want 4", h.NumErrors())
	}
	k := h.ErrorKinds()
	if k["transport"] != 2 || k["corrupt"] != 1 || k["error"] != 1 {
		t.Errorf("kinds = %v, want transport:2 corrupt:1 error:1", k)
	}
	if NewHDR().ErrorKinds() != nil {
		t.Error("no errors should return a nil kinds map")
	}
}

// TestHDRCorrectedP99 asserts the CO-corrected series only exists once
// RecordCorrected is used, and that a stalled sample inflates it above the raw
// p99 (the point of the correction).
func TestHDRCorrectedP99(t *testing.T) {
	raw := NewHDR()
	for i := 0; i < 1000; i++ {
		raw.Record(time.Millisecond)
	}
	if _, ok := raw.CorrectedP99(); ok {
		t.Error("closed-loop histogram should have no corrected series")
	}

	co := NewHDR()
	expected := time.Millisecond
	for i := 0; i < 999; i++ {
		co.RecordCorrected(time.Millisecond, expected)
	}
	co.RecordCorrected(500*time.Millisecond, expected) // one long stall
	p99, ok := co.CorrectedP99()
	if !ok {
		t.Fatal("corrected series should exist after RecordCorrected")
	}
	if p99 <= time.Millisecond {
		t.Errorf("corrected p99 = %v, want inflated well above 1ms by the stall", p99)
	}
}

func TestHDRWriteHGRM(t *testing.T) {
	h := NewHDR()
	for i := 0; i < 100; i++ {
		h.RecordCorrected(time.Millisecond, time.Millisecond)
	}
	var b strings.Builder
	if err := h.WriteHGRM(&b); err != nil {
		t.Fatalf("WriteHGRM: %v", err)
	}
	out := b.String()
	for _, want := range []string{"raw latencies", "coordinated-omission-corrected", "Percentile"} {
		if !strings.Contains(out, want) {
			t.Errorf("hgrm output missing %q\n%s", want, out)
		}
	}
}

// dur returns a slice of n identical durations of value v.
func dur(n int, v time.Duration) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = v
	}
	return out
}
