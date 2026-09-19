package metrics

import (
	"math"
	"testing"
	"time"
)

func TestParseSeq(t *testing.T) {
	tests := []struct {
		description string
		body        string
		expectedN   int64
		expectedOK  bool
	}{
		// positive
		{"trailing ordinal after a subject", "orders 42", 42, true},
		{"bare number is its own ordinal", "42", 42, true},
		{"multi-word body, integer tail", "hello from natscli 7", 7, true},
		// boundary
		{"sequence zero", "m 0", 0, true},
		{"first sequence", "m 1", 1, true},
		{"single digit", "m 9", 9, true},
		{"max int64 tail", "m " + "9223372036854775807", math.MaxInt64, true},
		// negative
		{"no trailing integer", "hello", 0, false},
		{"non-numeric tail", "msg x", 0, false},
		{"empty string", "", 0, false},
		{"only whitespace", "   ", 0, false},
		{"float tail is not an integer", "m 1.5", 0, false},
		{"overflowing integer tail", "m 99999999999999999999999", 0, false},
		// corner
		{"trailing whitespace is ignored", "orders 42   ", 42, true},
		{"multiple spaces between tokens", "orders    42", 42, true},
		{"negative tail parses (defensive)", "m -3", -3, true},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			n, ok := ParseSeq(tc.body)
			if ok != tc.expectedOK {
				t.Fatalf("ParseSeq(%q) ok = %v, expected %v", tc.body, ok, tc.expectedOK)
			}
			if ok && n != tc.expectedN {
				t.Fatalf("ParseSeq(%q) = %d, expected %d", tc.body, n, tc.expectedN)
			}
		})
	}
}

func TestGapTracker(t *testing.T) {
	tests := []struct {
		description  string
		seqs         []int64 // sequences observed in order
		expectedGaps []int64 // gap returned for each observation
		expectedSum  int64   // total gaps (lost messages) over the run
	}{
		// positive / in-order
		{
			description:  "first observation never reports a gap",
			seqs:         []int64{5},
			expectedGaps: []int64{0},
			expectedSum:  0,
		},
		{
			description:  "consecutive sequences have no gaps",
			seqs:         []int64{1, 2, 3, 4},
			expectedGaps: []int64{0, 0, 0, 0},
			expectedSum:  0,
		},
		// forward jumps
		{
			description:  "a forward jump counts the skipped messages",
			seqs:         []int64{1, 4},
			expectedGaps: []int64{0, 2},
			expectedSum:  2,
		},
		{
			description:  "multiple gaps accumulate",
			seqs:         []int64{1, 3, 7},
			expectedGaps: []int64{0, 1, 3},
			expectedSum:  4,
		},
		// boundary
		{
			description:  "starting at zero then one is in order",
			seqs:         []int64{0, 1},
			expectedGaps: []int64{0, 0},
			expectedSum:  0,
		},
		// corner: duplicate
		{
			description:  "a duplicate reports no gap and keeps the baseline",
			seqs:         []int64{5, 5, 6},
			expectedGaps: []int64{0, 0, 0},
			expectedSum:  0,
		},
		// corner: reorder (lower after higher)
		{
			description:  "out-of-order 43 after 45 reports no negative gap",
			seqs:         []int64{45, 43},
			expectedGaps: []int64{0, 0},
			expectedSum:  0,
		},
		// corner: respawn reset (sequence restarts at 1)
		{
			description:  "a respawn resetting to 1 is a new run, not a huge gap",
			seqs:         []int64{100, 1, 2, 3},
			expectedGaps: []int64{0, 0, 0, 0},
			expectedSum:  0,
		},
		{
			description:  "gap accounting resumes after a reset",
			seqs:         []int64{100, 1, 4},
			expectedGaps: []int64{0, 0, 2},
			expectedSum:  2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			var g GapTracker
			var sum int64
			for i, seq := range tc.seqs {
				got := g.Observe(seq)
				if got != tc.expectedGaps[i] {
					t.Fatalf("Observe step %d (seq=%d): gap = %d, expected %d",
						i, seq, got, tc.expectedGaps[i])
				}
				sum += got
			}
			if sum != tc.expectedSum {
				t.Fatalf("total gaps = %d, expected %d", sum, tc.expectedSum)
			}
		})
	}
}

// TestNopRecorder is a smoke check that the disabled Recorder never panics —
// the clients call it on every message when -metrics-addr is unset.
func TestNopRecorder(t *testing.T) {
	var r Recorder = Nop{}
	r.IncPublished()
	r.IncPublishError()
	r.IncReceived()
	r.IncReconnect()
	r.AddGaps(3)
	r.ObserveLatency(2 * time.Millisecond)
}

// TestSetupDisabled verifies an empty Addr yields the no-op recorder and no
// server, so metrics stay strictly opt-in.
func TestSetupDisabled(t *testing.T) {
	r, err := Setup(Config{Addr: ""})
	if err != nil {
		t.Fatalf("Setup(disabled): unexpected error: %v", err)
	}
	if _, ok := r.(Nop); !ok {
		t.Fatalf("Setup(disabled) = %T, expected metrics.Nop", r)
	}
}

// --- micro-benchmarks (execute under `go test -bench`; compile-checked by
// `go test ./...`). They exercise the per-message subscriber hot paths. ---

// liveMetrics builds a real OTel-backed *Metrics on an ephemeral port, so the
// live-recorder benchmarks measure the actual counter/histogram cost rather
// than the Nop{} no-op. The /metrics server binds 127.0.0.1:0 and is left to
// the process to reclaim on exit.
func liveMetrics(tb testing.TB) *Metrics {
	tb.Helper()
	r, err := Setup(Config{Addr: "127.0.0.1:0", Bus: "bench", Mode: "core", Role: "sub"})
	if err != nil {
		tb.Fatalf("Setup: %v", err)
	}
	m, ok := r.(*Metrics)
	if !ok {
		tb.Fatalf("Setup returned %T, expected *Metrics", r)
	}
	return m
}

// recorders pairs the live and disabled recorders so each benchmark reports
// both the enabled cost and the no-op baseline.
func recorders(b *testing.B) []struct {
	name string
	rec  Recorder
} {
	return []struct {
		name string
		rec  Recorder
	}{
		{"live", liveMetrics(b)},
		{"nop", Nop{}},
	}
}

func BenchmarkParseSeq(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := ParseSeq("orders 123456"); !ok {
			b.Fatal("expected a sequence tail")
		}
	}
}

// BenchmarkGapObserve streams strictly-increasing sequences through the
// mutex-guarded in-order path (the common case).
func BenchmarkGapObserve(b *testing.B) {
	var g GapTracker
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		g.Observe(int64(i))
	}
}

// BenchmarkRecordReceived measures the full per-message subscriber wrapper
// (IncReceived + ParseSeq + GapTracker.Observe) for both recorders.
func BenchmarkRecordReceived(b *testing.B) {
	for _, r := range recorders(b) {
		b.Run(r.name, func(b *testing.B) {
			var g GapTracker
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				RecordReceived(r.rec, &g, "orders 123")
			}
		})
	}
}

// BenchmarkIncPublished measures the publisher's per-message counter Add
// against the stored attribute set, for both recorders.
func BenchmarkIncPublished(b *testing.B) {
	for _, r := range recorders(b) {
		b.Run(r.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r.rec.IncPublished()
			}
		})
	}
}
