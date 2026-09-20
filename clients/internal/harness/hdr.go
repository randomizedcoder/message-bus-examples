package harness

import (
	"fmt"
	"io"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// maxTrackNs is the highest latency the RTT histograms resolve (60 s). Anything
// slower is a timeout, counted as an error rather than a latency sample; a value
// above this is clamped so a stray outlier never widens every bucket.
const maxTrackNs = 60 * int64(time.Second)

// HDR is an HDR-histogram-backed latency accumulator (design §8). It replaces
// the exact-sort Latencies for the full matrix: constant memory regardless of
// sample count, and it keeps a second, coordinated-omission-corrected series
// (hdrhistogram RecordCorrectedValue) so open-loop cells can report both the raw
// and the CO-corrected distribution and the reader can see the difference rather
// than trust the correction (§8.2). It records the same harness.Summary the
// exact accumulator did, so the print paths are unchanged.
type HDR struct {
	raw       *hdrhistogram.Histogram
	corrected *hdrhistogram.Histogram // populated only when RecordCorrected is used
	errors    int
	errKinds  map[string]int64
}

// NewHDR returns an HDR tracking 1 ns … 60 s at 3 significant figures.
func NewHDR() *HDR {
	return &HDR{
		raw:       hdrhistogram.New(1, maxTrackNs, 3),
		corrected: hdrhistogram.New(1, maxTrackNs, 3),
	}
}

// clamp keeps a duration inside the trackable range (≥1 ns so a sub-ns sample is
// still counted; ≤60 s so an outlier does not re-scale the histogram).
func clamp(d time.Duration) int64 {
	v := int64(d)
	if v < 1 {
		return 1
	}
	if v > maxTrackNs {
		return maxTrackNs
	}
	return v
}

// Record adds one latency sample to the raw histogram.
func (r *HDR) Record(d time.Duration) { _ = r.raw.RecordValue(clamp(d)) }

// RecordCorrected adds one sample to both the raw histogram and the
// coordinated-omission-corrected histogram, back-filling the expected values a
// stalled sender would have missed at the given interval (open-loop, §8.2).
func (r *HDR) RecordCorrected(d, expected time.Duration) {
	v := clamp(d)
	_ = r.raw.RecordValue(v)
	_ = r.corrected.RecordCorrectedValue(v, clamp(expected))
}

// AddError records an error under the generic "error" kind.
func (r *HDR) AddError() { r.AddErrorKind("error") }

// AddErrorKind records an error under a specific kind (transport|corrupt|
// encode|timeout|late), so the emitted cell carries an errors{kind} breakdown.
func (r *HDR) AddErrorKind(kind string) {
	r.errors++
	if r.errKinds == nil {
		r.errKinds = map[string]int64{}
	}
	r.errKinds[kind]++
}

// NumErrors returns the total error count.
func (r *HDR) NumErrors() int { return r.errors }

// ErrorKinds returns the per-kind error counts (nil if there were none).
func (r *HDR) ErrorKinds() map[string]int64 {
	if len(r.errKinds) == 0 {
		return nil
	}
	out := make(map[string]int64, len(r.errKinds))
	for k, v := range r.errKinds {
		out[k] = v
	}
	return out
}

// Count returns the number of recorded latency samples (excludes errors).
func (r *HDR) Count() int64 { return r.raw.TotalCount() }

// Summarize reduces the raw histogram to a harness.Summary over elapsed wall
// time (the same shape the exact accumulator produced).
func (r *HDR) Summarize(elapsed time.Duration) Summary {
	s := Summary{Count: int(r.raw.TotalCount()), Errors: r.errors}
	if r.raw.TotalCount() == 0 {
		return s
	}
	s.Min = time.Duration(r.raw.Min())
	s.Max = time.Duration(r.raw.Max())
	s.Mean = time.Duration(int64(r.raw.Mean()))
	s.P50 = time.Duration(r.raw.ValueAtQuantile(50))
	s.P90 = time.Duration(r.raw.ValueAtQuantile(90))
	s.P99 = time.Duration(r.raw.ValueAtQuantile(99))
	s.P999 = time.Duration(r.raw.ValueAtQuantile(99.9))
	if elapsed > 0 {
		s.ThroughputPerSec = float64(r.raw.TotalCount()) / elapsed.Seconds()
	}
	return s
}

// CorrectedP99 returns the coordinated-omission-corrected p99 and whether any
// corrected samples exist (false for closed-loop cells that never call
// RecordCorrected).
func (r *HDR) CorrectedP99() (time.Duration, bool) {
	if r.corrected.TotalCount() == 0 {
		return 0, false
	}
	return time.Duration(r.corrected.ValueAtQuantile(99)), true
}

// WriteHGRM writes the standard HdrHistogram percentile-distribution format
// (value / percentile / total-count / 1÷(1−percentile)), values scaled to
// microseconds, so the files interoperate with HdrHistogram plotters. The
// CO-corrected series is appended when present (design §8.2, §8.4).
func (r *HDR) WriteHGRM(w io.Writer) error {
	if _, err := fmt.Fprintln(w, "#[raw latencies, microseconds]"); err != nil {
		return err
	}
	if _, err := r.raw.PercentilesPrint(w, 5, 1000.0); err != nil {
		return err
	}
	if r.corrected.TotalCount() > 0 {
		if _, err := fmt.Fprintln(w, "\n#[coordinated-omission-corrected, microseconds]"); err != nil {
			return err
		}
		if _, err := r.corrected.PercentilesPrint(w, 5, 1000.0); err != nil {
			return err
		}
	}
	return nil
}
