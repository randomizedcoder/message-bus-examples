package rpcmetrics

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
)

// gather collects the current value of a single-labelled counter family, summed
// over every series, plus the per-"result"-label breakdown, from the registry.
func counterByResult(t *testing.T, mf *dto.MetricFamily) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, m := range mf.GetMetric() {
		result := ""
		for _, l := range m.GetLabel() {
			if l.GetName() == "result" {
				result = l.GetValue()
			}
		}
		out[result] += m.GetCounter().GetValue()
	}
	return out
}

// TestObserveCounts drives each result kind through a Recorder and asserts the
// rpc_* counters land on the right series. A run.json/HDR path proves exact
// percentiles elsewhere; here we prove the Prometheus wiring and label routing.
func TestObserveCounts(t *testing.T) {
	tests := []struct {
		description  string
		result       string
		hasResp      bool
		wantResponse bool // a responses_total{result} series is emitted
		wantTimeout  bool // timeouts_total is bumped
		wantError    bool // errors_total is bumped
	}{
		{description: "ok response counts a response, no error/timeout", result: ResultOK, hasResp: true, wantResponse: true},
		{description: "non-ok response counts a response, no error/timeout", result: ResultNonOK, hasResp: true, wantResponse: true},
		{description: "status timeout counts a response and a timeout", result: ResultTimeout, hasResp: true, wantResponse: true, wantTimeout: true},
		{description: "transport timeout counts a timeout, no response", result: ResultTimeout, hasResp: false, wantTimeout: true},
		{description: "transport error counts an error, no response", result: ResultError, hasResp: false, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			mp, reg, err := metrics.NewProvider("", ByteBucketsView)
			if err != nil {
				t.Fatalf("NewProvider: %v", err)
			}
			inst, err := New(mp.Meter("test"))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			rec := inst.For(Labels{Transport: "grpc", Codec: "proto", Service: "echo", Method: "Echo"})
			rec.Observe(context.Background(), tt.result, 2*time.Millisecond, 128, tt.hasResp, 256)

			fams, err := reg.Gather()
			if err != nil {
				t.Fatalf("Gather: %v", err)
			}
			byName := map[string]*dto.MetricFamily{}
			for _, mf := range fams {
				byName[mf.GetName()] = mf
			}

			// Every call counts exactly one request.
			if got := counterByResult(t, byName["rpc_requests_total"])[""]; got != 1 {
				t.Errorf("rpc_requests_total = %v, want 1", got)
			}
			// A response series exists (and equals 1 on the result label) only when
			// a Response actually came back.
			resp := counterByResult(t, byName["rpc_responses_total"])
			if tt.wantResponse {
				if got := resp[tt.result]; got != 1 {
					t.Errorf("rpc_responses_total{result=%q} = %v, want 1", tt.result, got)
				}
			} else if len(resp) != 0 {
				t.Errorf("rpc_responses_total = %v, want no series (no response arrived)", resp)
			}
			if got := counterByResult(t, byName["rpc_timeouts_total"])[""]; (got > 0) != tt.wantTimeout {
				t.Errorf("rpc_timeouts_total = %v, wantTimeout = %v", got, tt.wantTimeout)
			}
			if got := counterByResult(t, byName["rpc_errors_total"])[""]; (got > 0) != tt.wantError {
				t.Errorf("rpc_errors_total = %v, wantError = %v", got, tt.wantError)
			}
		})
	}
}

// TestNilRecorderIsNoop proves a disabled process (nil Instruments → nil
// Recorder) records nothing and does not panic — the contract that lets the
// driver hold one Recorder whether or not -metrics-addr was set.
func TestNilRecorderIsNoop(t *testing.T) {
	var in *Instruments
	rec := in.For(Labels{Transport: "grpc"})
	if rec != nil {
		t.Fatalf("For on nil Instruments = %v, want nil", rec)
	}
	// Must not panic.
	rec.Observe(context.Background(), ResultOK, time.Millisecond, 10, true, 20)
}
