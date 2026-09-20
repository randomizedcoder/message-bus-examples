package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/runrecord"
)

// emitOptions are the -out/-hgrm/-repeat knobs the host harness passes so one
// benchcli invocation drops a per-cell record it can fold into run.json (design
// §8.5, §9.3). All empty/zero for interactive use, where emitCell is a no-op.
type emitOptions struct {
	out    string // per-cell record JSON path
	hgrm   string // .hgrm histogram path
	repeat int
}

// cellResult is what one finished run-mode cell measured on the driver side:
// the histogram plus the run-mode knobs (inflight/rate) and the counters the
// mode produced (late_sends). The host harness merges the agent/broker columns
// in later (design §8.4).
type cellResult struct {
	hdr       *harness.HDR
	elapsed   time.Duration
	wireReq   int64
	inflight  int
	rate      float64
	lateSends int64
	missing   int64      // fault mode: sequences that got no valid reply (design §8.4; 0 for other modes)
	sat       *satResult // saturation ramp verdict (nil for other modes; print-only)
}

// cellID is the run.json cell key: transport/codec/fixture/pool/gc/mode
// (design §8.5, e.g. "grpc_unary/proto/medium/all/default/latency").
func cellID(c harness.Cell) string {
	return strings.Join([]string{c.Transport, c.Codec, c.Fixture, c.Pool, c.GC, c.Mode}, "/")
}

// summaryFromHDR fills the driver-measurable columns of a runrecord.Summary for
// a finished cell. Agent/broker/one-way/GC columns are merged in later by the
// host harness (P4b-3) from the agent memstats and Prometheus; this is exactly
// what the driver alone knows (design §8.4).
func summaryFromHDR(c harness.Cell, r cellResult) runrecord.Summary {
	s := r.hdr.Summarize(r.elapsed)
	us := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000 }
	out := runrecord.Summary{
		Tier: c.Tier, Transport: c.Transport, Codec: c.Codec, Fixture: c.Fixture,
		Pool: c.Pool, GC: c.GC, Mode: c.Mode, Inflight: r.inflight, Rate: r.rate,
		Msgs: int64(s.Count), Errors: r.hdr.ErrorKinds(), ThroughputMsgS: s.ThroughputPerSec,
		RTTP50US: us(s.P50), RTTP90US: us(s.P90), RTTP99US: us(s.P99),
		RTTP999US: us(s.P999), RTTMaxUS: us(s.Max),
		WireBytesReq: r.wireReq, LateSends: r.lateSends, Missing: r.missing,
	}
	if r.wireReq > 0 && s.ThroughputPerSec > 0 {
		out.ThroughputMiBS = s.ThroughputPerSec * float64(r.wireReq) / (1024 * 1024)
	}
	if p99, ok := r.hdr.CorrectedP99(); ok {
		out.RTTCoCorrectedP99US = us(p99)
	}
	return out
}

// emitCell writes the .hgrm (when -hgrm set) and the per-cell record JSON (when
// -out set). Both empty → no-op, so interactive runs are unaffected.
func emitCell(e emitOptions, c harness.Cell, r cellResult) error {
	if e.hgrm != "" {
		var buf bytes.Buffer
		if err := r.hdr.WriteHGRM(&buf); err != nil {
			return err
		}
		if err := os.WriteFile(e.hgrm, buf.Bytes(), 0o644); err != nil {
			return err
		}
	}
	if e.out == "" {
		return nil
	}
	cell := runrecord.Cell{
		ID:      cellID(c),
		Repeat:  e.repeat,
		Summary: summaryFromHDR(c, r),
		HGRM:    e.hgrm,
	}
	b, err := json.MarshalIndent(cell, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(e.out, b, 0o644)
}

// printRunExtras prints the mode-specific tail lines a plain summary omits: the
// late-send count for any open-loop run, and the saturation ramp verdict (the
// knee, the floor it was measured against, and where/why the ramp stopped).
func printRunExtras(r cellResult) {
	if r.lateSends > 0 {
		fmt.Printf("  late sends      %d\n", r.lateSends)
	}
	if r.missing > 0 {
		fmt.Printf("  missing         %d (integrity — sequences with no valid reply)\n", r.missing)
	}
	if r.sat == nil {
		return
	}
	s := r.sat
	if s.knee > 0 {
		fmt.Printf("  saturation knee %.0f req/s (floor p99 %s; ramp stopped at %.0f req/s: %s)\n",
			s.knee, d(s.floor), s.stopRate, satReason(s.reason))
	} else {
		fmt.Printf("  saturation knee none — base %.0f req/s already saturates (floor p99 %s; %s)\n",
			s.stopRate, d(s.floor), satReason(s.reason))
	}
}

// satReason expands the saturation stop code into a human phrase.
func satReason(reason string) string {
	switch reason {
	case "p99":
		return "p99 exceeded 10× floor"
	case "late":
		return "late_sends exceeded 1%"
	case "cap":
		return "ramp cap reached without saturating"
	default:
		return reason
	}
}

// wireLen is the on-wire byte size of m under the run's codec (the request
// footprint reported per cell). Returns 0 if the message will not marshal.
func wireLen(c codec.Codec, m proto.Message) int64 {
	b, err := c.MarshalAppend(nil, m)
	if err != nil {
		return 0
	}
	return int64(len(b))
}
