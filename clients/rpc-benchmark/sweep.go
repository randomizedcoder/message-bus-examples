package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	benchmarkv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/benchmark/v1"
	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/rpcmetrics"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/runrecord"
)

// defaultLadder is the §26 payload-size sweep: 100 B, 1 KiB, 10 KiB, 100 KiB,
// 1 MiB. It is selected with -sizes default; 1 MiB is the AllTypes.blob
// protovalidate ceiling, so the ladder stays within a single valid message.
var defaultLadder = []int{100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024}

// workload is one benchmark cell's request shape: a service.method and a factory
// that mints a fresh Request (new request_id) per call. fixture names the cell
// for the report / run.json; blobBytes is the controlled payload size (0 for the
// representative customer.Lookup, which has no size knob).
type workload struct {
	service   string
	method    string
	fixture   string
	blobBytes int
	newReq    func() *rpcv1.Request
}

// buildWorkloads turns the -sizes flag into the cells to run. Empty means the
// single representative customer.Lookup (unchanged default); otherwise each size
// becomes an echo.Echo round-trip whose AllTypes.blob is that many bytes, so the
// request and its echoed response both grow with the size (§26).
func buildWorkloads(sizes, customerID, region string, timeout time.Duration, idemKey string) ([]workload, error) {
	if strings.TrimSpace(sizes) == "" {
		payload := &benchmarkv1.CustomerLookupRequest{CustomerId: customerID, Region: region}
		return []workload{{
			service: "customer", method: "Lookup", fixture: "customer",
			newReq: mkReq("customer", "Lookup", payload, timeout, idemKey),
		}}, nil
	}
	ns, err := parseSizes(sizes)
	if err != nil {
		return nil, err
	}
	out := make([]workload, 0, len(ns))
	for _, n := range ns {
		payload := &benchmarkv1.AllTypes{Blob: make([]byte, n)}
		out = append(out, workload{
			service: "echo", method: "Echo", fixture: humanSize(n), blobBytes: n,
			newReq: mkReq("echo", "Echo", payload, timeout, idemKey),
		})
	}
	return out, nil
}

// mkReq returns a factory that builds a fresh envelope around a fixed payload;
// only request_id / client_sent_at differ per call. When idemKey is non-empty
// every request carries it, so the whole run is one logical operation and the
// service replays the first OK response for the rest — the deterministic §29
// duplicate-operation demonstration. The payload is fixed and valid, so a build
// failure is a programming error, not a runtime condition.
func mkReq(service, method string, payload proto.Message, timeout time.Duration, idemKey string) func() *rpcv1.Request {
	return func() *rpcv1.Request {
		req, err := rpc.NewRequest(service, method, payload, timeout)
		if err != nil {
			panic(err)
		}
		if idemKey != "" {
			req.IdempotencyKey = idemKey
		}
		return req
	}
}

// protoSize returns the serialized envelope size, the value recorded as the
// cell's request wire footprint.
func protoSize(m proto.Message) int { return proto.Size(m) }

// cellRun couples a workload with its measured result and the driver-side
// allocation delta over the run (§35 / §8.3), so run.json can report both.
type cellRun struct {
	wl       workload
	res      result
	reqBytes int
	mem      runrecord.Memstats
}

// runWorkloads drives each workload as one cell, wrapping it in a MemStats delta
// and, when inst != nil, an rpc_* metrics sink bound to the cell's labels.
func runWorkloads(ctx context.Context, client benchClient, wls []workload, cfg benchConfig, inst *rpcmetrics.Instruments, transport, codec string) []cellRun {
	out := make([]cellRun, 0, len(wls))
	for _, wl := range wls {
		reqBytes := protoSize(wl.newReq())
		var sink *metricsSink
		if inst != nil {
			sink = &metricsSink{
				rec: inst.For(rpcmetrics.Labels{
					Transport: transport, Codec: codec,
					Service: wl.service, Method: wl.method,
				}),
				reqBytes: reqBytes,
			}
		}

		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		res := runBench(ctx, client, wl.newReq, cfg, sink)
		runtime.ReadMemStats(&m1)

		out = append(out, cellRun{
			wl: wl, res: res, reqBytes: reqBytes,
			mem: runrecord.Memstats{
				Mallocs:      m1.Mallocs - m0.Mallocs,
				TotalAlloc:   m1.TotalAlloc - m0.TotalAlloc,
				NumGC:        m1.NumGC - m0.NumGC,
				PauseTotalNs: m1.PauseTotalNs - m0.PauseTotalNs,
			},
		})
	}
	return out
}

// buildRun assembles the reproducible run.json record (§35): git tree, wall
// clock, host, Go version, per-fixture wire sizes, and one cell per workload
// carrying the latency summary, allocation delta, and .hgrm histogram.
func buildRun(cells []cellRun, cfg benchConfig, transport, codec string, started, finished time.Time) *runrecord.Run {
	run := &runrecord.Run{
		RunID:      fmt.Sprintf("rpc-%s", started.UTC().Format("20060102T150405Z")),
		StartedAt:  started.UTC().Format(time.RFC3339),
		FinishedAt: finished.UTC().Format(time.RFC3339),
		Git:        gitInfo(),
		Versions:   map[string]string{"go": runtime.Version()},
		Host:       hostInfo(),
		Corpus:     runrecord.Corpus{Fixtures: map[string]runrecord.FixtureSizes{}},
		Matrix:     runrecord.Matrix{Repeats: 1},
	}
	for _, c := range cells {
		id := fmt.Sprintf("rpc/%s/%s/%s/%s", transport, codec, c.wl.fixture, cfg.mode)
		run.Matrix.Cells = append(run.Matrix.Cells, id)
		run.Corpus.Fixtures[c.wl.fixture] = runrecord.FixtureSizes{ProtoBytes: c.reqBytes}
		run.Cells = append(run.Cells, runrecord.Cell{
			ID:       id,
			Summary:  cellSummary(c, cfg, transport, codec),
			Memstats: map[string]runrecord.Memstats{"driver": c.mem},
			HGRM:     hgrm(c.res.hdr),
		})
	}
	return run
}

// cellSummary maps one cell's harness result onto the flat runrecord.Summary
// column set, mirroring benchcli's summaryFromHDR for the RPC tier (§8.4, §27):
// identity, throughput, the p50…p99.9 + p95 + max latency ladder, wire size, and
// the driver alloc/GC-per-message columns.
func cellSummary(c cellRun, cfg benchConfig, transport, codec string) runrecord.Summary {
	s := c.res.Summary
	us := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000 }
	sum := runrecord.Summary{
		Tier: "rpc", Transport: transport, Codec: codec, Fixture: c.wl.fixture,
		Pool: "none", GC: "default", Mode: cfg.mode,
		Msgs:           int64(s.Count),
		Errors:         c.res.hdr.ErrorKinds(),
		Retries:        c.res.Retries,
		Replays:        c.res.Replays,
		ThroughputMsgS: s.ThroughputPerSec,
		RTTP50US:       us(s.P50),
		RTTP90US:       us(s.P90),
		RTTP95US:       us(c.res.hdr.ValueAt(95)),
		RTTP99US:       us(s.P99),
		RTTP999US:      us(s.P999),
		RTTMaxUS:       us(s.Max),
		WireBytesReq:   int64(c.reqBytes),
	}
	if cfg.mode == modeOpen {
		sum.Rate = cfg.rate
	} else {
		sum.Inflight = cfg.concurrency
	}
	if c.res.CorrectedOK {
		sum.RTTCoCorrectedP99US = us(c.res.CorrectedP99)
	}
	if c.reqBytes > 0 {
		sum.ThroughputMiBS = s.ThroughputPerSec * float64(c.reqBytes) / (1024 * 1024)
	}
	if s.Count > 0 {
		sum.DriverAllocBytesPerMsg = float64(c.mem.TotalAlloc) / float64(s.Count)
		sum.DriverAllocsPerMsg = float64(c.mem.Mallocs) / float64(s.Count)
	}
	return sum
}

// hgrm renders the cell's HDR percentile distribution as a string for run.json.
func hgrm(h *harness.HDR) string {
	if h == nil {
		return ""
	}
	var b bytes.Buffer
	if err := h.WriteHGRM(&b); err != nil {
		return ""
	}
	return b.String()
}

// parseSizes parses a comma list of byte sizes. "default" expands to the §26
// ladder; each entry is a bare byte count or a B/KiB/MiB suffix (case-insensitive).
func parseSizes(csv string) ([]int, error) {
	if strings.EqualFold(strings.TrimSpace(csv), "default") {
		return append([]int(nil), defaultLadder...), nil
	}
	var out []int
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := parseSize(part)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-sizes %q lists no sizes", csv)
	}
	return out, nil
}

// parseSize parses one size token: "100" / "100B" / "1KiB" / "1MiB".
func parseSize(s string) (int, error) {
	s = strings.TrimSpace(s)
	mult := 1
	switch {
	case hasSuffixFold(s, "MiB"):
		mult, s = 1024*1024, s[:len(s)-3]
	case hasSuffixFold(s, "KiB"):
		mult, s = 1024, s[:len(s)-3]
	case hasSuffixFold(s, "B"):
		s = s[:len(s)-1]
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("bad size %q (want e.g. 100B, 1KiB, 1MiB)", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("size %q is negative", s)
	}
	return n * mult, nil
}

func hasSuffixFold(s, suffix string) bool {
	return len(s) >= len(suffix) && strings.EqualFold(s[len(s)-len(suffix):], suffix)
}

// humanSize labels a byte count for the fixture name: exact binary multiples
// render as 1KiB / 100KiB / 1MiB, everything else as a raw byte count.
func humanSize(n int) string {
	switch {
	case n != 0 && n%(1024*1024) == 0:
		return fmt.Sprintf("%dMiB", n/(1024*1024))
	case n != 0 && n%1024 == 0:
		return fmt.Sprintf("%dKiB", n/1024)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// gitInfo reads the tree the run was built from, best-effort: an empty Git{} when
// git is unavailable (the harness can fold it in later, §35).
func gitInfo() runrecord.Git {
	rev, err := runGit("rev-parse", "HEAD")
	if err != nil {
		return runrecord.Git{}
	}
	dirty, _ := runGit("status", "--porcelain")
	return runrecord.Git{Rev: rev, Dirty: strings.TrimSpace(dirty) != ""}
}

func runGit(args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

// hostInfo records the driver machine's CPU model and kernel, best-effort from
// /proc (Linux); missing files leave the fields blank (§35).
func hostInfo() runrecord.Host {
	return runrecord.Host{CPU: cpuModel(), Kernel: kernel()}
}

func cpuModel() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "model name" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func kernel() string {
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
