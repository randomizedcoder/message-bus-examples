package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
	grpctransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/grpc"
)

// hasEnvelope is satisfied by every request message that carries an Envelope as
// field 1 (all of them). It lets the loop re-stamp the envelope per iteration
// without knowing the concrete request type.
type hasEnvelope interface{ GetEnvelope() *workloadsv1.Envelope }

// runGRPC drives a gRPC unary latency loop against a region-agent: dial with the
// pooled codecV2, then send -n requests for the chosen fixture (tiny→Ping, the
// DeployRequest fixtures→Deploy), measuring monotonic RTT and recording the
// mbbench_* instruments. It is the transport counterpart to `benchcli codec`
// and shares the exact codec + pool code, so the numbers are comparable
// (design §6). -mode selects the run mode (latency|windowed|openloop|saturation|
// coldstart, §8.2); the shared driver in drive.go implements them.
func runGRPC(args []string) error {
	fs := flag.NewFlagSet("grpc", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:30710", "region-agent gRPC address (host:port)")
	codecName := fs.String("codec", "proto", "proto|protojson|vtproto")
	poolName := fs.String("pool", "all", "none|messages|buffers|all")
	fixtureName := fs.String("fixture", "medium", "tiny|small|medium|large|sparse|dense")
	region := fs.String("region", "us-west-2", "region label for metrics")
	compression := fs.String("compression", "none", "none|gzip (gRPC only)")
	n := fs.Int("n", 10000, "number of requests")
	seed := fs.Uint64("seed", 42, "corpus seed")
	runID := fs.String("run-id", "local", "run id (metrics/log correlation only)")
	metricsAddr := fs.String("metrics-addr", "", "serve /metrics here (empty = off)")
	validate := fs.Bool("validate", false, "protovalidate each response")
	timeout := fs.Duration("timeout", 5*time.Second, "per-request timeout")
	gc := fs.String("gc", "default", "gc profile label for the cell id (default|limit)")
	repeat := fs.Int("repeat", 0, "repeat index for the emitted cell record")
	out := fs.String("out", "", "write the per-cell record JSON here (design §8.5; empty = off)")
	hgrm := fs.String("hgrm", "", "write the HDR .hgrm histogram here (empty = off)")
	runMode := fs.String("mode", "latency", "run mode: latency|windowed|openloop|saturation|coldstart (design §8.2)")
	inflight := fs.Int("inflight", 0, "in-flight concurrency (0 = mode default: latency 1; windowed needs >=2; openloop/saturation cap 1024)")
	rate := fs.Float64("rate", 0, "offered load in req/s (openloop); base rate the ramp doubles from (saturation)")
	duration := fs.Duration("duration", 30*time.Second, "wall-clock budget for open-loop modes")
	step := fs.Duration("step", 10*time.Second, "per-ramp-step window (saturation)")
	floor := fs.Duration("floor", 0, "reference p99 for the saturation ceiling (0 = measure from the first ramp step)")
	conns := fs.Int("conns", 0, "fresh connections to sample (coldstart; 0 = 20)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cdc, err := codec.ByName(*codecName)
	if err != nil {
		return err
	}
	mode, err := pool.ParseMode(*poolName)
	if err != nil {
		return err
	}
	fixture := corpus.Fixture(*fixtureName)
	c := corpus.New(*seed)
	req, newResp, err := grpcPair(c, fixture)
	if err != nil {
		return err
	}
	codecEnum, err := codecEnum(*codecName)
	if err != nil {
		return err
	}

	opts := transport.Options{Codec: cdc, Pool: mode.NewBufferPool(), Region: *region, Validate: *validate}
	cc, err := grpctransport.Dial(*addr, opts, *compression)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *addr, err)
	}
	req0 := grpctransport.NewRequester(cc)
	defer req0.Close()

	// Metrics + instruments (role=client). The provider serves /metrics on
	// metricsAddr when set; the Prometheus job scrapes it over the host bridge.
	mp, _, err := metrics.NewProvider(*metricsAddr)
	if err != nil {
		return err
	}
	inst, err := harness.New(mp.Meter("github.com/randomizedcoder/message-bus-examples/clients/benchcli"))
	if err != nil {
		return err
	}
	cell := harness.Cell{
		Transport: "grpc_unary", Codec: *codecName, Fixture: *fixtureName, Pool: mode.String(),
		GC: *gc, Mode: *runMode, Region: *region, Role: "client", Tier: "rpc",
	}

	rc := &reqCell{
		requester: req0, reqProto: req, newResp: newResp,
		cenum: codecEnum, tenum: workloadsv1.Transport_TRANSPORT_GRPC_UNARY, inst: inst, cell: cell,
		// coldstart dials a fresh channel per sample; teardown closes it.
		dial: func() (transport.Requester, func(), error) {
			cc, err := grpctransport.Dial(*addr, opts, *compression)
			if err != nil {
				return nil, nil, err
			}
			r := grpctransport.NewRequester(cc)
			return r, func() { r.Close(); cc.Close() }, nil
		},
	}
	cfg := driveConfig{
		mode: *runMode, n: *n, inflight: *inflight, rate: *rate,
		duration: *duration, step: *step, floor: *floor, conns: *conns,
		timeout: *timeout, runID: *runID, fixture: *fixtureName,
	}
	res, err := drive(context.Background(), rc, cfg, wireLen(cdc, req))
	if err != nil {
		return err
	}
	printGRPCSummary(*addr, cell, res.hdr.Summarize(res.elapsed), res.elapsed)
	printRunExtras(res)
	return emitCell(emitOptions{out: *out, hgrm: *hgrm, repeat: *repeat}, cell, res)
}

// grpcPair returns the request message for the fixture and a constructor for a
// fresh response of the matching type. tiny is the Ping "codec floor"; the
// DeployRequest fixtures exercise the nested/repeated/map paths. max (a LogChunk)
// is a streaming/bus payload, not a unary request.
func grpcPair(c *corpus.Corpus, f corpus.Fixture) (proto.Message, func() proto.Message, error) {
	m, err := c.Message(f)
	if err != nil {
		return nil, nil, err
	}
	switch m.(type) {
	case *workloadsv1.PingRequest:
		return m, func() proto.Message { return &workloadsv1.PingResponse{} }, nil
	case *workloadsv1.DeployRequest:
		return m, func() proto.Message { return &workloadsv1.DeployResponse{} }, nil
	default:
		return nil, nil, fmt.Errorf("fixture %q (%T) is not a gRPC unary request; use a streaming/bus transport", f, m)
	}
}

func codecEnum(name string) (workloadsv1.Codec, error) {
	switch name {
	case "proto":
		return workloadsv1.Codec_CODEC_PROTO, nil
	case "protojson":
		return workloadsv1.Codec_CODEC_PROTOJSON, nil
	case "vtproto":
		return workloadsv1.Codec_CODEC_VTPROTO, nil
	default:
		return 0, fmt.Errorf("unknown codec %q", name)
	}
}

func printGRPCSummary(addr string, cell harness.Cell, s harness.Summary, elapsed time.Duration) {
	fmt.Printf("grpc_unary → %s  codec=%s fixture=%s pool=%s\n", addr, cell.Codec, cell.Fixture, cell.Pool)
	fmt.Printf("  requests        %d (errors %d)\n", s.Count+s.Errors, s.Errors)
	fmt.Printf("  throughput      %.0f req/s over %s\n", s.ThroughputPerSec, elapsed.Round(time.Millisecond))
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "  RTT\tmin\tp50\tp90\tp99\tp99.9\tmax\tmean\n")
	fmt.Fprintf(tw, "  \t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		d(s.Min), d(s.P50), d(s.P90), d(s.P99), d(s.P999), d(s.Max), d(s.Mean))
	tw.Flush()
}

func d(t time.Duration) string { return t.Round(time.Microsecond).String() }
