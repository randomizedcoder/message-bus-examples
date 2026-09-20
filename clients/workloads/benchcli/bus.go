package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// busFlags is the flag set shared by the bus subcommands (the request/response
// ones and MQTT). Each subcommand parses these plus a couple of its own.
type busFlags struct {
	fs                           *flag.FlagSet
	addr                         *string
	codecName, poolName, fixture *string
	region, runID                *string
	metricsAddr                  *string
	user, pass, sentinels        *string
	qos                          *int
	n                            *int
	seed                         *uint64
	timeout                      *time.Duration
	validate                     *bool
	gc                           *string
	repeat                       *int
	out, hgrm                    *string
	mode                         *string
	inflight                     *int
	rate                         *float64
	duration                     *time.Duration
	step                         *time.Duration
	floor                        *time.Duration
	conns                        *int
}

func newBusFlags(name, defaultAddr, defaultFixture string) *busFlags {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	return &busFlags{
		fs:          fs,
		addr:        fs.String("addr", defaultAddr, "broker address (host:port)"),
		codecName:   fs.String("codec", "proto", "proto|protojson|vtproto"),
		poolName:    fs.String("pool", "all", "none|messages|buffers|all"),
		fixture:     fs.String("fixture", defaultFixture, "corpus fixture"),
		region:      fs.String("region", "us-west-2", "region label + subject/queue/stream/topic segment"),
		runID:       fs.String("run-id", "local", "run id (metrics/log correlation only)"),
		metricsAddr: fs.String("metrics-addr", "", "serve /metrics here (empty = off)"),
		user:        fs.String("user", "admin", "broker username (rabbitmq)"),
		pass:        fs.String("pass", os.Getenv("RABBITMQ_PASS"), "broker password (rabbitmq/valkey; default from *_PASS env)"),
		sentinels:   fs.String("sentinels", "", "valkey Sentinel host:port list (comma-separated; overrides -addr)"),
		qos:         fs.Int("qos", 1, "MQTT QoS (0|1)"),
		n:           fs.Int("n", 10000, "number of messages"),
		seed:        fs.Uint64("seed", 42, "corpus seed"),
		timeout:     fs.Duration("timeout", 5*time.Second, "per-request timeout"),
		validate:    fs.Bool("validate", false, "protovalidate each response"),
		gc:          fs.String("gc", "default", "gc profile label for the cell id (default|limit)"),
		repeat:      fs.Int("repeat", 0, "repeat index for the emitted cell record"),
		out:         fs.String("out", "", "write the per-cell record JSON here (design §8.5; empty = off)"),
		hgrm:        fs.String("hgrm", "", "write the HDR .hgrm histogram here (empty = off)"),
		mode:        fs.String("mode", "latency", "run mode: latency|windowed|openloop|saturation|coldstart|fault (design §8.2)"),
		inflight:    fs.Int("inflight", 0, "in-flight concurrency (0 = mode default: latency 1; windowed needs >=2; openloop/saturation/fault cap 1024)"),
		rate:        fs.Float64("rate", 0, "offered load in msg/s (openloop/fault); base rate the ramp doubles from (saturation)"),
		duration:    fs.Duration("duration", 30*time.Second, "wall-clock budget for open-loop modes"),
		step:        fs.Duration("step", 10*time.Second, "per-ramp-step window (saturation)"),
		floor:       fs.Duration("floor", 0, "reference p99 for the saturation ceiling (0 = measure from the first ramp step)"),
		conns:       fs.Int("conns", 0, "fresh connections to sample (coldstart; 0 = 20)"),
	}
}

// emit returns the -out/-hgrm/-repeat knobs as emitOptions.
func (b *busFlags) emit() emitOptions {
	return emitOptions{out: *b.out, hgrm: *b.hgrm, repeat: *b.repeat}
}

// driveConfig assembles the run-mode configuration from the parsed flags.
func (b *busFlags) driveConfig() driveConfig {
	return driveConfig{
		mode: *b.mode, n: *b.n, inflight: *b.inflight, rate: *b.rate,
		duration: *b.duration, step: *b.step, floor: *b.floor, conns: *b.conns,
		timeout: *b.timeout, runID: *b.runID, fixture: *b.fixture,
	}
}

// setup resolves the codec/pool/fixture/options and builds the metrics
// instruments + cell shared by all bus runs. The cell's mode comes from -mode.
func (b *busFlags) setup(transportLabel string, tenum workloadsv1.Transport) (*busRun, error) {
	cdc, err := codec.ByName(*b.codecName)
	if err != nil {
		return nil, err
	}
	pmode, err := pool.ParseMode(*b.poolName)
	if err != nil {
		return nil, err
	}
	cenum, err := codecEnum(*b.codecName)
	if err != nil {
		return nil, err
	}
	opts := transport.Options{Codec: cdc, Pool: pmode.NewBufferPool(), Region: *b.region, Validate: *b.validate}
	mp, _, err := metrics.NewProvider(*b.metricsAddr)
	if err != nil {
		return nil, err
	}
	inst, err := harness.New(mp.Meter("github.com/randomizedcoder/message-bus-examples/clients/benchcli"))
	if err != nil {
		return nil, err
	}
	cell := harness.Cell{
		Transport: transportLabel, Codec: *b.codecName, Fixture: *b.fixture, Pool: pmode.String(),
		GC: *b.gc, Mode: *b.mode, Region: *b.region, Role: "client", Tier: tierForTransport(tenum),
	}
	return &busRun{opts: opts, inst: inst, cell: cell, cenum: cenum, tenum: tenum, corpus: corpus.New(*b.seed)}, nil
}

type busRun struct {
	opts      transport.Options
	inst      *harness.Instruments
	cell      harness.Cell
	cenum     workloadsv1.Codec
	tenum     workloadsv1.Transport
	corpus    *corpus.Corpus
	requester transport.Requester // set by the RPC subcommands (nil for MQTT)
}

// tierForTransport maps the transport enum to its delivery-semantics tier
// (design §2.3) for the metrics label.
func tierForTransport(t workloadsv1.Transport) string {
	switch t {
	case workloadsv1.Transport_TRANSPORT_MQTT:
		return "at_most_once" // fire-and-forget; QoS 1 shades toward at-least-once (design §2.3)
	case workloadsv1.Transport_TRANSPORT_NATS_JETSTREAM,
		workloadsv1.Transport_TRANSPORT_RABBITMQ_QUORUM, workloadsv1.Transport_TRANSPORT_VALKEY_STREAM:
		return "at_least_once"
	default:
		return "rpc"
	}
}

// busPair returns the request message + a response constructor for a fixture on
// a request/response bus. Only the DeployRequest fixtures are RPC payloads
// (tiny=Ping and max=LogChunk are gRPC/stream shapes — design §2.2).
func busPair(c *corpus.Corpus, f corpus.Fixture) (proto.Message, func() proto.Message, error) {
	m, err := c.Message(f)
	if err != nil {
		return nil, nil, err
	}
	if _, ok := m.(*workloadsv1.DeployRequest); !ok {
		return nil, nil, fmt.Errorf("fixture %q (%T) is not a bus RPC request; use small|medium|large|sparse|dense", f, m)
	}
	return m, func() proto.Message { return &workloadsv1.DeployResponse{} }, nil
}
