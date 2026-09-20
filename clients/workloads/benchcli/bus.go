package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
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
	}
}

// setup resolves the codec/pool/fixture/options and builds the metrics
// instruments + cell shared by all bus runs.
func (b *busFlags) setup(transportLabel, mode string, tenum workloadsv1.Transport) (*busRun, error) {
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
		Mode: mode, Region: *b.region, Role: "client", Tier: tierForTransport(tenum),
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

// runReqLoop is the request/response latency loop shared by the NATS/RabbitMQ/
// Valkey RPC subcommands: it re-stamps the envelope each iteration, sends via
// the Requester, records RTT + the mbbench_* counters, and verifies the reply
// echoes the request's message_id (the cheap per-message correctness check —
// the response survived the codec + transport round trip; design §9.4).
func (r *busRun) runReqLoop(req proto.Message, newResp func() proto.Message, n int, runID, fixture string, timeout time.Duration) harness.Latencies {
	ctx := context.Background()
	r.inst.SetActiveCell(ctx, r.cell, true)
	defer r.inst.SetActiveCell(ctx, r.cell, false)
	env := req.(hasEnvelope).GetEnvelope()
	resp := newResp()
	var lat harness.Latencies
	for i := 0; i < n; i++ {
		if err := envelope.Fill(env, runID, uint64(i), r.cenum, r.tenum, fixture); err != nil {
			lat.AddError()
			continue
		}
		proto.Reset(resp)
		rctx, cancel := context.WithTimeout(ctx, timeout)
		t0 := time.Now()
		err := r.request(rctx, req, resp)
		rtt := time.Since(t0)
		cancel()
		r.inst.Message(ctx, r.cell, harness.ResultSent)
		if err != nil {
			lat.AddError()
			r.inst.Error(ctx, r.cell, "transport")
			if lat.NumErrors() <= 3 {
				fmt.Fprintf(os.Stderr, "message %d: %v\n", i, err)
			}
			continue
		}
		if re := envelope.Of(resp); re == nil || !bytes.Equal(re.GetMessageId(), env.GetMessageId()) {
			lat.AddError()
			r.inst.Error(ctx, r.cell, "corrupt")
			continue
		}
		r.inst.Message(ctx, r.cell, harness.ResultReceived)
		r.inst.RTT(ctx, r.cell, rtt)
		lat.Add(rtt)
	}
	return lat
}

// request dispatches through the active Requester.
func (r *busRun) request(ctx context.Context, req, resp proto.Message) error {
	return r.requester.Request(ctx, req, resp)
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
