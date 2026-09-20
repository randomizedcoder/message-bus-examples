// Command region-agent is the in-cluster proto-bench server. One replica runs
// per region (pinned to a node); the benchmark driver on the host connects to
// it over a per-region NodePort and measures codec, transport, and GC behaviour
// end-to-end (design §9).
//
// P2 implements the gRPC side: all four generated services are registered (so
// reflection and grpcurl see the whole surface), with the unary RPCs — Deploy,
// Ping, ClockProbe, FetchLogs — handled and the streaming RPCs left returning
// codes.Unimplemented until P3/P4. The bus responders/consumers (NATS,
// RabbitMQ, Valkey, MQTT) and the synthetic telemetry/usage/log emitters arrive
// in P3. /metrics (with Go runtime + process collectors) and /healthz are served
// on -metrics-addr; the gRPC listener is on -grpc-addr.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/timestamppb"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
	grpctransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/grpc"
)

var version = "dev" // set via -ldflags -X main.version=… in mkGoBinary

func main() {
	region := flag.String("region", envOr("REGION", "us-west-2"), "logical region this agent serves")
	grpcAddr := flag.String("grpc-addr", ":9090", "gRPC listen address")
	metricsAddr := flag.String("metrics-addr", ":9464", "/metrics + /healthz listen address")
	codecName := flag.String("codec", "proto", "proto|protojson|vtproto (server-forced codec)")
	poolName := flag.String("pool", "all", "none|messages|buffers|all")
	// Bus endpoints are accepted now (so the Deployment's args are stable) but
	// only wired in P3.
	flag.String("nats", "", "NATS URL (P3)")
	flag.String("amqp", "", "RabbitMQ URL (P3)")
	flag.String("valkey-sentinels", "", "Valkey Sentinel host:port list (P3)")
	flag.String("mqtt", "", "MQTT broker URL (P3)")
	flag.Parse()

	if err := run(*region, *grpcAddr, *metricsAddr, *codecName, *poolName); err != nil {
		log.Fatalf("region-agent: %v", err)
	}
}

func run(region, grpcAddr, metricsAddr, codecName, poolName string) error {
	cdc, err := codec.ByName(codecName)
	if err != nil {
		return err
	}
	mode, err := pool.ParseMode(poolName)
	if err != nil {
		return err
	}
	opts := transport.Options{Codec: cdc, Pool: mode.NewBufferPool(), Region: region}

	// Metrics: build the provider (Go runtime + process collectors + latency
	// buckets) without its own server, then serve /metrics and /healthz together.
	mp, reg, err := metrics.NewProvider("")
	if err != nil {
		return fmt.Errorf("metrics provider: %w", err)
	}
	inst, err := harness.New(mp.Meter("github.com/randomizedcoder/message-bus-examples/clients/region-agent"))
	if err != nil {
		return fmt.Errorf("instruments: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	httpSrv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server: %v", err)
		}
	}()

	responderID := region + "/" + hostname()
	srv := grpctransport.NewServer(opts, grpc.StatsHandler(grpctransport.StatsHandler{}))
	impl := &agent{region: region, pool: poolName, responderID: responderID, inst: inst, validate: opts.Validate}
	workloadsv1.RegisterWorkloadServiceServer(srv, impl)
	workloadsv1.RegisterBenchServiceServer(srv, impl)
	workloadsv1.RegisterTelemetryServiceServer(srv, impl)
	workloadsv1.RegisterLogServiceServer(srv, impl)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", grpcAddr, err)
	}
	log.Printf("region-agent %s (%s) serving gRPC on %s, metrics on %s, codec=%s pool=%s",
		version, responderID, grpcAddr, metricsAddr, codecName, poolName)
	return srv.Serve(lis)
}

// agent implements the four workloads.v1 gRPC services. It embeds the generated
// Unimplemented servers so the streaming RPCs return codes.Unimplemented until
// P3/P4; the unary RPCs below are handled.
type agent struct {
	workloadsv1.UnimplementedWorkloadServiceServer
	workloadsv1.UnimplementedBenchServiceServer
	workloadsv1.UnimplementedTelemetryServiceServer
	workloadsv1.UnimplementedLogServiceServer

	region      string
	pool        string
	responderID string
	inst        *harness.Instruments
	validate    bool
}

// stamp does the receive→send envelope work shared by every unary handler:
// receive-stamp with the accurate wire length from the stats handler, record
// server duration + a received/ok message, and send-stamp just before return.
// It returns the (possibly freshly allocated) envelope to echo in the response
// and the Cell so the caller can add its own records if needed.
//
// A request with no envelope (e.g. a bare grpcurl `{}` probe) gets a synthesized
// one so the handler still returns a valid, responder-stamped envelope instead
// of panicking — the real driver always fills the envelope (design §7.1).
func (a *agent) stamp(ctx context.Context, env *workloadsv1.Envelope) (*workloadsv1.Envelope, harness.Cell, func()) {
	if env == nil {
		env = &workloadsv1.Envelope{}
	}
	start := time.Now()
	cell := a.cell(env)
	envelope.StampReceive(env, a.responderID, nil, false)
	if info := grpctransport.RPCInfoFromContext(ctx); info != nil {
		if wb := info.WireBytes(); wb > 0 {
			env.RequestWireBytes = wb
			a.inst.WireBytes(ctx, cell, int(wb), harness.DirForward)
		}
	}
	a.inst.Message(ctx, cell, harness.ResultReceived)
	done := func() {
		envelope.StampSend(env)
		a.inst.ServerDuration(ctx, cell, time.Since(start))
		a.inst.Message(ctx, cell, harness.ResultOK)
	}
	return env, cell, done
}

func (a *agent) Ping(ctx context.Context, req *workloadsv1.PingRequest) (*workloadsv1.PingResponse, error) {
	env, _, done := a.stamp(ctx, req.GetEnvelope())
	done()
	return &workloadsv1.PingResponse{Envelope: env}, nil
}

func (a *agent) Deploy(ctx context.Context, req *workloadsv1.DeployRequest) (*workloadsv1.DeployResponse, error) {
	env, _, done := a.stamp(ctx, req.GetEnvelope())
	done()
	// A regional agent "applies" the workload to its own cluster, so it returns
	// APPLIED with the assigned cluster (both required by the response's CEL).
	return &workloadsv1.DeployResponse{
		Envelope:        env,
		Ref:             req.GetRef(),
		Status:          workloadsv1.DeployStatus_DEPLOY_STATUS_APPLIED,
		AssignedCluster: &workloadsv1.ClusterRef{Region: a.region, ClusterId: a.region + "-0"},
		Generation:      1,
	}, nil
}

func (a *agent) ClockProbe(ctx context.Context, req *workloadsv1.ClockProbeRequest) (*workloadsv1.ClockProbeResponse, error) {
	t2 := timestamppb.Now() // server receive
	env, _, done := a.stamp(ctx, req.GetEnvelope())
	done()
	return &workloadsv1.ClockProbeResponse{
		Envelope:     env,
		T1:           req.GetT1(),
		T2:           t2,
		T3:           timestamppb.Now(), // server send
		ClockSource:  clockSource(),
		ChronySynced: chronySynced(),
	}, nil
}

func (a *agent) FetchLogs(ctx context.Context, req *workloadsv1.FetchLogsRequest) (*workloadsv1.FetchLogsResponse, error) {
	// FetchLogs is unary; a real fetch is P3. For now return an empty, valid
	// response echoing the envelope so the RPC and reflection work end-to-end.
	env, _, done := a.stamp(ctx, req.GetEnvelope())
	done()
	return &workloadsv1.FetchLogsResponse{Envelope: env}, nil
}

// cell builds the per-request label set from the envelope plus the agent's own
// fixed pool/region (design §11.4, role=server).
func (a *agent) cell(env *workloadsv1.Envelope) harness.Cell {
	return harness.Cell{
		Transport: transportLabel(env.GetTransport()),
		Codec:     codecLabel(env.GetCodec()),
		Fixture:   env.GetFixture(),
		Pool:      a.pool,
		Mode:      "latency",
		Region:    a.region,
		Role:      "server",
		Tier:      tierFor(env.GetTransport()),
	}
}

func codecLabel(c workloadsv1.Codec) string {
	switch c {
	case workloadsv1.Codec_CODEC_PROTO:
		return "proto"
	case workloadsv1.Codec_CODEC_PROTOJSON:
		return "protojson"
	case workloadsv1.Codec_CODEC_VTPROTO:
		return "vtproto"
	default:
		return "unknown"
	}
}

func transportLabel(t workloadsv1.Transport) string {
	switch t {
	case workloadsv1.Transport_TRANSPORT_GRPC_UNARY:
		return "grpc_unary"
	case workloadsv1.Transport_TRANSPORT_GRPC_SERVER_STREAM:
		return "grpc_server_stream"
	case workloadsv1.Transport_TRANSPORT_GRPC_CLIENT_STREAM:
		return "grpc_client_stream"
	case workloadsv1.Transport_TRANSPORT_GRPC_BIDI:
		return "grpc_bidi"
	default:
		return "grpc_unary"
	}
}

// tierFor maps a transport to its delivery-semantics tier (design §2.3). gRPC is
// the request/response tier; the buses join in P3.
func tierFor(workloadsv1.Transport) string { return "rpc" }

// clockSource reads the node's current clocksource (readable from the pod).
func clockSource() string {
	b, err := os.ReadFile("/sys/devices/system/clocksource/clocksource0/current_clocksource")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(b))
}

// chronySynced reports whether chronyc tracking says "Leap status: Normal".
// chronyd runs on the VM host, not in the pod, so this is usually false in
// cluster; the driver's clock probe (P4) is the authoritative offset source.
func chronySynced() bool {
	out, err := exec.Command("chronyc", "-n", "tracking").Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "Leap status     : Normal")
}

func hostname() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
