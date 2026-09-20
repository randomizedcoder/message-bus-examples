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
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
	grpctransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/grpc"
	mqtttransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/mqtt"
	natstransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/nats"
	rmqtransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/rabbitmq"
	valkeytransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/valkey"
)

var version = "dev" // set via -ldflags -X main.version=… in mkGoBinary

func main() {
	region := flag.String("region", envOr("REGION", "us-west-2"), "logical region this agent serves")
	grpcAddr := flag.String("grpc-addr", ":9090", "gRPC listen address")
	metricsAddr := flag.String("metrics-addr", ":9464", "/metrics + /healthz listen address")
	codecName := flag.String("codec", "proto", "proto|protojson|vtproto (server-forced codec)")
	poolName := flag.String("pool", "all", "none|messages|buffers|all")
	// Bus endpoints: when set, the agent starts that bus's responder/consumer for
	// its region (design §3.9). Empty = that bus off.
	nats := flag.String("nats", "", "NATS URL, e.g. nats://nats.nats.svc:4222 (Deploy request-reply)")
	amqp := flag.String("amqp", "", "RabbitMQ URL, e.g. amqp://user:pass@rabbitmq.rabbitmq.svc:5672/ (Deploy RPC)")
	valkeySentinels := flag.String("valkey-sentinels", "", "Valkey Sentinel host:port list, comma-separated (Deploy stream RPC)")
	mqttAddr := flag.String("mqtt", "", "MQTT broker host:port, e.g. mqtt.mqtt.svc:1883 (Telemetry, one-way)")
	// Off by default so the headline latency modes pay nothing for it (fairness
	// rule: integrity is on only in coldstart, fault, and the correctness pass —
	// design §8.3). The k8s-proto-bench harness flips these on for -modes=
	// correctness (§9.4) by patching the Deployment args and waiting for rollout.
	integrity := flag.Bool("integrity", false, "stamp request_wire_bytes + request_sha256 over the received wire (design §9.4)")
	validate := flag.Bool("validate", false, "protovalidate each decoded request; reject violations (design §9.4)")
	flag.Parse()

	buses := busConfig{
		nats: *nats, amqp: *amqp,
		valkeySentinels: *valkeySentinels, valkeyPass: os.Getenv("VALKEY_PASSWORD"),
		mqtt: *mqttAddr,
	}
	if err := run(*region, *grpcAddr, *metricsAddr, *codecName, *poolName, *integrity, *validate, buses); err != nil {
		log.Fatalf("region-agent: %v", err)
	}
}

// busConfig holds the region agent's bus endpoints; an empty field disables that
// bus. valkeyPass comes from the VALKEY_PASSWORD env (the valkey-credentials
// Secret), the AMQP creds are already in the -amqp URL (via $(VAR) expansion in
// the Deployment), and cluster NATS/MQTT are unauthenticated.
type busConfig struct {
	nats            string
	amqp            string
	valkeySentinels string
	valkeyPass      string
	mqtt            string
}

func run(region, grpcAddr, metricsAddr, codecName, poolName string, integrity, validate bool, buses busConfig) error {
	cdc, err := codec.ByName(codecName)
	if err != nil {
		return err
	}
	mode, err := pool.ParseMode(poolName)
	if err != nil {
		return err
	}
	opts := transport.Options{Codec: cdc, Pool: mode.NewBufferPool(), Region: region, Integrity: integrity, Validate: validate}

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

	// Bus responders/consumers for this region (design §3.9). Started before the
	// gRPC Serve blocks; they run until the process exits.
	impl.startBuses(context.Background(), opts, buses)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", grpcAddr, err)
	}
	log.Printf("region-agent %s (%s) serving gRPC on %s, metrics on %s, codec=%s pool=%s",
		version, responderID, grpcAddr, metricsAddr, codecName, poolName)
	return srv.Serve(lis)
}

// newDeployReq / newTelemetry build fresh messages for the bus decoders (the
// subject/queue/stream/topic fixes the type — design §3.9).
func newDeployReq() proto.Message { return &workloadsv1.DeployRequest{} }
func newTelemetry() proto.Message { return &workloadsv1.TelemetrySample{} }

// deployHandler is the transport-neutral Deploy application logic the bus
// responders share (the gRPC path is agent.Deploy). The bus Responder owns
// decode / receive-stamp / validate / send-stamp / encode around it, so this is
// pure app logic plus the role=server message counters.
func (a *agent) deployHandler(ctx context.Context, req proto.Message) (proto.Message, error) {
	dr, ok := req.(*workloadsv1.DeployRequest)
	if !ok {
		return nil, fmt.Errorf("deployHandler: unexpected %T", req)
	}
	cell := a.cell(dr.GetEnvelope())
	a.inst.Message(ctx, cell, harness.ResultReceived)
	resp := &workloadsv1.DeployResponse{
		Envelope:        dr.GetEnvelope(),
		Ref:             dr.GetRef(),
		Status:          workloadsv1.DeployStatus_DEPLOY_STATUS_APPLIED,
		AssignedCluster: &workloadsv1.ClusterRef{Region: a.region, ClusterId: a.region + "-0"},
		Generation:      1,
	}
	a.inst.Message(ctx, cell, harness.ResultOK)
	return resp, nil
}

// startBuses dials each configured bus and launches its responder/consumer in a
// goroutine. A dial/serve failure is logged, not fatal — the agent still serves
// gRPC and the other buses (a broker may simply not be up yet).
func (a *agent) startBuses(ctx context.Context, opts transport.Options, b busConfig) {
	if b.nats != "" {
		if nc, err := natstransport.Dial(b.nats); err != nil {
			log.Printf("nats: dial %s: %v", b.nats, err)
		} else {
			r := natstransport.NewResponder(nc, opts, a.responderID, a.region, newDeployReq)
			go a.serve("nats", r, opts)
			log.Printf("nats responder: %s on %s", b.nats, natstransport.DeploySubject(a.region))
			// JetStream durable telemetry consumer for this region (tier B, §3.9).
			// EnsureStream is idempotent across all agents; a failure here is
			// logged, not fatal — the request-reply responder still serves.
			if js, err := natstransport.JetStream(nc); err != nil {
				log.Printf("nats: jetstream context: %v", err)
			} else if err := natstransport.EnsureStream(js); err != nil {
				log.Printf("nats: ensure stream %s: %v", natstransport.StreamName, err)
			} else {
				cons := natstransport.NewConsumer(js, opts, a.region)
				go a.consumeJetStream(ctx, cons, opts)
				log.Printf("nats jetstream consumer: stream %s subject %s", natstransport.StreamName, natstransport.TelemetrySubject(a.region))
			}
		}
	}
	if b.amqp != "" {
		if conn, err := rmqtransport.Dial(b.amqp); err != nil {
			log.Printf("rabbitmq: dial: %v", err)
		} else {
			if r, err := rmqtransport.NewResponder(conn, opts, a.responderID, a.region, newDeployReq); err != nil {
				log.Printf("rabbitmq: responder: %v", err)
			} else {
				go a.serve("rabbitmq", r, opts)
				log.Printf("rabbitmq responder: key %s", rmqtransport.RoutingKey(a.region))
			}
			// Quorum-queue durable telemetry consumer for this region (tier B,
			// §3.9). Reuses the same connection; a failure here is logged, not
			// fatal — the RPC responder still serves.
			if cons, err := rmqtransport.NewConsumer(conn, opts, a.region); err != nil {
				log.Printf("rabbitmq: quorum consumer: %v", err)
			} else {
				go a.consumeQuorum(ctx, cons, opts)
				log.Printf("rabbitmq quorum consumer: queue %s", rmqtransport.TelemetryQueue(a.region))
			}
		}
	}
	if b.valkeySentinels != "" {
		rdb := newValkeyClient(b.valkeySentinels, b.valkeyPass)
		if r, err := valkeytransport.NewResponder(rdb, opts, a.responderID, a.region, newDeployReq); err != nil {
			log.Printf("valkey: responder: %v", err)
		} else {
			go a.serve("valkey", r, opts)
			log.Printf("valkey responder: stream %s", valkeytransport.DeployStream(a.region))
		}
	}
	if b.mqtt != "" {
		if mc, err := mqtttransport.Dial(b.mqtt, "region-agent-"+a.region); err != nil {
			log.Printf("mqtt: dial %s: %v", b.mqtt, err)
		} else {
			cons := mqtttransport.NewConsumer(mc, opts, a.region, 1)
			go a.consumeTelemetry(ctx, cons, opts)
			log.Printf("mqtt consumer: topic wl/%s/telemetry", a.region)
		}
	}
}

// serve runs a request/response Responder until ctx ends, logging a serve error.
func (a *agent) serve(name string, r transport.Responder, opts transport.Options) {
	if err := r.Serve(context.Background(), a.deployHandler); err != nil {
		log.Printf("%s: serve: %v", name, err)
	}
}

// consumeTelemetry decodes and validates each one-way MQTT telemetry sample,
// recording the role=server counters (there is no reply — §9.4).
func (a *agent) consumeTelemetry(ctx context.Context, cons transport.Consumer, opts transport.Options) {
	err := cons.Consume(ctx, func(m transport.Msg) error {
		req, derr := mqtttransport.Decode(m, newTelemetry, a.responderID, opts)
		cell := a.cell(envelope.Of(req))
		a.inst.Message(ctx, cell, harness.ResultReceived)
		if derr != nil {
			a.inst.Error(ctx, cell, "validate")
			return nil
		}
		a.inst.Message(ctx, cell, harness.ResultOK)
		return nil
	})
	if err != nil {
		log.Printf("mqtt: consume: %v", err)
	}
}

// consumeJetStream decodes each durable telemetry delivery, records the
// role=server counters, and acks it explicitly (tier B: acked after the agent
// has taken it — design §2.3). A redelivery (NumDelivered > 1) is counted so the
// dashboard shows the tier-B redelivery rate; a decode/validate failure is still
// acked (a poison message must not redeliver forever) but counted as an error.
func (a *agent) consumeJetStream(ctx context.Context, cons transport.Consumer, opts transport.Options) {
	err := cons.Consume(ctx, func(m transport.Msg) error {
		req, derr := natstransport.Decode(m, newTelemetry, a.responderID, opts)
		cell := a.cell(envelope.Of(req))
		a.inst.Message(ctx, cell, harness.ResultReceived)
		if natstransport.Redelivered(m) {
			a.inst.Message(ctx, cell, harness.ResultRedelivered)
		}
		if derr != nil {
			a.inst.Error(ctx, cell, "validate")
			_ = m.Ack() // drop the poison message rather than redeliver it forever
			return nil
		}
		a.inst.Message(ctx, cell, harness.ResultOK)
		return m.Ack()
	})
	if err != nil {
		log.Printf("nats jetstream: consume: %v", err)
	}
}

// consumeQuorum decodes each durable quorum-queue telemetry delivery, records the
// role=server counters, and acks it (tier B — design §2.3). A redelivery (the
// AMQP redelivered flag) is counted; a decode/validate failure is still acked (a
// poison message must not requeue forever) but counted as an error.
func (a *agent) consumeQuorum(ctx context.Context, cons transport.Consumer, opts transport.Options) {
	err := cons.Consume(ctx, func(m transport.Msg) error {
		req, derr := rmqtransport.Decode(m, newTelemetry, a.responderID, opts)
		cell := a.cell(envelope.Of(req))
		a.inst.Message(ctx, cell, harness.ResultReceived)
		if rmqtransport.Redelivered(m) {
			a.inst.Message(ctx, cell, harness.ResultRedelivered)
		}
		if derr != nil {
			a.inst.Error(ctx, cell, "validate")
			_ = m.Ack() // drop the poison message rather than requeue it forever
			return nil
		}
		a.inst.Message(ctx, cell, harness.ResultOK)
		return m.Ack()
	})
	if err != nil {
		log.Printf("rabbitmq quorum: consume: %v", err)
	}
}

// newValkeyClient builds a Sentinel-backed FailoverClient (primary discovery +
// failover following), reusing the valkeycli topology (design §6).
func newValkeyClient(sentinels, pass string) *redis.Client {
	var addrs []string
	for _, s := range strings.Split(sentinels, ",") {
		if s = strings.TrimSpace(s); s != "" {
			addrs = append(addrs, s)
		}
	}
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:      "mymaster",
		SentinelAddrs:   addrs,
		Password:        pass,
		MaxRetries:      -1,
		MinRetryBackoff: 200 * time.Millisecond,
	})
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
	case workloadsv1.Transport_TRANSPORT_NATS_REQUEST_REPLY:
		return "nats_request_reply"
	case workloadsv1.Transport_TRANSPORT_NATS_JETSTREAM:
		return "nats_jetstream"
	case workloadsv1.Transport_TRANSPORT_RABBITMQ_RPC:
		return "rabbitmq_rpc"
	case workloadsv1.Transport_TRANSPORT_RABBITMQ_QUORUM:
		return "rabbitmq_quorum"
	case workloadsv1.Transport_TRANSPORT_VALKEY_STREAM:
		return "valkey_stream"
	case workloadsv1.Transport_TRANSPORT_MQTT:
		return "mqtt"
	default:
		return "unknown"
	}
}

// tierFor maps a transport to its delivery-semantics tier (design §2.3): RPC for
// the request/response transports, at-least-once for the durable/streaming ones,
// at-most-once for fire-and-forget MQTT.
func tierFor(t workloadsv1.Transport) string {
	switch t {
	case workloadsv1.Transport_TRANSPORT_MQTT:
		return "at_most_once"
	case workloadsv1.Transport_TRANSPORT_NATS_JETSTREAM,
		workloadsv1.Transport_TRANSPORT_RABBITMQ_QUORUM,
		workloadsv1.Transport_TRANSPORT_VALKEY_STREAM:
		return "at_least_once"
	default:
		return "rpc"
	}
}

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
