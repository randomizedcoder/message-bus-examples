// Command rpc-benchmark is the load driver for the RPC lab (§25): it drives an
// rpc.Call workload against a GatewayService endpoint (gateway-A, or a service
// directly) and reports the latency distribution and error/timeout rates. It
// reuses the proto-bench latency machinery (harness.HDR, coordinated-omission
// correction) so RPC numbers are comparable to the codec/transport benchmarks.
//
// It runs every transport binding (grpc, nats, natsjs, rabbitmq*, mqtt, valkey*)
// behind the benchClient interface, and supports the three reproducibility
// pillars the proposal asks for: a §26 payload-size sweep (-sizes, over the
// echo.Echo service), §22 rpc_* Prometheus metrics (-metrics-addr), and a §35
// reproducible run.json (-out) carrying git/host/versions and one cell per size.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/grpcx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/mqttx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/natsx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/rabbitmqx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/rpcmetrics"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/tracing"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/valkeyx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/runrecord"
)

func main() {
	addr := flag.String("addr", "localhost:9430", "endpoint: a GatewayService host:port (grpc), a NATS broker host:port (nats/natsjs), or a RabbitMQ host:port / amqp URL (rabbitmq*)")
	transport := flag.String("transport", "grpc", "transport: grpc | nats | natsjs (durable JetStream) | rabbitmq (reply queue) | rabbitmq-direct (Direct Reply-To) | mqtt | valkey (Pub/Sub) | valkey-stream (durable Streams)")
	amqpUser := flag.String("user", "admin", "RabbitMQ username (rabbitmq* transports)")
	amqpPass := flag.String("pass", os.Getenv("RABBITMQ_PASS"), "RabbitMQ password (rabbitmq* transports; default $RABBITMQ_PASS)")
	mqttQoS := flag.Int("mqtt-qos", 1, "MQTT QoS (0, 1, or 2) for -transport mqtt; reported as mqtt-qos<N> so QoS runs stay distinctly labeled (§13)")
	valkeyPass := flag.String("valkey-pass", os.Getenv("VALKEY_PASSWORD"), "Valkey primary password (valkey* transports; default $VALKEY_PASSWORD). For valkey*, -addr is the comma-separated Sentinel list")
	codec := flag.String("codec", "proto", "envelope codec label for metrics / run.json (proto for every current transport)")
	mode := flag.String("mode", "closed", "load mode: closed (concurrency+requests) | open (rate+duration)")
	requests := flag.Int("requests", 10000, "closed mode: total request budget")
	concurrency := flag.Int("concurrency", 32, "closed mode: number of concurrent workers")
	rate := flag.Float64("rate", 0, "open mode: offered requests/sec")
	duration := flag.Duration("duration", 10*time.Second, "open mode: wall-clock budget")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call timeout")
	customerID := flag.String("customer-id", "11111111-1111-1111-1111-111111111111", "customer id (uuid) for the Lookup payload")
	region := flag.String("region", "us-west-2", "region for the Lookup payload")
	sizes := flag.String("sizes", "", "payload-size sweep (§26): comma list like 100B,1KiB,10KiB,100KiB,1MiB or the word 'default'; each size runs an echo.Echo round-trip of that request size. Empty = one representative customer.Lookup")
	metricsAddr := flag.String("metrics-addr", "", "serve rpc_* Prometheus metrics on this host:port (§22); empty disables")
	metricsLinger := flag.Duration("metrics-linger", 0, "after the run, keep -metrics-addr serving for this long so a scrape can collect the final counters")
	out := flag.String("out", "", "write a reproducible run.json here (§35); empty disables")
	retries := flag.Int("retries", 0, "§29: retry a transient failure (timeout / unavailable) up to this many times, re-sending under a new request_id but the same idempotency_key so a service with an idempotency cache still executes the operation once")
	retryBackoff := flag.Duration("retry-backoff", 0, "fixed delay before each retry when -retries > 0")
	idemKey := flag.String("idempotency-key", "", "§29: stamp every request with this idempotency_key, so after the first OK the service replays the cached response — a deterministic duplicate-operation demonstration. Empty = each call is a distinct logical operation")
	asJSON := flag.Bool("json", false, "emit each cell as a JSON object (one per line) instead of the text report")
	traceMode := flag.String("trace", "off", "distributed tracing (§23): off | stdout (stdout writes each span as JSON to stderr; enable it on gateway + service too to capture the whole path)")
	flag.Parse()

	shutdownTracing, terr := tracing.NewProvider("rpc-benchmark", *traceMode)
	if terr != nil {
		log.Fatalf("rpc-benchmark: tracing: %v", terr)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	cfg := benchConfig{
		mode:        *mode,
		requests:    *requests,
		concurrency: *concurrency,
		rate:        *rate,
		duration:    *duration,
		timeout:     *timeout,
	}
	if err := cfg.validate(); err != nil {
		log.Fatalf("rpc-benchmark: %v", err)
	}

	wls, err := buildWorkloads(*sizes, *customerID, *region, *timeout, *idemKey)
	if err != nil {
		log.Fatalf("rpc-benchmark: %v", err)
	}

	// The driver programs to rpc.Client; every transport satisfies it, so the
	// load engine (bench.go) is unchanged across transports. label is the
	// transport as reported: mqtt carries its QoS so QoS 0/1/2 runs stay
	// distinctly labeled (§13, they are not equivalent semantics).
	var client rpc.Client
	label := *transport
	switch *transport {
	case "grpc":
		client, err = grpcx.Dial(*addr)
	case "nats":
		client, err = natsx.Dial(*addr)
	case "natsjs":
		client, err = natsx.DialJetStream(*addr)
	case "rabbitmq":
		client, err = rabbitmqx.Dial(rabbitmqx.URL(*addr, *amqpUser, *amqpPass), rabbitmqx.ModeReplyQueue)
	case "rabbitmq-direct":
		client, err = rabbitmqx.Dial(rabbitmqx.URL(*addr, *amqpUser, *amqpPass), rabbitmqx.ModeDirect)
	case "mqtt":
		if *mqttQoS < 0 || *mqttQoS > 2 {
			log.Fatalf("rpc-benchmark: -mqtt-qos must be 0, 1, or 2, got %d", *mqttQoS)
		}
		label = fmt.Sprintf("mqtt-qos%d", *mqttQoS)
		client, err = mqttx.Dial(*addr, byte(*mqttQoS))
	case "valkey":
		client, err = valkeyx.Dial(*addr, *valkeyPass)
	case "valkey-stream":
		client, err = valkeyx.DialStream(*addr, *valkeyPass)
	default:
		log.Fatalf("rpc-benchmark: unknown -transport %q (grpc|nats|natsjs|rabbitmq|rabbitmq-direct|mqtt|valkey|valkey-stream)", *transport)
	}
	if err != nil {
		log.Fatalf("rpc-benchmark: dial %s over %s: %v", *addr, label, err)
	}
	// §23 tracing: wrap the transport so each call is a client span that injects
	// trace context into the envelope. This sits INSIDE the retry wrapper below so
	// every attempt is its own span under its own request_id.
	client = rpc.NewTracingClient(client)
	// §29 safe retry: wrap the transport so a transient failure is re-sent under a
	// fresh request_id but the same idempotency_key. The wrapper is a drop-in
	// rpc.Client, so the load engine is unchanged.
	if *retries > 0 {
		client = rpc.NewRetryClient(client, rpc.RetryPolicy{MaxAttempts: *retries + 1, Backoff: *retryBackoff})
	}
	defer client.Close()

	// Optional rpc_* metrics (§22). ByteBucketsView gives the *_bytes histograms
	// byte-scaled buckets; NewProvider already covers the *_seconds ones.
	var inst *rpcmetrics.Instruments
	if *metricsAddr != "" {
		mp, _, merr := metrics.NewProvider(*metricsAddr, rpcmetrics.ByteBucketsView)
		if merr != nil {
			log.Fatalf("rpc-benchmark: metrics: %v", merr)
		}
		if inst, merr = rpcmetrics.New(mp.Meter("github.com/randomizedcoder/message-bus-examples/clients/rpc-benchmark")); merr != nil {
			log.Fatalf("rpc-benchmark: metrics instruments: %v", merr)
		}
		log.Printf("rpc-benchmark: serving rpc_* metrics on %s", *metricsAddr)
	}

	started := time.Now()
	cells := runWorkloads(context.Background(), client, wls, cfg, inst, label, *codec)
	finished := time.Now()

	for _, c := range cells {
		if *asJSON {
			emitJSON(label, *codec, cfg, c)
		} else {
			printReport(label, *codec, cfg, c)
		}
	}

	if *out != "" {
		run := buildRun(cells, cfg, label, *codec, started, finished)
		if err := runrecord.Save(*out, run); err != nil {
			log.Fatalf("rpc-benchmark: write run.json %s: %v", *out, err)
		}
		log.Printf("rpc-benchmark: wrote run.json to %s (%d cell(s))", *out, len(cells))
	}

	if *metricsAddr != "" && *metricsLinger > 0 {
		log.Printf("rpc-benchmark: holding metrics open for %s", *metricsLinger)
		time.Sleep(*metricsLinger)
	}

	// A run in which no cell produced a single successful sample is a failure of
	// the run itself.
	total := 0
	for _, c := range cells {
		total += c.res.Summary.Count
	}
	if total == 0 {
		// os.Exit skips the deferred tracer flush; export any pending spans first.
		_ = shutdownTracing(context.Background())
		os.Exit(1)
	}
}

// printReport writes the human-readable per-cell block: identity, the accounting
// line (ok / errors / timeouts / non-ok), and the p50…max latency ladder with
// p95 (§27) and the coordinated-omission-corrected p99 when open-loop.
func printReport(transport, codec string, cfg benchConfig, c cellRun) {
	s := c.res.Summary
	fmt.Printf("rpc-benchmark  transport=%s  codec=%s  fixture=%s  mode=%s  elapsed=%s\n",
		transport, codec, c.wl.fixture, cfg.mode, c.res.Elapsed.Round(time.Millisecond))
	if c.wl.blobBytes > 0 {
		fmt.Printf("  payload=%s (%d B blob)  request-wire=%d B\n", c.wl.fixture, c.wl.blobBytes, c.reqBytes)
	}
	if cfg.mode == modeClosed {
		fmt.Printf("  budget=%d concurrency=%d\n", cfg.requests, cfg.concurrency)
	} else {
		fmt.Printf("  offered-rate=%.0f/s duration=%s\n", cfg.rate, cfg.duration)
	}
	total := int64(s.Count) + int64(s.Errors)
	fmt.Printf("  ok=%d errors=%d (timeouts=%d non-ok=%d) of %d  throughput=%.0f ok/s\n",
		s.Count, s.Errors, c.res.Timeouts, c.res.NonOK, total, s.ThroughputPerSec)
	if c.res.Retries > 0 || c.res.Replays > 0 {
		fmt.Printf("  retries=%d idempotent-replays=%d (§29)\n", c.res.Retries, c.res.Replays)
	}
	fmt.Printf("  latency  min=%s p50=%s p90=%s p95=%s p99=%s p99.9=%s max=%s mean=%s\n",
		d(s.Min), d(s.P50), d(s.P90), d(c.res.hdr.ValueAt(95)), d(s.P99), d(s.P999), d(s.Max), d(s.Mean))
	if c.res.CorrectedOK {
		fmt.Printf("  co-corrected p99=%s\n", d(c.res.CorrectedP99))
	}
}

// benchJSON is the machine-readable per-cell shape (one JSON object per line);
// run.json (-out) is the richer §35 record.
type benchJSON struct {
	Transport    string  `json:"transport"`
	Codec        string  `json:"codec"`
	Fixture      string  `json:"fixture"`
	PayloadBytes int     `json:"payload_bytes,omitempty"`
	RequestBytes int     `json:"request_wire_bytes"`
	Mode         string  `json:"mode"`
	ElapsedMS    float64 `json:"elapsed_ms"`
	OK           int     `json:"ok"`
	Errors       int     `json:"errors"`
	Timeouts     int64   `json:"timeouts"`
	NonOK        int64   `json:"non_ok"`
	Retries      int64   `json:"retries"`
	Replays      int64   `json:"idempotent_replays"`
	ThroughputPS float64 `json:"throughput_ok_per_sec"`
	P50MS        float64 `json:"p50_ms"`
	P90MS        float64 `json:"p90_ms"`
	P95MS        float64 `json:"p95_ms"`
	P99MS        float64 `json:"p99_ms"`
	P999MS       float64 `json:"p99_9_ms"`
	MaxMS        float64 `json:"max_ms"`
	CorrP99MS    float64 `json:"co_corrected_p99_ms,omitempty"`
}

func emitJSON(transport, codec string, cfg benchConfig, c cellRun) {
	s := c.res.Summary
	j := benchJSON{
		Transport:    transport,
		Codec:        codec,
		Fixture:      c.wl.fixture,
		PayloadBytes: c.wl.blobBytes,
		RequestBytes: c.reqBytes,
		Mode:         cfg.mode,
		ElapsedMS:    ms(c.res.Elapsed),
		OK:           s.Count,
		Errors:       s.Errors,
		Timeouts:     c.res.Timeouts,
		NonOK:        c.res.NonOK,
		Retries:      c.res.Retries,
		Replays:      c.res.Replays,
		ThroughputPS: s.ThroughputPerSec,
		P50MS:        ms(s.P50),
		P90MS:        ms(s.P90),
		P95MS:        ms(c.res.hdr.ValueAt(95)),
		P99MS:        ms(s.P99),
		P999MS:       ms(s.P999),
		MaxMS:        ms(s.Max),
	}
	if c.res.CorrectedOK {
		j.CorrP99MS = ms(c.res.CorrectedP99)
	}
	b, _ := json.Marshal(j)
	fmt.Println(string(b))
}

func d(v time.Duration) string { return v.Round(time.Microsecond).String() }
func ms(v time.Duration) float64 {
	return float64(v.Microseconds()) / 1000.0
}
