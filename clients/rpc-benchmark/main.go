// Command rpc-benchmark is the load driver for the RPC lab (§25): it drives an
// rpc.Call workload against a GatewayService endpoint (gateway-A, or a service
// directly) and reports the latency distribution and error/timeout rates. It
// reuses the proto-bench latency machinery (harness.HDR, coordinated-omission
// correction) so RPC numbers are comparable to the codec/transport benchmarks.
//
// This is the Phase-2 skeleton: gRPC transport only, one workload
// (customer.Lookup), closed- and open-loop modes. Later phases add the other
// transports (--transport nats|rabbitmq|…), payload-size sweeps, and a
// reproducible run.json; the driver (bench.go) is already transport-agnostic
// behind the benchClient interface so those add a client, not a new engine.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	benchmarkv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/benchmark/v1"
	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/grpcx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/natsx"
)

func main() {
	addr := flag.String("addr", "localhost:9430", "endpoint: a GatewayService host:port (grpc) or a NATS broker host:port (nats)")
	transport := flag.String("transport", "grpc", "transport: grpc | nats")
	mode := flag.String("mode", "closed", "load mode: closed (concurrency+requests) | open (rate+duration)")
	requests := flag.Int("requests", 10000, "closed mode: total request budget")
	concurrency := flag.Int("concurrency", 32, "closed mode: number of concurrent workers")
	rate := flag.Float64("rate", 0, "open mode: offered requests/sec")
	duration := flag.Duration("duration", 10*time.Second, "open mode: wall-clock budget")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call timeout")
	customerID := flag.String("customer-id", "11111111-1111-1111-1111-111111111111", "customer id (uuid) for the Lookup payload")
	region := flag.String("region", "us-west-2", "region for the Lookup payload")
	asJSON := flag.Bool("json", false, "emit the result as a JSON object instead of the text report")
	flag.Parse()

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

	// The driver programs to rpc.Client; both transports satisfy it, so the
	// load engine (bench.go) is unchanged across transports.
	var client rpc.Client
	var err error
	switch *transport {
	case "grpc":
		client, err = grpcx.Dial(*addr)
	case "nats":
		client, err = natsx.Dial(*addr)
	default:
		log.Fatalf("rpc-benchmark: unknown -transport %q (grpc|nats)", *transport)
	}
	if err != nil {
		log.Fatalf("rpc-benchmark: dial %s over %s: %v", *addr, *transport, err)
	}
	defer client.Close()

	payload := &benchmarkv1.CustomerLookupRequest{CustomerId: *customerID, Region: *region}
	newReq := func() *rpcv1.Request {
		req, err := rpc.NewRequest("customer", "Lookup", payload, *timeout)
		if err != nil {
			// payload and timeout are fixed and valid here, so this cannot fail;
			// panic rather than silently skew the loop.
			panic(err)
		}
		return req
	}

	res := runBench(context.Background(), client, newReq, cfg)
	if *asJSON {
		emitJSON(*transport, cfg, res)
	} else {
		printReport(*transport, cfg, res)
	}
	// A run that produced no successful samples is a failure of the run itself.
	if res.Summary.Count == 0 {
		os.Exit(1)
	}
}

func printReport(transport string, cfg benchConfig, res result) {
	s := res.Summary
	fmt.Printf("rpc-benchmark  transport=%s  mode=%s  elapsed=%s\n", transport, cfg.mode, res.Elapsed.Round(time.Millisecond))
	if cfg.mode == modeClosed {
		fmt.Printf("  budget=%d concurrency=%d\n", cfg.requests, cfg.concurrency)
	} else {
		fmt.Printf("  offered-rate=%.0f/s duration=%s\n", cfg.rate, cfg.duration)
	}
	total := int64(s.Count) + int64(s.Errors)
	fmt.Printf("  ok=%d errors=%d (timeouts=%d non-ok=%d) of %d  throughput=%.0f ok/s\n",
		s.Count, s.Errors, res.Timeouts, res.NonOK, total, s.ThroughputPerSec)
	fmt.Printf("  latency  min=%s p50=%s p90=%s p99=%s p99.9=%s max=%s mean=%s\n",
		d(s.Min), d(s.P50), d(s.P90), d(s.P99), d(s.P999), d(s.Max), d(s.Mean))
	if res.CorrectedOK {
		fmt.Printf("  co-corrected p99=%s\n", d(res.CorrectedP99))
	}
}

// benchJSON is the machine-readable shape (a precursor to the §35 run.json).
type benchJSON struct {
	Transport    string  `json:"transport"`
	Mode         string  `json:"mode"`
	ElapsedMS    float64 `json:"elapsed_ms"`
	OK           int     `json:"ok"`
	Errors       int     `json:"errors"`
	Timeouts     int64   `json:"timeouts"`
	NonOK        int64   `json:"non_ok"`
	ThroughputPS float64 `json:"throughput_ok_per_sec"`
	P50MS        float64 `json:"p50_ms"`
	P90MS        float64 `json:"p90_ms"`
	P99MS        float64 `json:"p99_ms"`
	P999MS       float64 `json:"p99_9_ms"`
	MaxMS        float64 `json:"max_ms"`
	CorrP99MS    float64 `json:"co_corrected_p99_ms,omitempty"`
}

func emitJSON(transport string, cfg benchConfig, res result) {
	s := res.Summary
	j := benchJSON{
		Transport:    transport,
		Mode:         cfg.mode,
		ElapsedMS:    ms(res.Elapsed),
		OK:           s.Count,
		Errors:       s.Errors,
		Timeouts:     res.Timeouts,
		NonOK:        res.NonOK,
		ThroughputPS: s.ThroughputPerSec,
		P50MS:        ms(s.P50),
		P90MS:        ms(s.P90),
		P99MS:        ms(s.P99),
		P999MS:       ms(s.P999),
		MaxMS:        ms(s.Max),
	}
	if res.CorrectedOK {
		j.CorrP99MS = ms(res.CorrectedP99)
	}
	b, _ := json.Marshal(j)
	fmt.Println(string(b))
}

func d(v time.Duration) string { return v.Round(time.Microsecond).String() }
func ms(v time.Duration) float64 {
	return float64(v.Microseconds()) / 1000.0
}
