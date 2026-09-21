// Command rpc-client is the caller side of the RPC lab (§17): it builds a routed
// rpc.v1 envelope for a demo method, sends it through a GatewayService endpoint
// (rpc-gateway, or rpc-service directly — both speak Call), and prints the
// decoded Response plus the §20 timeline. It is the reference for the uniform
// rpc.Call(ctx, request) API from the client's point of view.
//
// Only the "customer.Lookup" method is wired here (its payload is
// benchmark.v1.CustomerLookupRequest); it is enough to exercise unary Call and,
// with -idempotency-key + -count>1, the idempotency cache (§29): the first call
// executes, retries return the cached Response tagged idempotent-replay.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	benchmarkv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/benchmark/v1"
	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/grpcx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/mqttx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/natsx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/rabbitmqx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/tracing"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/valkeyx"
)

func main() {
	addr := flag.String("addr", "localhost:9430", "endpoint: a GatewayService host:port (grpc), a NATS broker host:port (nats/natsjs), or a RabbitMQ host:port / amqp URL (rabbitmq*)")
	transport := flag.String("transport", "grpc", "transport: grpc | nats | natsjs | rabbitmq | rabbitmq-direct | mqtt | valkey | valkey-stream")
	amqpUser := flag.String("user", "admin", "RabbitMQ username (rabbitmq* transports)")
	amqpPass := flag.String("pass", os.Getenv("RABBITMQ_PASS"), "RabbitMQ password (rabbitmq* transports; default $RABBITMQ_PASS)")
	mqttQoS := flag.Int("mqtt-qos", 1, "MQTT QoS (0, 1, or 2) for -transport mqtt; QoS 0/1/2 are distinct semantics")
	valkeyPass := flag.String("valkey-pass", os.Getenv("VALKEY_PASSWORD"), "Valkey primary password (valkey* transports; default $VALKEY_PASSWORD). For valkey*, -addr is the comma-separated Sentinel list")
	service := flag.String("service", "customer", "service to route to")
	method := flag.String("method", "Lookup", "method to invoke")
	customerID := flag.String("customer-id", "11111111-1111-1111-1111-111111111111", "customer id (uuid) for the Lookup payload")
	region := flag.String("region", "us-west-2", "region for the Lookup payload")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call timeout (envelope + ctx deadline)")
	idemKey := flag.String("idempotency-key", "", "idempotency key; with -count>1 proves retry dedup (§29)")
	count := flag.Int("count", 1, "number of identical calls to issue")
	stream := flag.Bool("stream", false, "issue the calls over one bidi CallStream instead of unary Call")
	asJSON := flag.Bool("json", false, "emit one JSON object per call instead of the text summary")
	traceMode := flag.String("trace", "off", "distributed tracing (§23): off | stdout (stdout writes each span as JSON to stderr)")
	flag.Parse()

	if *service != "customer" || *method != "Lookup" {
		log.Fatalf("rpc-client: only customer.Lookup is wired in this phase, got %s.%s", *service, *method)
	}
	if *count < 1 {
		log.Fatalf("rpc-client: -count must be >= 1, got %d", *count)
	}

	shutdownTracing, terr := tracing.NewProvider("rpc-client", *traceMode)
	if terr != nil {
		log.Fatalf("rpc-client: tracing: %v", terr)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	// client is the interface both transports satisfy; gclient is the concrete
	// gRPC client, needed only for -stream (NATS Core has no streaming, §11).
	var (
		client  rpc.Client
		gclient *grpcx.Client
		err     error
	)
	switch *transport {
	case "grpc":
		gclient, err = grpcx.Dial(*addr)
		client = gclient
	case "nats":
		if *stream {
			log.Fatalf("rpc-client: -stream requires -transport grpc (NATS Core req/reply has no streaming)")
		}
		client, err = natsx.Dial(*addr)
	case "natsjs":
		if *stream {
			log.Fatalf("rpc-client: -stream requires -transport grpc (JetStream RPC has no streaming)")
		}
		client, err = natsx.DialJetStream(*addr)
	case "rabbitmq":
		if *stream {
			log.Fatalf("rpc-client: -stream requires -transport grpc (RabbitMQ RPC has no streaming)")
		}
		client, err = rabbitmqx.Dial(rabbitmqx.URL(*addr, *amqpUser, *amqpPass), rabbitmqx.ModeReplyQueue)
	case "rabbitmq-direct":
		if *stream {
			log.Fatalf("rpc-client: -stream requires -transport grpc (RabbitMQ RPC has no streaming)")
		}
		client, err = rabbitmqx.Dial(rabbitmqx.URL(*addr, *amqpUser, *amqpPass), rabbitmqx.ModeDirect)
	case "mqtt":
		if *stream {
			log.Fatalf("rpc-client: -stream requires -transport grpc (MQTT req/reply has no streaming)")
		}
		if *mqttQoS < 0 || *mqttQoS > 2 {
			log.Fatalf("rpc-client: -mqtt-qos must be 0, 1, or 2, got %d", *mqttQoS)
		}
		client, err = mqttx.Dial(*addr, byte(*mqttQoS))
	case "valkey":
		if *stream {
			log.Fatalf("rpc-client: -stream requires -transport grpc (Valkey Pub/Sub req/reply has no streaming)")
		}
		client, err = valkeyx.Dial(*addr, *valkeyPass)
	case "valkey-stream":
		if *stream {
			log.Fatalf("rpc-client: -stream requires -transport grpc (Valkey Streams req/reply has no gRPC streaming)")
		}
		client, err = valkeyx.DialStream(*addr, *valkeyPass)
	default:
		log.Fatalf("rpc-client: unknown -transport %q (grpc|nats|natsjs|rabbitmq|rabbitmq-direct|mqtt|valkey|valkey-stream)", *transport)
	}
	if err != nil {
		log.Fatalf("rpc-client: dial %s over %s: %v", *addr, *transport, err)
	}
	// Trace the unary calls (§23): the client span injects trace context into the
	// envelope so the gateway and service continue the same trace. (-stream uses
	// the raw gRPC client below and is not traced.)
	client = rpc.NewTracingClient(client)
	defer client.Close()

	payload := &benchmarkv1.CustomerLookupRequest{
		CustomerId: *customerID,
		Region:     *region,
	}

	exit := 0
	if *stream {
		if err := callStream(gclient, *service, *method, payload, *idemKey, *timeout, *count, *asJSON); err != nil {
			exit = 1
		}
	} else {
		for i := 1; i <= *count; i++ {
			if err := call(client, *service, *method, payload, *idemKey, *timeout, *asJSON, i); err != nil {
				exit = 1
			}
		}
	}
	// os.Exit skips deferred shutdowns, so flush the tracer explicitly or the
	// client spans (batch-exported) are lost (§23).
	_ = shutdownTracing(context.Background())
	os.Exit(exit)
}

// callStream issues count identical requests over one bidi CallStream and reads
// the count responses back, reporting each. A non-OK response or any transport
// error yields a non-zero exit. This exercises the §10 streaming path end to end
// (client -> gateway -> service, all over CallStream).
func callStream(client *grpcx.Client, service, method string, payload *benchmarkv1.CustomerLookupRequest, idemKey string, timeout time.Duration, count int, asJSON bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	st, err := client.OpenStream(ctx)
	if err != nil {
		log.Printf("rpc-client: open stream: %v", err)
		return err
	}

	start := time.Now()
	for i := 1; i <= count; i++ {
		req, err := rpc.NewRequest(service, method, payload, timeout)
		if err != nil {
			log.Printf("rpc-client: build request #%d: %v", i, err)
			return err
		}
		req.IdempotencyKey = idemKey
		if err := st.Send(req); err != nil {
			log.Printf("rpc-client: stream send #%d: %v", i, err)
			return err
		}
	}
	if err := st.CloseSend(); err != nil {
		log.Printf("rpc-client: stream close-send: %v", err)
		return err
	}

	failed := false
	for i := 1; i <= count; i++ {
		resp, err := st.Recv()
		if err != nil {
			log.Printf("rpc-client: stream recv #%d: %v", i, err)
			return err
		}
		report := newReport(i, &rpcv1.Request{RequestId: resp.GetRequestId()}, resp, time.Since(start))
		if asJSON {
			b, _ := json.Marshal(report)
			fmt.Println(string(b))
		} else {
			report.print()
		}
		if resp.GetStatus() != rpcv1.Status_STATUS_OK {
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("one or more streamed responses were non-OK")
	}
	return nil
}

// call issues one Call and reports it. It returns an error when the call could
// not be made or came back non-OK, so a non-OK response drives a non-zero exit.
func call(client rpc.Client, service, method string, payload *benchmarkv1.CustomerLookupRequest, idemKey string, timeout time.Duration, asJSON bool, seq int) error {
	req, err := rpc.NewRequest(service, method, payload, timeout)
	if err != nil {
		log.Printf("rpc-client: build request: %v", err)
		return err
	}
	req.IdempotencyKey = idemKey

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	resp, err := client.Call(ctx, req)
	elapsed := time.Since(start)
	if err != nil {
		log.Printf("rpc-client: call #%d: transport error: %v", seq, err)
		return err
	}

	report := newReport(seq, req, resp, elapsed)
	if asJSON {
		b, _ := json.Marshal(report)
		fmt.Println(string(b))
	} else {
		report.print()
	}
	if resp.GetStatus() != rpcv1.Status_STATUS_OK {
		return fmt.Errorf("status %s: %s", resp.GetStatus(), resp.GetErrorMessage())
	}
	return nil
}

// report is the flattened view of one Response for -json output and printing.
type report struct {
	Seq          int           `json:"seq"`
	RequestID    string        `json:"request_id"`
	Status       string        `json:"status"`
	ErrorMessage string        `json:"error_message,omitempty"`
	Replay       bool          `json:"idempotent_replay"`
	RoundTripMS  float64       `json:"round_trip_ms"`
	Customer     *customerView `json:"customer,omitempty"`
}

type customerView struct {
	CustomerID string   `json:"customer_id"`
	Name       string   `json:"name"`
	Email      string   `json:"email"`
	AccountIDs []string `json:"account_ids,omitempty"`
}

func newReport(seq int, req *rpcv1.Request, resp *rpcv1.Response, elapsed time.Duration) *report {
	r := &report{
		Seq:          seq,
		RequestID:    req.GetRequestId(),
		Status:       resp.GetStatus().String(),
		ErrorMessage: resp.GetErrorMessage(),
		Replay:       resp.GetMetadata()["idempotent-replay"] == "true",
		RoundTripMS:  float64(elapsed.Microseconds()) / 1000.0,
	}
	if resp.GetStatus() == rpcv1.Status_STATUS_OK && resp.GetPayload() != nil {
		out := &benchmarkv1.CustomerLookupResponse{}
		if err := rpc.UnpackInto(resp.GetPayload(), out); err == nil {
			r.Customer = &customerView{
				CustomerID: out.GetCustomerId(),
				Name:       out.GetName(),
				Email:      out.GetEmail(),
				AccountIDs: out.GetAccountIds(),
			}
		} else {
			// Fall back to a raw protojson dump so an unexpected payload type is
			// still visible rather than silently dropped.
			r.ErrorMessage = fmt.Sprintf("decode payload: %v (%s)", err, protojson.Format(resp))
		}
	}
	return r
}

func (r *report) print() {
	replay := ""
	if r.Replay {
		replay = " [idempotent-replay]"
	}
	fmt.Printf("#%d %s%s  rtt=%.3fms  request_id=%s\n", r.Seq, r.Status, replay, r.RoundTripMS, r.RequestID)
	if r.Customer != nil {
		fmt.Printf("    customer_id=%s name=%q email=%s accounts=%v\n",
			r.Customer.CustomerID, r.Customer.Name, r.Customer.Email, r.Customer.AccountIDs)
	}
	if r.ErrorMessage != "" {
		fmt.Printf("    error: %s\n", r.ErrorMessage)
	}
}
