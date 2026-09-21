// Command rpc-service is the terminal backend of the RPC lab (§17): a
// GatewayService gRPC server whose handlers implement the actual business
// methods (customer.Lookup, …). It decodes the routed rpc.v1 envelope, validates
// the payload, dispatches by service.method, and applies an idempotency cache so
// a retried logical operation is not executed twice (§29).
//
// A gateway (rpc-gateway) forwards routed requests here; a client may also point
// at it directly. Everything speaks GatewayService.Call, so every hop is uniform.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"

	benchmarkv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/benchmark/v1"
	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/grpcx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/mqttx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/natsx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/rabbitmqx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/valkeyx"
)

func main() {
	addr := flag.String("grpc-addr", ":9440", "GatewayService gRPC listen address")
	natsAddr := flag.String("nats", "", "also serve GatewayService over NATS Core req/reply at this broker (host:port or nats://…); empty disables")
	natsJSAddr := flag.String("nats-jetstream", "", "also serve GatewayService over durable NATS JetStream at this broker (host:port or nats://…); empty disables")
	amqpURL := flag.String("amqp", "", "also serve GatewayService over RabbitMQ req/reply at this broker (full amqp://user:pass@host:port/ URL); empty disables")
	mqttAddr := flag.String("mqtt", "", "also serve GatewayService over MQTT req/reply at this broker (host:port or tcp://…); empty disables")
	mqttQoS := flag.Int("mqtt-qos", 1, "MQTT QoS for the -mqtt responder (0, 1, or 2)")
	valkeyAddr := flag.String("valkey", "", "also serve GatewayService over Valkey Pub/Sub req/reply at these Sentinels (comma-separated host:port list); empty disables")
	valkeyStreamAddr := flag.String("valkey-stream", "", "also serve GatewayService over durable Valkey Streams req/reply at these Sentinels (comma-separated host:port list); empty disables")
	valkeyPass := flag.String("valkey-pass", os.Getenv("VALKEY_PASSWORD"), "Valkey primary password for the -valkey/-valkey-stream responders (default $VALKEY_PASSWORD)")
	idemTTL := flag.Duration("idempotency-ttl", 5*time.Minute, "how long a completed operation is remembered for retry dedup (0 disables)")
	validate := flag.Bool("validate", true, "run protovalidate on decoded request payloads")
	flag.Parse()

	mux := rpc.NewMux(*validate)
	registerCustomer(mux)
	registerEcho(mux)

	cache := rpc.NewIdempotencyCache(*idemTTL)
	handler := idempotent(cache, mux)

	// Optional NATS responder: the same handler serves the §11 NATS Core path,
	// so a request arriving over gRPC or NATS is dispatched and deduped identically.
	if *natsAddr != "" {
		resp, err := natsx.Serve(context.Background(), *natsAddr, handler)
		if err != nil {
			log.Fatalf("rpc-service: serve NATS at %s: %v", *natsAddr, err)
		}
		defer resp.Close()
		log.Printf("rpc-service: also serving GatewayService over NATS at %s (subject %s)", *natsAddr, natsx.SubjectWildcard)
	}

	// Optional durable JetStream responder: same handler, at-least-once delivery
	// with redelivery deduped by the idempotency cache (§29, §30).
	if *natsJSAddr != "" {
		resp, err := natsx.ServeJetStream(context.Background(), *natsJSAddr, handler)
		if err != nil {
			log.Fatalf("rpc-service: serve NATS JetStream at %s: %v", *natsJSAddr, err)
		}
		defer resp.Close()
		log.Printf("rpc-service: also serving GatewayService over durable JetStream at %s (subject %s)", *natsJSAddr, natsx.JSSubjectWildcard)
	}

	// Optional RabbitMQ responder: same handler over AMQP req/reply (§12),
	// consuming the shared request queue and replying on each request's reply-to.
	if *amqpURL != "" {
		resp, err := rabbitmqx.Serve(context.Background(), *amqpURL, handler)
		if err != nil {
			log.Fatalf("rpc-service: serve RabbitMQ at %s: %v", *amqpURL, err)
		}
		defer resp.Close()
		log.Printf("rpc-service: also serving GatewayService over RabbitMQ (queue %s)", rabbitmqx.RequestQueue)
	}

	// Optional MQTT responder: same handler over MQTT req/reply (§13), at the
	// chosen QoS. QoS>=1 may redeliver, deduped by the idempotency cache (§29).
	if *mqttAddr != "" {
		if *mqttQoS < 0 || *mqttQoS > 2 {
			log.Fatalf("rpc-service: -mqtt-qos must be 0, 1, or 2, got %d", *mqttQoS)
		}
		resp, err := mqttx.Serve(context.Background(), *mqttAddr, byte(*mqttQoS), handler)
		if err != nil {
			log.Fatalf("rpc-service: serve MQTT at %s: %v", *mqttAddr, err)
		}
		defer resp.Close()
		log.Printf("rpc-service: also serving GatewayService over MQTT at %s (qos=%d, request topic %s)", *mqttAddr, *mqttQoS, "rpc/request/+/+")
	}

	// Optional Valkey Pub/Sub responder: same handler over the ephemeral path.
	if *valkeyAddr != "" {
		resp, err := valkeyx.Serve(context.Background(), *valkeyAddr, *valkeyPass, handler)
		if err != nil {
			log.Fatalf("rpc-service: serve Valkey Pub/Sub at %s: %v", *valkeyAddr, err)
		}
		defer resp.Close()
		log.Printf("rpc-service: also serving GatewayService over Valkey Pub/Sub at %s", *valkeyAddr)
	}

	// Optional durable Valkey Streams responder: at-least-once delivery with
	// redelivery deduped by the idempotency cache (§29).
	if *valkeyStreamAddr != "" {
		resp, err := valkeyx.ServeStream(context.Background(), *valkeyStreamAddr, *valkeyPass, handler)
		if err != nil {
			log.Fatalf("rpc-service: serve Valkey Streams at %s: %v", *valkeyStreamAddr, err)
		}
		defer resp.Close()
		log.Printf("rpc-service: also serving GatewayService over durable Valkey Streams at %s", *valkeyStreamAddr)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("rpc-service: listen %s: %v", *addr, err)
	}
	srv := grpc.NewServer()
	grpcx.RegisterServer(srv, handler)
	reflection.Register(srv)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		srv.GracefulStop()
	}()

	log.Printf("rpc-service: serving GatewayService on %s (idempotency-ttl=%s validate=%t)", lis.Addr(), *idemTTL, *validate)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("rpc-service: serve: %v", err)
	}
}

// idempotent wraps h so a request carrying an idempotency_key that was seen
// before returns the cached Response (tagged replay) instead of re-invoking h.
// Only OK responses are cached.
func idempotent(cache *rpc.IdempotencyCache, mux *rpc.Mux) rpc.Handler {
	return rpc.HandlerFunc(func(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
		key := req.GetIdempotencyKey()
		if cached, ok := cache.Get(key); ok {
			replay := proto.Clone(cached).(*rpcv1.Response)
			// Stamp this attempt's request_id onto the replay: the cached response
			// carries the original attempt's id, but a retry (§29) has a new
			// request_id, and out-of-band transports (JetStream, RabbitMQ, …)
			// correlate the reply by request_id — so the replay must answer for
			// the id that is actually waiting.
			replay.RequestId = req.GetRequestId()
			if replay.Metadata == nil {
				replay.Metadata = map[string]string{}
			}
			replay.Metadata["idempotent-replay"] = "true"
			return replay, nil
		}
		resp := mux.Dispatch(ctx, req)
		if resp.GetStatus() == rpcv1.Status_STATUS_OK {
			cache.Put(key, resp)
		}
		return resp, nil
	})
}

// registerEcho wires the "echo" service used by the payload-size sweep (§26):
// Echo returns the request's AllTypes unchanged, so the response envelope grows
// in step with the request. AllTypes.blob (max 1 MiB, protovalidate-bounded) is
// the size knob, letting rpc-benchmark sweep 100 B … 1 MiB round-trips over any
// transport without a bespoke per-size message.
func registerEcho(mux *rpc.Mux) {
	mux.Handle("echo", "Echo",
		func() proto.Message { return &benchmarkv1.AllTypes{} },
		func(ctx context.Context, in proto.Message) (proto.Message, error) {
			return in, nil
		},
	)
}

// registerCustomer wires the demo "customer" service. Lookup synthesizes a
// deterministic profile from the id + region so responses are reproducible.
func registerCustomer(mux *rpc.Mux) {
	mux.Handle("customer", "Lookup",
		func() proto.Message { return &benchmarkv1.CustomerLookupRequest{} },
		func(ctx context.Context, in proto.Message) (proto.Message, error) {
			r := in.(*benchmarkv1.CustomerLookupRequest)
			short := r.GetCustomerId()
			if i := strings.IndexByte(short, '-'); i > 0 {
				short = short[:i]
			}
			return &benchmarkv1.CustomerLookupResponse{
				CustomerId: r.GetCustomerId(),
				Name:       fmt.Sprintf("Customer %s (%s)", short, r.GetRegion()),
				Email:      fmt.Sprintf("%s@%s.example.com", short, r.GetRegion()),
				AccountIds: []string{r.GetCustomerId()},
			}, nil
		},
	)
}
