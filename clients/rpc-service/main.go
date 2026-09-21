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
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/natsx"
)

func main() {
	addr := flag.String("grpc-addr", ":9440", "GatewayService gRPC listen address")
	natsAddr := flag.String("nats", "", "also serve GatewayService over NATS Core req/reply at this broker (host:port or nats://…); empty disables")
	idemTTL := flag.Duration("idempotency-ttl", 5*time.Minute, "how long a completed operation is remembered for retry dedup (0 disables)")
	validate := flag.Bool("validate", true, "run protovalidate on decoded request payloads")
	flag.Parse()

	mux := rpc.NewMux(*validate)
	registerCustomer(mux)

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
