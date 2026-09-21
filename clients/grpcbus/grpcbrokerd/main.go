// Command grpcbrokerd is the central gRPC message bus: a BrokerService server
// that fans each published Message out to every live subscriber of its topic.
// Publish is a unary RPC; Subscribe is a server-stream that stays open for the
// subscriber's session and yields each fan-out Message (design: gRPC bus PR 1).
//
// It is the gRPC-native sibling of the NATS/RabbitMQ/MQTT/Valkey buses: deployed
// centrally on the cluster, with host-run grpcbuscli pub/sub clients pointing at
// its NodePort. Delivery is ephemeral fan-out — no history, replay, or acks.
//
//	grpcbrokerd -grpc-addr :9450 -metrics-addr :9451
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	busv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/bus/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/grpcbus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
)

func main() {
	addr := flag.String("grpc-addr", ":9450", "BrokerService gRPC listen address")
	metricsAddr := flag.String("metrics-addr", "", "serve OTel /metrics on this host:port; empty disables metrics")
	buffer := flag.Int("buffer", grpcbus.DefaultSubBuffer, "per-subscriber message buffer depth (full buffer drops, never blocks the publisher)")
	validate := flag.Bool("validate", true, "run protovalidate on published messages")
	flag.Parse()

	// Optional /metrics endpoint on its own OTel meter (grpcbus_* instruments).
	var bm *grpcbus.Metrics
	if *metricsAddr != "" {
		mp, _, err := metrics.NewProvider(*metricsAddr)
		if err != nil {
			log.Fatalf("grpcbrokerd: metrics: %v", err)
		}
		if bm, err = grpcbus.NewMetrics(mp.Meter("grpcbus")); err != nil {
			log.Fatalf("grpcbrokerd: metrics instruments: %v", err)
		}
		log.Printf("grpcbrokerd: serving /metrics on %s", *metricsAddr)
	}

	broker := grpcbus.NewBroker(*buffer, bm)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("grpcbrokerd: listen %s: %v", *addr, err)
	}
	srv := grpc.NewServer()
	busv1.RegisterBrokerServiceServer(srv, &server{broker: broker, validate: *validate})
	reflection.Register(srv)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		srv.GracefulStop()
	}()

	log.Printf("grpcbrokerd: serving BrokerService on %s (buffer=%d validate=%t)", lis.Addr(), *buffer, *validate)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("grpcbrokerd: serve: %v", err)
	}
}

// server adapts grpcbus.Broker to the generated BrokerServiceServer.
type server struct {
	busv1.UnimplementedBrokerServiceServer
	broker   *grpcbus.Broker
	validate bool // run protovalidate on requests
}

// Publish validates (optionally) and fans the message out, reporting the live
// subscriber count it reached.
func (s *server) Publish(ctx context.Context, req *busv1.PublishRequest) (*busv1.PublishResponse, error) {
	msg := req.GetMessage()
	if msg == nil {
		return nil, status.Error(codes.InvalidArgument, "message is required")
	}
	if s.validate {
		if err := protovalidate.Validate(req); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid message: %v", err)
		}
	}
	delivered := s.broker.Publish(msg)
	return &busv1.PublishResponse{Delivered: uint64(delivered)}, nil
}

// Subscribe registers the caller on its topic and streams each fan-out message
// until the client disconnects or the server shuts down.
func (s *server) Subscribe(req *busv1.SubscribeRequest, stream busv1.BrokerService_SubscribeServer) error {
	if s.validate {
		if err := protovalidate.Validate(req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid subscribe: %v", err)
		}
	} else if req.GetTopic() == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}

	ch, cancel := s.broker.Subscribe(req.GetTopic(), req.GetSubscriberId())
	defer cancel()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			// Client hung up or deadline hit — a normal end of subscription.
			if err := ctx.Err(); errors.Is(err, context.Canceled) {
				return nil
			}
			return status.FromContextError(ctx.Err()).Err()
		case msg := <-ch:
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}
