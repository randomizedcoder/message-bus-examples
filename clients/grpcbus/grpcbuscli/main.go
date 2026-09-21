// grpcbuscli — a minimal pub/sub client for the gRPC-native message bus
// (grpcbrokerd), mirroring the natscli/rabbitmqcli/mqttcli/valkeycli shape so the
// gRPC bus is exercised exactly like the other buses.
//
//	grpcbuscli pub -addr 127.0.0.1:9450 -subject demo -msg "hello"
//	grpcbuscli pub -subject demo -count 100 -rate 10/s
//	grpcbuscli sub -subject demo -count 5 -timeout 10s -json
//
// pub calls BrokerService.Publish once per message; sub opens a server-stream
// Subscribe and prints each message. Delivery is ephemeral fan-out: sub only sees
// messages published while it is connected.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	busv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/bus/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/cli"
)

func main() {
	f := cli.Parse("grpcbuscli", "127.0.0.1:9450", "")
	id := fmt.Sprintf("grpcbuscli-%d", os.Getpid())

	cc, err := grpc.NewClient(f.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", f.Addr, err)
	}
	defer cc.Close()
	client := busv1.NewBrokerServiceClient(cc)

	switch f.Cmd {
	case "pub":
		runPub(f, client, id)
	case "sub":
		runSub(f, client, id)
	}
}

// runPub sends one Publish per message. cli.PubLoop appends a 1-based sequence to
// each body when -count > 1; we also stamp Message.seq so a subscriber can detect
// the ephemeral bus's drops.
func runPub(f *cli.Flags, client busv1.BrokerServiceClient, id string) {
	var seq uint64
	if err := f.PubLoop(func(body string) error {
		n := atomic.AddUint64(&seq, 1)
		resp, err := client.Publish(context.Background(), &busv1.PublishRequest{
			Message: &busv1.Message{
				Topic:       f.Subject,
				Payload:     []byte(body),
				ContentType: "text/plain",
				Seq:         n,
				PublisherId: id,
				PublishedAt: timestamppb.Now(),
			},
		})
		if err != nil {
			return err
		}
		if resp.GetDelivered() == 0 {
			log.Printf("published seq %d to %q: 0 subscribers (dropped)", n, f.Subject)
		}
		return nil
	}); err != nil {
		log.Fatalf("%v", err)
	}
}

// runSub opens a Subscribe server-stream and prints each message until -count is
// reached, -timeout elapses, or Ctrl-C. The blocking Recv runs in a goroutine;
// the main goroutine cancels the stream context to unblock it on exit.
func runSub(f *cli.Flags, client busv1.BrokerServiceClient, id string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Subscribe(ctx, &busv1.SubscribeRequest{Topic: f.Subject, SubscriberId: id})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	log.Printf("subscribed to %q on %s; waiting for messages (Ctrl-C to quit)", f.Subject, f.Addr)

	lim := f.NewLimiter()
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				// Canceled context (our own exit) or server shutdown ends the stream.
				if ctx.Err() == nil {
					log.Printf("recv: %v", err)
				}
				return
			}
			f.Emit(msg.GetTopic(), string(msg.GetPayload()))
			lim.Hit()
		}
	}()

	select {
	case <-f.Stop():
	case <-lim.Done():
	}
	cancel()
	// Give the in-flight Send/Emit a moment to flush before the process exits.
	time.Sleep(50 * time.Millisecond)
}
