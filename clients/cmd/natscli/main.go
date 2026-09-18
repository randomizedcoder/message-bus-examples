// natscli — a minimal NATS pub/sub client for the message-bus-examples cluster.
//
//	natscli pub -addr 127.0.0.1:30422 -subject demo -msg "hello"
//	natscli pub -subject demo -count 100 -rate 10/s
//	natscli sub -subject demo -count 5 -timeout 10s -json
//
// Connects to the NATS NodePort exposed by the in-cluster JetStream
// cluster. Reconnects automatically so it keeps running across a node
// failover (see `nix run .#k8s-chaos-failover`).
package main

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/cli"
)

func main() {
	f := cli.Parse("natscli", "127.0.0.1:30422", "")

	nc, err := nats.Connect("nats://"+f.Addr,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Printf("reconnected to %s", c.ConnectedUrl())
		}),
	)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Drain()

	switch f.Cmd {
	case "pub":
		if err := f.PubLoop(func(body string) error {
			if err := nc.Publish(f.Subject, []byte(body)); err != nil {
				return err
			}
			return nc.Flush()
		}); err != nil {
			log.Fatalf("%v", err)
		}
	case "sub":
		lim := f.NewLimiter()
		if _, err := nc.Subscribe(f.Subject, func(m *nats.Msg) {
			f.Emit(m.Subject, string(m.Data))
			lim.Hit()
		}); err != nil {
			log.Fatalf("subscribe: %v", err)
		}
		log.Printf("subscribed to %q on %s; waiting for messages (Ctrl-C to quit)", f.Subject, f.Addr)
		select {
		case <-f.Stop():
		case <-lim.Done():
		}
	}
}
