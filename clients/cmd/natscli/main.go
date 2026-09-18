// natscli — a minimal NATS pub/sub client for the message-bus-examples cluster.
//
//	natscli pub  -addr 127.0.0.1:30422 -subject demo -msg "hello"
//	natscli sub  -addr 127.0.0.1:30422 -subject demo
//
// Connects to the NATS NodePort exposed by the in-cluster JetStream
// cluster. Reconnects automatically so it keeps running across a node
// failover (see `nix run .#k8s-chaos-failover`).
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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
		if err := nc.Publish(f.Subject, []byte(f.Msg)); err != nil {
			log.Fatalf("publish: %v", err)
		}
		if err := nc.Flush(); err != nil {
			log.Fatalf("flush: %v", err)
		}
		fmt.Printf("published to %q: %s\n", f.Subject, f.Msg)
	case "sub":
		if _, err := nc.Subscribe(f.Subject, func(m *nats.Msg) {
			fmt.Printf("%s  [%s] %s\n", time.Now().Format(time.RFC3339), m.Subject, string(m.Data))
		}); err != nil {
			log.Fatalf("subscribe: %v", err)
		}
		fmt.Printf("subscribed to %q on %s; waiting for messages (Ctrl-C to quit)\n", f.Subject, f.Addr)
		waitForSignal()
	}
}

func waitForSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
