// nats-leaf — a self-contained demo of a NATS leaf-node topology
// (https://docs.nats.io/concepts/topologies).
//
// The cluster in this repo runs as a hub of three servers on the
// control-plane nodes, plus one leaf node on the worker that dials the hub
// and bridges subject interest between them. A client attached to the leaf
// sees a normal NATS server; messages cross the leaf link transparently.
//
// This demo opens two connections — one to the hub NodePort and one to the
// leaf NodePort — and proves interest is bridged in both directions:
//
//	hub -> leaf : subscribe on the leaf, publish on the hub, expect delivery
//	leaf -> hub : subscribe on the hub, publish on the leaf, expect delivery
//
// Run it with:
//
//	nix run .#nats-leaf
//	nix run .#nats-leaf -- -hub-addr 10.33.33.10:30422 -leaf-addr 10.33.33.13:30423
//
// Self-verifying: exits non-zero unless a message crosses the leaf link in
// both directions.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	hubAddr := flag.String("hub-addr", "127.0.0.1:30422", "hub (cluster) NATS host:port (NodePort)")
	leafAddr := flag.String("leaf-addr", "127.0.0.1:30423", "leaf NATS host:port (NodePort)")
	timeout := flag.Duration("timeout", 5*time.Second, "how long to wait for a message to cross the leaf link")
	settle := flag.Duration("settle", 300*time.Millisecond, "pause after subscribing so subject interest propagates across the leaf link")
	flag.Parse()

	hub := connect("hub", *hubAddr)
	defer hub.Drain()
	leaf := connect("leaf", *leafAddr)
	defer leaf.Drain()
	log.Printf("connected: hub=%s leaf=%s", *hubAddr, *leafAddr)

	ok := true
	// hub -> leaf: a subscriber on the leaf must receive a message published
	// on the hub, which means interest crossed the leaf link into the cluster.
	if !crosses(hub, leaf, "hub", "leaf", "leaf.demo.hub2leaf", *settle, *timeout) {
		ok = false
	}
	// leaf -> hub: and the reverse.
	if !crosses(leaf, hub, "leaf", "hub", "leaf.demo.leaf2hub", *settle, *timeout) {
		ok = false
	}

	fmt.Println()
	if !ok {
		fmt.Fprintln(os.Stderr, "FAIL: a message did not cross the leaf link")
		os.Exit(1)
	}
	fmt.Println("PASS: subject interest bridged both ways across the leaf link")
}

// crosses subscribes on subConn, publishes the same subject on pubConn, and
// reports whether the message arrived within timeout. subLabel/pubLabel are
// only for the printed trace.
func crosses(pubConn, subConn *nats.Conn, pubLabel, subLabel, subject string, settle, timeout time.Duration) bool {
	got := make(chan string, 1)
	sub, err := subConn.Subscribe(subject, func(m *nats.Msg) {
		select {
		case got <- string(m.Data):
		default:
		}
	})
	if err != nil {
		log.Fatalf("subscribe on %s: %v", subLabel, err)
	}
	defer sub.Unsubscribe()
	if err := subConn.Flush(); err != nil {
		log.Fatalf("flush %s subscription: %v", subLabel, err)
	}
	// Let interest propagate across the leaf link before publishing.
	time.Sleep(settle)

	body := fmt.Sprintf("crossing %s->%s", pubLabel, subLabel)
	fmt.Printf("\n[%s pub] -> %s : %q\n", pubLabel, subject, body)
	if err := pubConn.Publish(subject, []byte(body)); err != nil {
		log.Fatalf("publish on %s: %v", pubLabel, err)
	}
	if err := pubConn.Flush(); err != nil {
		log.Fatalf("flush %s publish: %v", pubLabel, err)
	}

	select {
	case data := <-got:
		fmt.Printf("[%s sub] <- %s : %q  (crossed the leaf link)\n", subLabel, subject, data)
		return true
	case <-time.After(timeout):
		fmt.Printf("[%s sub] TIMEOUT after %s — nothing arrived\n", subLabel, timeout)
		return false
	}
}

// connect dials NATS with the same resilience options the other clients use.
func connect(label, addr string) *nats.Conn {
	nc, err := nats.Connect("nats://"+addr,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("[%s] disconnected: %v", label, err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Printf("[%s] reconnected to %s", label, c.ConnectedUrl())
		}),
	)
	if err != nil {
		log.Fatalf("connect %s (%s): %v", label, addr, err)
	}
	return nc
}
