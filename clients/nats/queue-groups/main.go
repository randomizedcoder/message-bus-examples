// nats-queue-groups — a self-contained demo of NATS queue groups
// (https://docs.nats.io/concepts/queue-groups).
//
// In plain pub/sub every subscriber receives every message. When
// subscribers share a queue-group name, NATS delivers each message to only
// ONE (randomly chosen) member of the group — automatic load balancing
// across competing consumers.
//
// This program starts N workers that all QueueSubscribe to `tasks` under the
// queue group "workers", publishes M messages, and tallies how many each
// worker handled. The tallies always sum to M, and the load spreads across
// more than one worker.
//
// Run it with:
//
//	nix run .#nats-queue-groups
//	nix run .#nats-queue-groups -- -count 12 -workers 4 -addr 10.33.33.10:30422
//
// Self-verifying: exits non-zero unless every message was delivered exactly
// once and more than one worker participated.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	subject = "tasks"
	queue   = "workers"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:30422", "NATS host:port (NodePort)")
	count := flag.Int("count", 9, "number of messages to publish")
	workers := flag.Int("workers", 3, "number of queue-group members")
	flag.Parse()

	if *workers < 1 {
		log.Fatalf("-workers must be >= 1")
	}

	nc := connect(*addr)
	defer nc.Drain()

	var out sync.Mutex
	logln := func(format string, a ...any) {
		out.Lock()
		fmt.Printf(format+"\n", a...)
		out.Unlock()
	}

	// One atomic tally per worker, plus a grand total across the group.
	tallies := make([]atomic.Int64, *workers)
	var total atomic.Int64
	for w := range *workers {
		if _, err := nc.QueueSubscribe(subject, queue, func(m *nats.Msg) {
			tallies[w].Add(1)
			total.Add(1)
			logln("  [worker %d] handled %s", w, string(m.Data))
		}); err != nil {
			log.Fatalf("queue subscribe (worker %d): %v", w, err)
		}
	}
	// All members must be registered before we publish so the server can
	// spread messages across the full group.
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush subscriptions: %v", err)
	}
	log.Printf("connected to %s; %d workers in queue group %q, publishing %d messages", *addr, *workers, queue, *count)

	for i := 1; i <= *count; i++ {
		body := fmt.Sprintf("task-%d", i)
		logln("[pub] -> %s : %s", subject, body)
		if err := nc.Publish(subject, []byte(body)); err != nil {
			log.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush publishes: %v", err)
	}

	// Wait until every message has been handled (or give up after a bit).
	deadline := time.Now().Add(5 * time.Second)
	for total.Load() < int64(*count) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Println("\nsummary (messages handled per worker):")
	var sum int64
	distinct := 0
	for w := range *workers {
		n := tallies[w].Load()
		sum += n
		if n > 0 {
			distinct++
		}
		fmt.Printf("  worker %d: %d\n", w, n)
	}
	fmt.Printf("total handled: %d/%d across %d/%d workers\n", sum, *count, distinct, *workers)

	if sum != int64(*count) {
		fmt.Fprintln(os.Stderr, "FAIL: messages handled did not equal messages published (each should be delivered exactly once)")
		os.Exit(1)
	}
	if *workers > 1 && *count >= *workers && distinct < 2 {
		fmt.Fprintln(os.Stderr, "FAIL: expected the load to spread across more than one worker")
		os.Exit(1)
	}
	fmt.Println("PASS: each message was delivered to exactly one worker, load balanced across the group")
}

// connect dials NATS with the same resilience options the other clients use.
func connect(addr string) *nats.Conn {
	nc, err := nats.Connect("nats://"+addr,
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
	return nc
}
