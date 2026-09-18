// nats-subjects — a self-contained demo of NATS subjects and subject
// hierarchies (https://docs.nats.io/concepts/subjects).
//
// It starts three subscribers on one connection, each listening at a
// different level of the same subject hierarchy, then publishes a fixed
// list of `orders.*.*` subjects and shows which subscribers each message
// reaches:
//
//	exact  orders.retail.placed   — one specific subject
//	wild   orders.retail.*        — one token wildcard
//	full   orders.>               — the whole subtree
//
// Run it with:
//
//	nix run .#nats-subjects
//	nix run .#nats-subjects -- -addr 10.33.33.10:30422
//
// The program is self-verifying: it asserts each subscriber received the
// expected number of messages and exits non-zero on any mismatch.
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

// sub is one subscriber in the demo: a human label, the subject pattern it
// listens on, how many of the published messages it is expected to match,
// and a live counter of how many it actually received.
type sub struct {
	label   string
	pattern string
	want    int64
	got     atomic.Int64
}

func main() {
	addr := flag.String("addr", "127.0.0.1:30422", "NATS host:port (NodePort)")
	settle := flag.Duration("settle", 100*time.Millisecond, "pause after each publish so async deliveries print in order")
	flag.Parse()

	// The subjects the publisher will send, in order.
	subjects := []string{
		"orders.retail.placed",
		"orders.retail.shipped",
		"orders.wholesale.placed",
		"orders.wholesale.shipped",
	}

	// The three subscribers, one per hierarchy level. `want` is computed
	// from `subjects` above: exact matches one, the retail wildcard matches
	// the two retail subjects, and the full wildcard matches everything.
	subs := []*sub{
		{label: "exact", pattern: "orders.retail.placed", want: 1},
		{label: "wild ", pattern: "orders.retail.*", want: 2},
		{label: "full ", pattern: "orders.>", want: int64(len(subjects))},
	}

	nc := connect(*addr)
	defer nc.Drain()

	// A single mutex serialises stdout so the per-message grouping stays
	// readable even though deliveries arrive on background goroutines.
	var out sync.Mutex
	logln := func(format string, a ...any) {
		out.Lock()
		fmt.Printf(format+"\n", a...)
		out.Unlock()
	}

	var total atomic.Int64
	for _, s := range subs {
		if _, err := nc.Subscribe(s.pattern, func(m *nats.Msg) {
			s.got.Add(1)
			total.Add(1)
			logln("  [sub %s %-22s] GOT %s", s.label, s.pattern, m.Subject)
		}); err != nil {
			log.Fatalf("subscribe %q: %v", s.pattern, err)
		}
	}
	// Make sure all three subscriptions are registered on the server before
	// we publish, so no early message is missed.
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush subscriptions: %v", err)
	}

	var wantTotal int64
	for _, s := range subs {
		wantTotal += s.want
	}

	log.Printf("connected to %s; 3 subscribers listening, publishing %d subjects", *addr, len(subjects))
	for _, subj := range subjects {
		logln("[pub] -> %s", subj)
		if err := nc.Publish(subj, []byte(subj)); err != nil {
			log.Fatalf("publish %q: %v", subj, err)
		}
		if err := nc.Flush(); err != nil {
			log.Fatalf("flush publish: %v", err)
		}
		// Let the async handlers print under the message they belong to.
		time.Sleep(*settle)
	}

	// Final settle so any straggler delivery is counted before we assert.
	deadline := time.Now().Add(2 * time.Second)
	for total.Load() < wantTotal && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Println("\nsummary (each subscriber's match count):")
	ok := true
	for _, s := range subs {
		got := s.got.Load()
		status := "ok"
		if got != s.want {
			status = "MISMATCH"
			ok = false
		}
		fmt.Printf("  %-5s %-22s got=%d want=%d  %s\n", s.label, s.pattern, got, s.want, status)
	}
	if !ok {
		fmt.Fprintln(os.Stderr, "FAIL: subscriber match counts did not equal expectations")
		os.Exit(1)
	}
	fmt.Println("PASS: exact matched 1, retail wildcard matched 2, full wildcard matched all")
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
