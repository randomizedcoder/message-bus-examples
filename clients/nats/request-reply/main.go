// nats-request-reply — a self-contained demo of the NATS request-reply
// pattern (https://docs.nats.io/concepts/request-reply).
//
// Request-reply layers synchronous, RPC-style calls on top of NATS's
// asynchronous pub/sub. This program runs both halves in one process:
//
//	responder — subscribes to `demo.request` and Respond()s to each message
//	requester — issues N Request() calls and waits for each reply
//
// Under the hood each Request() creates a unique `_INBOX.<nuid>` reply
// subject, subscribes to it, publishes the request carrying that reply
// subject, and blocks until the responder publishes back to it.
//
// Run it with:
//
//	nix run .#nats-request-reply
//	nix run .#nats-request-reply -- -count 5 -addr 10.33.33.10:30422
//
// Self-verifying: it exits non-zero unless every request received a reply.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const subject = "demo.request"

func main() {
	addr := flag.String("addr", "127.0.0.1:30422", "NATS host:port (NodePort)")
	count := flag.Int("count", 3, "number of requests to send")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request reply timeout")
	flag.Parse()

	nc := connect(*addr)
	defer nc.Drain()

	// Responder: a service that answers requests on `subject`. It replies to
	// the auto-generated inbox carried on each request (m.Reply).
	if _, err := nc.Subscribe(subject, func(m *nats.Msg) {
		reply := fmt.Sprintf("re: %q at %s", string(m.Data), time.Now().Format(time.RFC3339Nano))
		if err := m.Respond([]byte(reply)); err != nil {
			log.Printf("respond: %v", err)
		}
	}); err != nil {
		log.Fatalf("subscribe (responder): %v", err)
	}
	// Ensure the responder is live before the requester starts.
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush responder: %v", err)
	}
	log.Printf("connected to %s; responder listening on %q, sending %d request(s)", *addr, subject, *count)

	// Requester: N synchronous round trips.
	replies := 0
	for i := 1; i <= *count; i++ {
		req := fmt.Sprintf("ping %d", i)
		fmt.Printf("[req] -> %s : %s\n", subject, req)
		msg, err := nc.Request(subject, []byte(req), *timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[req] request %d failed: %v\n", i, err)
			continue
		}
		replies++
		fmt.Printf("[rep] <- %s : %s\n", msg.Subject, string(msg.Data))
	}

	fmt.Printf("\nsummary: %d/%d requests answered\n", replies, *count)
	if replies != *count {
		fmt.Fprintln(os.Stderr, "FAIL: not every request received a reply")
		os.Exit(1)
	}
	fmt.Println("PASS: every request got a synchronous reply via its _INBOX subject")
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
