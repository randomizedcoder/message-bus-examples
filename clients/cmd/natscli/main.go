// natscli — a minimal NATS pub/sub client for the message-bus-examples cluster.
//
//	natscli pub -addr 127.0.0.1:30422 -subject demo -msg "hello"
//	natscli pub -subject demo -count 100 -rate 10/s
//	natscli sub -subject demo -count 5 -timeout 10s -json
//
// With -jetstream the client uses durable JetStream: pub creates the stream
// (idempotently) and publishes with an ack; sub binds a durable consumer, so
// messages persist and are redelivered across restarts / node failover.
// Without -jetstream it uses core (fire-and-forget) NATS pub/sub.
package main

import (
	"errors"
	"log"
	"strings"
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

	if f.JetStream {
		runJetStream(f, nc)
		return
	}
	runCore(f, nc)
}

// runCore is the default fire-and-forget core NATS path.
func runCore(f *cli.Flags, nc *nats.Conn) {
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

// runJetStream is the durable path: a persistent, replicated stream plus a
// durable consumer.
func runJetStream(f *cli.Flags, nc *nats.Conn) {
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}
	stream := streamNameFor(f.Subject)
	if err := ensureStream(js, stream, f.Subject); err != nil {
		log.Fatalf("ensure stream %q: %v", stream, err)
	}

	switch f.Cmd {
	case "pub":
		if err := f.PubLoop(func(body string) error {
			_, err := js.Publish(f.Subject, []byte(body))
			return err
		}); err != nil {
			log.Fatalf("%v", err)
		}
	case "sub":
		lim := f.NewLimiter()
		durable := stream + "_CLI"
		if _, err := js.Subscribe(f.Subject, func(m *nats.Msg) {
			f.Emit(m.Subject, string(m.Data))
			_ = m.Ack()
			lim.Hit()
		}, nats.Durable(durable), nats.ManualAck(), nats.BindStream(stream)); err != nil {
			log.Fatalf("subscribe (jetstream): %v", err)
		}
		log.Printf("subscribed to %q on stream %q (durable %q) via %s; waiting (Ctrl-C to quit)",
			f.Subject, stream, durable, f.Addr)
		select {
		case <-f.Stop():
		case <-lim.Done():
		}
	}
}

// ensureStream creates a persistent, 3-replica stream for subject if it does
// not already exist (idempotent — nothing pre-provisions streams server-side).
func ensureStream(js nats.JetStreamContext, name, subject string) error {
	_, err := js.StreamInfo(name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     name,
			Subjects: []string{subject},
			Storage:  nats.FileStorage,
			Replicas: 3,
		})
	}
	return err
}

// streamNameFor derives a valid JetStream stream name from a subject (stream
// names may not contain '.', '*', '>', '/', or spaces).
func streamNameFor(subject string) string {
	var b strings.Builder
	for _, r := range subject {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	s := b.String()
	if s == "" {
		s = "STREAM"
	}
	return "MBEX_" + strings.ToUpper(s)
}
