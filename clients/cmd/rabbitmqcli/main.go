// rabbitmqcli — a minimal RabbitMQ (AMQP 0-9-1) pub/sub client.
//
//	rabbitmqcli pub -addr 127.0.0.1:30567 -subject demo -msg "hello" -pass "$RABBITMQ_PASS"
//	rabbitmqcli pub -subject demo -count 100 -rate 10/s -pass "$RABBITMQ_PASS"
//	rabbitmqcli sub -subject demo -count 5 -timeout 10s -json -pass "$RABBITMQ_PASS"
//
// Publishing always uses publisher confirms — every message is acknowledged
// by the broker (or reported as nacked/timed-out).
//
// Default: pub/sub over a fanout exchange named by -subject; every subscriber
// binds its own exclusive queue, so each gets every message (ephemeral).
//
// With -durable: a durable **quorum queue** `<subject>.quorum` replicated
// across the 3-node cluster, fed via the default exchange with persistent
// messages. This is work-queue (shared, competing-consumer) semantics and it
// survives broker restarts / node loss — distinct from the ephemeral fanout.
//
// The password is the generated admin password. Fetch it with:
//
//	kubectl -n rabbitmq get secret rabbitmq-credentials \
//	  -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' | base64 -d
//
// and pass it via -pass or the RABBITMQ_PASS env var.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/cli"
)

func main() {
	f := cli.Parse("rabbitmqcli", "127.0.0.1:30567", os.Getenv("RABBITMQ_PASS"))

	url := fmt.Sprintf("amqp://%s:%s@%s/", f.User, f.Pass, f.Addr)
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("dial (check -user/-pass; see -h): %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	if f.Durable {
		runQuorum(f, ch)
		return
	}
	runFanout(f, ch)
}

// publishConfirmed publishes each message with publisher confirms and blocks
// for the broker's ack, returning an error on nack.
func publishConfirmed(f *cli.Flags, ch *amqp.Channel, exchange, key string, mode uint8) {
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("enable publisher confirms: %v", err)
	}
	if err := f.PubLoop(func(body string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, false, false,
			amqp.Publishing{ContentType: "text/plain", Body: []byte(body), DeliveryMode: mode})
		if err != nil {
			return err
		}
		if acked := dc.Wait(); !acked {
			return fmt.Errorf("message nacked by broker")
		}
		return nil
	}); err != nil {
		log.Fatalf("%v", err)
	}
}

// consume drains deliveries from msgs into Emit, honoring -count/-timeout.
func consume(f *cli.Flags, msgs <-chan amqp.Delivery, label string) {
	log.Printf("subscribed to %s on %s; waiting for messages (Ctrl-C to quit)", label, f.Addr)
	stop := f.Stop()
	lim := f.NewLimiter()
	for {
		select {
		case <-stop:
			return
		case <-lim.Done():
			return
		case m, ok := <-msgs:
			if !ok {
				return
			}
			f.Emit(f.Subject, string(m.Body))
			lim.Hit()
		}
	}
}

// runFanout is the default ephemeral pub/sub: a fanout exchange with per-
// subscriber exclusive queues.
func runFanout(f *cli.Flags, ch *amqp.Channel) {
	if err := ch.ExchangeDeclare(f.Subject, "fanout", false, true, false, false, nil); err != nil {
		log.Fatalf("exchange declare: %v", err)
	}
	switch f.Cmd {
	case "pub":
		publishConfirmed(f, ch, f.Subject, "", amqp.Transient)
	case "sub":
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			log.Fatalf("queue declare: %v", err)
		}
		if err := ch.QueueBind(q.Name, "", f.Subject, false, nil); err != nil {
			log.Fatalf("queue bind: %v", err)
		}
		msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
		if err != nil {
			log.Fatalf("consume: %v", err)
		}
		consume(f, msgs, fmt.Sprintf("exchange %q", f.Subject))
	}
}

// runQuorum is the durable path: a replicated quorum queue with persistent
// messages, delivered via the default exchange (work-queue semantics).
func runQuorum(f *cli.Flags, ch *amqp.Channel) {
	queue := f.Subject + ".quorum"
	if _, err := ch.QueueDeclare(queue, true, false, false, false,
		amqp.Table{"x-queue-type": "quorum"}); err != nil {
		log.Fatalf("quorum queue declare: %v", err)
	}
	switch f.Cmd {
	case "pub":
		// Default exchange ("") routes by queue name.
		publishConfirmed(f, ch, "", queue, amqp.Persistent)
	case "sub":
		msgs, err := ch.Consume(queue, "", true, false, false, false, nil)
		if err != nil {
			log.Fatalf("consume: %v", err)
		}
		consume(f, msgs, fmt.Sprintf("quorum queue %q", queue))
	}
}
