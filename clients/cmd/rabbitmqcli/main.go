// rabbitmqcli — a minimal RabbitMQ (AMQP 0-9-1) pub/sub client.
//
//	rabbitmqcli pub -addr 127.0.0.1:30567 -subject demo -msg "hello" -pass "$RABBITMQ_PASS"
//	rabbitmqcli sub -addr 127.0.0.1:30567 -subject demo -pass "$RABBITMQ_PASS"
//
// Pub/sub is modelled with a fanout exchange named by -subject: every
// subscriber binds its own exclusive queue, so each gets every message.
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

	// Fanout exchange shared by publishers and subscribers.
	if err := ch.ExchangeDeclare(f.Subject, "fanout", false, true, false, false, nil); err != nil {
		log.Fatalf("exchange declare: %v", err)
	}

	switch f.Cmd {
	case "pub":
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ch.PublishWithContext(ctx, f.Subject, "", false, false,
			amqp.Publishing{ContentType: "text/plain", Body: []byte(f.Msg)}); err != nil {
			log.Fatalf("publish: %v", err)
		}
		fmt.Printf("published to exchange %q: %s\n", f.Subject, f.Msg)
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
		fmt.Printf("subscribed to exchange %q on %s; waiting for messages (Ctrl-C to quit)\n", f.Subject, f.Addr)
		for m := range msgs {
			fmt.Printf("%s  [%s] %s\n", time.Now().Format(time.RFC3339), f.Subject, string(m.Body))
		}
	}
}
