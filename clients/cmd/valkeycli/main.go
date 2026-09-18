// valkeycli — a minimal ValKey pub/sub client (RESP PUBLISH/SUBSCRIBE).
//
//	valkeycli pub -addr 127.0.0.1:30637 -subject demo -msg "hello" -pass "$VALKEY_PASS"
//	valkeycli pub -subject demo -count 100 -rate 10/s -pass "$VALKEY_PASS"
//	valkeycli sub -subject demo -count 5 -timeout 10s -json -pass "$VALKEY_PASS"
//
// NOTE: Valkey/Redis pub/sub is fire-and-forget and not persisted; during
// a Sentinel failover in-flight messages can drop. Fetch the password:
//
//	kubectl -n valkey get secret valkey-credentials \
//	  -o jsonpath='{.data.password}' | base64 -d
//
// and pass it via -pass or the VALKEY_PASS env var.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/cli"
	"github.com/redis/go-redis/v9"
)

func main() {
	f := cli.Parse("valkeycli", "127.0.0.1:30637", os.Getenv("VALKEY_PASS"))
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:            f.Addr,
		Password:        f.Pass,
		MaxRetries:      -1,
		MinRetryBackoff: 200 * time.Millisecond,
	})
	defer rdb.Close()

	switch f.Cmd {
	case "pub":
		if err := f.PubLoop(func(body string) error {
			return rdb.Publish(ctx, f.Subject, body).Err()
		}); err != nil {
			log.Fatalf("%v (check -pass)", err)
		}
	case "sub":
		ps := rdb.Subscribe(ctx, f.Subject)
		defer ps.Close()
		if _, err := ps.Receive(ctx); err != nil {
			log.Fatalf("subscribe (check -pass): %v", err)
		}
		log.Printf("subscribed to %q on %s; waiting for messages (Ctrl-C to quit)", f.Subject, f.Addr)
		stop := f.Stop()
		lim := f.NewLimiter()
		ch := ps.Channel()
		for {
			select {
			case <-stop:
				return
			case <-lim.Done():
				return
			case m := <-ch:
				f.Emit(m.Channel, m.Payload)
				lim.Hit()
			}
		}
	}
}
