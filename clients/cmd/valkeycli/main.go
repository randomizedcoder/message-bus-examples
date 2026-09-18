// valkeycli — a minimal ValKey pub/sub client (RESP PUBLISH/SUBSCRIBE).
//
//	valkeycli pub -addr 127.0.0.1:30637 -subject demo -msg "hello" -pass "$VALKEY_PASS"
//	valkeycli sub -addr 127.0.0.1:30637 -subject demo -pass "$VALKEY_PASS"
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
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/cli"
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
		if err := rdb.Publish(ctx, f.Subject, f.Msg).Err(); err != nil {
			log.Fatalf("publish (check -pass): %v", err)
		}
		fmt.Printf("published to %q: %s\n", f.Subject, f.Msg)
	case "sub":
		ps := rdb.Subscribe(ctx, f.Subject)
		defer ps.Close()
		if _, err := ps.Receive(ctx); err != nil {
			log.Fatalf("subscribe (check -pass): %v", err)
		}
		fmt.Printf("subscribed to %q on %s; waiting for messages (Ctrl-C to quit)\n", f.Subject, f.Addr)
		for m := range ps.Channel() {
			fmt.Printf("%s  [%s] %s\n", time.Now().Format(time.RFC3339), m.Channel, m.Payload)
		}
	}
}
