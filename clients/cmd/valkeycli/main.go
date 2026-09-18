// valkeycli — a minimal ValKey pub/sub client (RESP PUBLISH/SUBSCRIBE).
//
//	valkeycli pub -addr 127.0.0.1:30637 -subject demo -msg "hello" -pass "$VALKEY_PASS"
//	valkeycli pub -subject demo -count 100 -rate 10/s -pass "$VALKEY_PASS"
//	valkeycli sub -subject demo -count 5 -timeout 10s -json -pass "$VALKEY_PASS"
//
// With -sentinels the client uses Sentinel-backed primary discovery
// (go-redis FailoverClient): it asks the Sentinels for the current primary
// and always connects there, following automatic failover. This routes every
// PUBLISH to the primary so it fans out to all replicas' subscribers.
//
//	valkeycli pub -sentinels 10.33.33.10:30650,10.33.33.10:30651,10.33.33.10:30652 \
//	  -subject demo -msg hi -pass "$VALKEY_PASS"
//
// Without -sentinels it connects to a single -addr (the round-robin NodePort,
// pinned per-host by sessionAffinity).
//
// NOTE: Valkey/Redis pub/sub is fire-and-forget and not persisted; during a
// failover in-flight messages can drop. Fetch the password:
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
	"strings"
	"time"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/cli"
	"github.com/redis/go-redis/v9"
)

// masterName matches `sentinel monitor <name>` in the valkey manifests.
const masterName = "mymaster"

func main() {
	f := cli.Parse("valkeycli", "127.0.0.1:30637", os.Getenv("VALKEY_PASS"))
	ctx := context.Background()

	rdb := newClient(f)
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
		log.Printf("subscribed to %q on %s; waiting for messages (Ctrl-C to quit)", f.Subject, target(f))
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

// target describes where the client is connected, for log messages.
func target(f *cli.Flags) string {
	if f.Sentinels != "" {
		return "sentinel[" + masterName + "] via " + f.Sentinels
	}
	return f.Addr
}

// newClient builds a Sentinel-backed FailoverClient when -sentinels is set
// (primary discovery + failover following), otherwise a direct client to -addr.
func newClient(f *cli.Flags) *redis.Client {
	if f.Sentinels != "" {
		var addrs []string
		for _, a := range strings.Split(f.Sentinels, ",") {
			if a = strings.TrimSpace(a); a != "" {
				addrs = append(addrs, a)
			}
		}
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:      masterName,
			SentinelAddrs:   addrs,
			Password:        f.Pass,
			MaxRetries:      -1,
			MinRetryBackoff: 200 * time.Millisecond,
		})
	}
	return redis.NewClient(&redis.Options{
		Addr:            f.Addr,
		Password:        f.Pass,
		MaxRetries:      -1,
		MinRetryBackoff: 200 * time.Millisecond,
	})
}
