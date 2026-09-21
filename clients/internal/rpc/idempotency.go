package rpc

import (
	"sync"
	"sync/atomic"
	"time"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// IdempotencyCache remembers the Response of a completed logical operation,
// keyed by idempotency_key, so a retried request (a new request_id but the same
// idempotency_key, §29) returns the original result instead of executing the
// operation twice. Entries expire after ttl.
//
// It is a demonstration cache, not a single-flight lock: two *concurrent*
// requests with the same key can both miss and both execute. The scenario §29
// cares about — a retry *after* a lost response — is sequential and is
// deduplicated. Hits reports how many retries were served from cache.
type IdempotencyCache struct {
	ttl     time.Duration
	mu      sync.Mutex
	entries map[string]entry
	hits    atomic.Uint64
}

type entry struct {
	resp    *rpcv1.Response
	expires time.Time
}

// NewIdempotencyCache returns a cache whose entries live for ttl. A ttl <= 0
// disables caching (Get always misses, Put is a no-op).
func NewIdempotencyCache(ttl time.Duration) *IdempotencyCache {
	return &IdempotencyCache{ttl: ttl, entries: make(map[string]entry)}
}

// Get returns the cached Response for key and true on a live hit; it lazily
// evicts an expired entry and returns false. An empty key or disabled cache
// always misses.
func (c *IdempotencyCache) Get(key string) (*rpcv1.Response, bool) {
	if key == "" || c.ttl <= 0 {
		return nil, false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if now.After(e.expires) {
		delete(c.entries, key)
		return nil, false
	}
	c.hits.Add(1)
	return e.resp, true
}

// Put stores resp under key with the cache's ttl. Empty key or disabled cache
// is a no-op. Only successful operations should be cached by the caller.
func (c *IdempotencyCache) Put(key string, resp *rpcv1.Response) {
	if key == "" || c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	c.entries[key] = entry{resp: resp, expires: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}

// Hits is the number of retries served from cache (a duplicate-operation
// counter for metrics, §22).
func (c *IdempotencyCache) Hits() uint64 { return c.hits.Load() }
