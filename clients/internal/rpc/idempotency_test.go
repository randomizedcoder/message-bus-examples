package rpc

import (
	"testing"
	"time"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

func okResp(id string) *rpcv1.Response {
	return &rpcv1.Response{RequestId: id, Status: rpcv1.Status_STATUS_OK}
}

func TestIdempotencyCacheGetPut(t *testing.T) {
	tests := []struct {
		description string
		ttl         time.Duration
		key         string
		putFirst    bool
		wantHit     bool
	}{
		{
			description: "put then get with a live ttl hits",
			ttl:         time.Minute,
			key:         "k1",
			putFirst:    true,
			wantHit:     true,
		},
		{
			description: "get a key that was never put misses",
			ttl:         time.Minute,
			key:         "absent",
			putFirst:    false,
			wantHit:     false,
		},
		{
			description: "empty key never caches (put no-op, get miss)",
			ttl:         time.Minute,
			key:         "",
			putFirst:    true,
			wantHit:     false,
		},
		{
			description: "ttl <= 0 disables the cache entirely",
			ttl:         0,
			key:         "k1",
			putFirst:    true,
			wantHit:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			c := NewIdempotencyCache(tt.ttl)
			if tt.putFirst {
				c.Put(tt.key, okResp("r-"+tt.key))
			}
			got, ok := c.Get(tt.key)
			if ok != tt.wantHit {
				t.Fatalf("Get hit = %v, want %v", ok, tt.wantHit)
			}
			if tt.wantHit {
				if got == nil || got.GetRequestId() != "r-"+tt.key {
					t.Fatalf("Get returned %v, want the stored response", got)
				}
				if c.Hits() != 1 {
					t.Errorf("Hits = %d, want 1", c.Hits())
				}
			} else if c.Hits() != 0 {
				t.Errorf("Hits = %d on a miss, want 0", c.Hits())
			}
		})
	}
}

func TestIdempotencyCacheExpiry(t *testing.T) {
	c := NewIdempotencyCache(time.Millisecond)
	c.Put("k", okResp("r"))

	if _, ok := c.Get("k"); !ok {
		t.Fatalf("expected an immediate hit before expiry")
	}
	time.Sleep(5 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatalf("expected a miss after ttl elapsed")
	}
	// Hits only counts the one live hit, not the expired lookup.
	if c.Hits() != 1 {
		t.Errorf("Hits = %d, want 1", c.Hits())
	}
}

func TestIdempotencyCacheHitsAccumulate(t *testing.T) {
	c := NewIdempotencyCache(time.Minute)
	c.Put("k", okResp("r"))
	const n = 3
	for i := 0; i < n; i++ {
		if _, ok := c.Get("k"); !ok {
			t.Fatalf("hit %d missed unexpectedly", i)
		}
	}
	if c.Hits() != n {
		t.Errorf("Hits = %d, want %d", c.Hits(), n)
	}
}
