package rpc

import (
	"errors"
	"sync"
	"testing"
	"time"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// recvWithin returns the Result on ch or fails if none arrives within d.
func recvWithin(t *testing.T, ch <-chan Result, d time.Duration) Result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(d):
		t.Fatalf("expected a Result within %s, got none", d)
		return Result{}
	}
}

// mustNotRecv fails if any Result arrives on ch within d.
func mustNotRecv(t *testing.T, ch <-chan Result, d time.Duration) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("expected no Result, got %+v", r)
	case <-time.After(d):
	}
}

func TestCorrelatorDeliver(t *testing.T) {
	const id = "req-1"
	resp := &rpcv1.Response{RequestId: id}

	tests := []struct {
		description   string
		run           func(t *testing.T, c *Correlator) bool // returns the last Deliver's result
		wantDelivered bool                                   // expected return of the asserted Deliver
	}{
		{
			description: "normal deliver reaches the registered caller",
			run: func(t *testing.T, c *Correlator) bool {
				_, cancel := c.Register(id)
				defer cancel()
				return c.Deliver(id, Result{Response: resp})
			},
			wantDelivered: true,
		},
		{
			description:   "deliver to an unregistered id is an orphan and returns false",
			run:           func(t *testing.T, c *Correlator) bool { return c.Deliver("never-registered", Result{Response: resp}) },
			wantDelivered: false,
		},
		{
			description: "duplicate response: the second deliver returns false",
			run: func(t *testing.T, c *Correlator) bool {
				ch, cancel := c.Register(id)
				defer cancel()
				if !c.Deliver(id, Result{Response: resp}) {
					t.Fatal("first deliver should succeed")
				}
				_ = recvWithin(t, ch, time.Second) // drain the single value
				return c.Deliver(id, Result{Response: resp})
			},
			wantDelivered: false,
		},
		{
			description: "late response after cancel returns false",
			run: func(t *testing.T, c *Correlator) bool {
				_, cancel := c.Register(id)
				cancel()
				return c.Deliver(id, Result{Response: resp})
			},
			wantDelivered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			c := NewCorrelator()
			if got := tt.run(t, c); got != tt.wantDelivered {
				t.Fatalf("Deliver returned %v, want %v", got, tt.wantDelivered)
			}
		})
	}
}

func TestCorrelatorDeliversResponseValue(t *testing.T) {
	c := NewCorrelator()
	const id = "req-value"
	ch, cancel := c.Register(id)
	defer cancel()
	want := &rpcv1.Response{RequestId: id, Status: rpcv1.Status_STATUS_OK}
	if !c.Deliver(id, Result{Response: want}) {
		t.Fatal("deliver should succeed")
	}
	got := recvWithin(t, ch, time.Second)
	if got.Err != nil {
		t.Fatalf("unexpected err: %v", got.Err)
	}
	if got.Response != want {
		t.Fatalf("got response %p, want %p", got.Response, want)
	}
}

func TestCorrelatorRegisterCancelReleases(t *testing.T) {
	c := NewCorrelator()
	if c.Len() != 0 {
		t.Fatalf("fresh correlator Len = %d, want 0", c.Len())
	}
	_, cancel := c.Register("a")
	if c.Len() != 1 {
		t.Fatalf("after Register Len = %d, want 1", c.Len())
	}
	cancel()
	if c.Len() != 0 {
		t.Fatalf("after cancel Len = %d, want 0", c.Len())
	}
	// cancel is idempotent.
	cancel()
	if c.Len() != 0 {
		t.Fatalf("after second cancel Len = %d, want 0", c.Len())
	}
}

func TestCorrelatorCancelAfterDeliverIsNoop(t *testing.T) {
	c := NewCorrelator()
	const id = "req-cad"
	ch, cancel := c.Register(id)
	if !c.Deliver(id, Result{Response: &rpcv1.Response{RequestId: id}}) {
		t.Fatal("deliver should succeed")
	}
	_ = recvWithin(t, ch, time.Second)
	cancel() // must not panic or affect anything
	if c.Len() != 0 {
		t.Fatalf("Len = %d, want 0", c.Len())
	}
}

func TestCorrelatorShutdown(t *testing.T) {
	c := NewCorrelator()
	ids := []string{"s1", "s2", "s3"}
	chans := make([]<-chan Result, len(ids))
	for i, id := range ids {
		ch, cancel := c.Register(id)
		defer cancel()
		chans[i] = ch
	}
	if c.Len() != len(ids) {
		t.Fatalf("Len = %d, want %d", c.Len(), len(ids))
	}
	shutErr := errors.New("connection dropped")
	c.Shutdown(shutErr)
	if c.Len() != 0 {
		t.Fatalf("after Shutdown Len = %d, want 0", c.Len())
	}
	for i, ch := range chans {
		got := recvWithin(t, ch, time.Second)
		if !errors.Is(got.Err, shutErr) {
			t.Fatalf("id %s: got err %v, want %v", ids[i], got.Err, shutErr)
		}
	}
	// A deliver after shutdown is an orphan.
	if c.Deliver("s1", Result{}) {
		t.Fatal("deliver after shutdown should return false")
	}
}

// TestCorrelatorConcurrentDeliverSingleShot fires many concurrent Delivers for
// one id; exactly one must win and the channel must receive exactly one value
// (the at-least-once duplicate-delivery corner case, §29).
func TestCorrelatorConcurrentDeliverSingleShot(t *testing.T) {
	c := NewCorrelator()
	const id = "race"
	ch, cancel := c.Register(id)
	defer cancel()

	const n = 64
	var wg sync.WaitGroup
	var wins int64
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if c.Deliver(id, Result{Response: &rpcv1.Response{RequestId: id}}) {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("concurrent delivers won %d times, want exactly 1", wins)
	}
	_ = recvWithin(t, ch, time.Second)
	mustNotRecv(t, ch, 50*time.Millisecond) // no second value
}

// TestCorrelatorCancelDeliverRace runs cancel and Deliver concurrently many
// times; it must never panic, never double-send, and the channel must carry at
// most one value.
func TestCorrelatorCancelDeliverRace(t *testing.T) {
	for iter := 0; iter < 500; iter++ {
		c := NewCorrelator()
		id := "r"
		ch, cancel := c.Register(id)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancel() }()
		go func() { defer wg.Done(); c.Deliver(id, Result{Response: &rpcv1.Response{RequestId: id}}) }()
		wg.Wait()
		// Drain 0 or 1 value; never block, never a second value.
		select {
		case <-ch:
		default:
		}
		select {
		case r := <-ch:
			t.Fatalf("iter %d: unexpected second value %+v", iter, r)
		default:
		}
		if c.Len() != 0 {
			t.Fatalf("iter %d: Len = %d, want 0", iter, c.Len())
		}
	}
}
