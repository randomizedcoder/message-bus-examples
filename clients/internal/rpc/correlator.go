package rpc

import (
	"sync"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// Result is the one outcome delivered to a caller waiting on a request_id: a
// Response on success, or an Err (transport failure, correlator shutdown).
type Result struct {
	Response *rpcv1.Response
	Err      error
}

// Correlator matches asynchronous responses back to the goroutine that issued
// the request, keyed by request_id. It is shared by every transport whose
// replies arrive out of band (RabbitMQ, MQTT, Valkey pub/sub, Redpanda, and
// streaming gRPC), so the timeout/cancel/duplicate/orphan races are implemented
// once here rather than subtly differently per broker (§16).
//
// Correctness rule: every path that delivers a value first removes the id via
// sync.Map.LoadAndDelete, so a given channel is sent to at most once. cancel()
// only ever Deletes (never sends). The channel is buffered (cap 1) so Deliver
// never blocks, even when the caller has already timed out and stopped reading.
type Correlator struct {
	pending sync.Map // request_id (string) -> chan Result
}

// NewCorrelator returns an empty Correlator ready for use.
func NewCorrelator() *Correlator { return &Correlator{} }

// Register reserves id and returns a channel that will receive exactly one
// Result plus a cancel func the caller MUST invoke (typically deferred) to
// release the slot on timeout or cancellation. Registering an id that is already
// outstanding is a caller bug (duplicate request_id); the later registration
// wins and the earlier channel is orphaned.
func (c *Correlator) Register(id string) (<-chan Result, func()) {
	ch := make(chan Result, 1)
	c.pending.Store(id, ch)
	var once sync.Once
	cancel := func() { once.Do(func() { c.pending.Delete(id) }) }
	return ch, cancel
}

// Deliver routes result to the caller waiting on id and reports whether anyone
// was waiting. It returns false when id is unknown: already delivered (a
// duplicate response), already cancelled/timed out (a late response), or never
// registered (an orphan response, §22) — the caller can count these.
func (c *Correlator) Deliver(id string, result Result) bool {
	v, ok := c.pending.LoadAndDelete(id)
	if !ok {
		return false
	}
	v.(chan Result) <- result
	return true
}

// Shutdown delivers err to every outstanding caller and clears the table, so a
// dropped transport connection unblocks waiters instead of leaving them to hang
// until their individual deadlines (§16, "shutdown with outstanding requests").
func (c *Correlator) Shutdown(err error) {
	c.pending.Range(func(k, _ any) bool {
		if v, ok := c.pending.LoadAndDelete(k); ok {
			v.(chan Result) <- Result{Err: err}
		}
		return true
	})
}

// Len reports the number of outstanding registrations. Intended for tests and
// shutdown accounting; it ranges the map, so it is O(n).
func (c *Correlator) Len() int {
	n := 0
	c.pending.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}
