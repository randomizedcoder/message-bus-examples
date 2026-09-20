// Package pool provides the low-memory-pressure primitives for the proto-bench
// harness: a size-class []byte pool whose method set matches grpc-go's
// mem.BufferPool (so one instance serves every transport, including gRPC,
// without this package importing grpc/mem) and a typed sync.Pool of generated
// messages. See docs/protobuf-grpc-benchmark-design.md §7.2.
package pool

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

// DefaultClasses are the size classes used when NewBuffers is called with no
// arguments. The top class is 1 MiB so the `max` fixture (≈960 KiB) sits just
// under it and can be compared pool=all vs pool=none.
var DefaultClasses = []int{256, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20}

// BufferPool is the byte-buffer pool interface. Its method set is identical to
// grpc-go's mem.BufferPool, so a *Buffers (or NopBuffers) satisfies both.
type BufferPool interface {
	Get(length int) *[]byte
	Put(*[]byte)
}

// Buffers is a size-class []byte pool. Rules (design §7.2):
//   - *[]byte, not []byte: avoids the interface-boxing allocation on Put.
//   - Exact-capacity classes: a pooled buffer is never larger than its class,
//     which fixes the "one huge buffer poisons the pool" retention problem
//     (Go issue #23199).
//   - Buffers larger than the top class are never pooled.
//   - Put drops anything whose cap is not exactly a class (e.g. a MarshalAppend
//     that grew the slice returns a fresh backing array of arbitrary cap).
type Buffers struct {
	classes []int
	pools   []sync.Pool
	hits    []atomic.Uint64
	misses  []atomic.Uint64
	drops   []atomic.Uint64
	// oversize accounting (n > top class): allocated fresh, never pooled.
	oversizeMiss atomic.Uint64
	oversizeDrop atomic.Uint64
}

// NewBuffers builds a pool over the given size classes (deduplicated and
// sorted ascending); with no classes it uses DefaultClasses.
func NewBuffers(classes ...int) *Buffers {
	if len(classes) == 0 {
		classes = DefaultClasses
	}
	cs := append([]int(nil), classes...)
	sort.Ints(cs)
	// dedupe in place
	out := cs[:0]
	for i, c := range cs {
		if c <= 0 {
			panic(fmt.Sprintf("pool: non-positive size class %d", c))
		}
		if i == 0 || c != cs[i-1] {
			out = append(out, c)
		}
	}
	cs = out
	return &Buffers{
		classes: cs,
		pools:   make([]sync.Pool, len(cs)),
		hits:    make([]atomic.Uint64, len(cs)),
		misses:  make([]atomic.Uint64, len(cs)),
		drops:   make([]atomic.Uint64, len(cs)),
	}
}

// classIndex returns the index of the smallest class >= n, or -1 if n exceeds
// the top class.
func (b *Buffers) classIndex(n int) int {
	i := sort.SearchInts(b.classes, n)
	if i == len(b.classes) {
		return -1
	}
	return i
}

// Get returns a *[]byte with len n and cap equal to the smallest class >= n.
// If n exceeds the top class a fresh slice of cap n is returned and never
// pooled.
func (b *Buffers) Get(n int) *[]byte {
	i := b.classIndex(n)
	if i < 0 {
		b.oversizeMiss.Add(1)
		buf := make([]byte, n)
		return &buf
	}
	if v := b.pools[i].Get(); v != nil {
		b.hits[i].Add(1)
		p := v.(*[]byte)
		*p = (*p)[:n]
		return p
	}
	b.misses[i].Add(1)
	buf := make([]byte, n, b.classes[i])
	return &buf
}

// Put returns a buffer to its class pool. A buffer whose cap is not exactly a
// class (grown by append, or oversize) is dropped so the pool only ever holds
// exact-capacity backing arrays.
func (b *Buffers) Put(p *[]byte) {
	if p == nil {
		return
	}
	c := cap(*p)
	i := sort.SearchInts(b.classes, c)
	if i == len(b.classes) || b.classes[i] != c {
		if c > 0 && c > b.classes[len(b.classes)-1] {
			b.oversizeDrop.Add(1)
		} else if i < len(b.classes) {
			b.drops[i].Add(1)
		} else {
			b.oversizeDrop.Add(1)
		}
		return
	}
	*p = (*p)[:0]
	b.pools[i].Put(p)
}

// ClassStat is per-size-class pool accounting.
type ClassStat struct {
	Class  int
	Hits   uint64
	Misses uint64
	Drops  uint64
}

// Stats is a snapshot of pool effectiveness for the run report. Totals include
// oversize (unpooled) operations; PerClass covers the fixed classes only.
type Stats struct {
	Hits     uint64
	Misses   uint64
	Drops    uint64
	PerClass []ClassStat
}

// Stats returns a snapshot of hit/miss/drop counters.
func (b *Buffers) Stats() Stats {
	s := Stats{PerClass: make([]ClassStat, len(b.classes))}
	for i, c := range b.classes {
		h, m, d := b.hits[i].Load(), b.misses[i].Load(), b.drops[i].Load()
		s.PerClass[i] = ClassStat{Class: c, Hits: h, Misses: m, Drops: d}
		s.Hits += h
		s.Misses += m
		s.Drops += d
	}
	s.Misses += b.oversizeMiss.Load()
	s.Drops += b.oversizeDrop.Load()
	return s
}

// NopBuffers is a no-op BufferPool: Get allocates, Put discards. It makes the
// -pool=none code path identical to -pool=all except for reuse.
type NopBuffers struct{}

func (NopBuffers) Get(n int) *[]byte { b := make([]byte, n); return &b }
func (NopBuffers) Put(*[]byte)       {}
func (NopBuffers) Stats() Stats      { return Stats{} }

// Msg is a typed sync.Pool of generated messages. PT is the pointer type; Put
// calls proto.Reset (which drops nested pointers — see design §7.1).
type Msg[T any, PT interface {
	*T
	proto.Message
}] struct {
	p sync.Pool
}

func (mp *Msg[T, PT]) Get() PT {
	if v := mp.p.Get(); v != nil {
		return v.(PT)
	}
	return PT(new(T))
}

func (mp *Msg[T, PT]) Put(m PT) {
	if m == nil {
		return
	}
	proto.Reset(m)
	mp.p.Put(m)
}

// MessagePool is the interface satisfied by both Msg and NopMsg, so -pool
// modes can swap pooling in and out behind one type.
type MessagePool[PT proto.Message] interface {
	Get() PT
	Put(PT)
}

// NopMsg is a no-op MessagePool: Get allocates, Put discards.
type NopMsg[T any, PT interface {
	*T
	proto.Message
}] struct{}

func (NopMsg[T, PT]) Get() PT  { return PT(new(T)) }
func (NopMsg[T, PT]) Put(m PT) {}
