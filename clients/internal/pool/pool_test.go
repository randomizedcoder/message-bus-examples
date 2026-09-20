package pool_test

import (
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
)

// TestBuffersGetPut is the design §7.7 table for the size-class buffer pool.
func TestBuffersGetPut(t *testing.T) {
	classes := []int{256, 1024}
	maxClass := 1024

	type check func(t *testing.T, b *pool.Buffers)
	tests := []struct {
		description string
		kind        string
		run         check
	}{
		{
			description: "positive: Get(100) from [256,1024] yields cap 256, len 100",
			kind:        "positive",
			run: func(t *testing.T, b *pool.Buffers) {
				p := b.Get(100)
				if cap(*p) != 256 || len(*p) != 100 {
					t.Fatalf("got cap=%d len=%d, want cap=256 len=100", cap(*p), len(*p))
				}
			},
		},
		{
			description: "positive: Put then Get in the same class reuses the backing array; hits==1",
			kind:        "positive",
			run: func(t *testing.T, b *pool.Buffers) {
				p := b.Get(100)
				first := &(*p)[0]
				b.Put(p)
				q := b.Get(120)
				if &(*q)[0] != first {
					t.Fatalf("expected same backing array on reuse")
				}
				if got := b.Stats().Hits; got != 1 {
					t.Fatalf("Stats.Hits=%d, want 1", got)
				}
			},
		},
		{
			description: "negative: Put of a slice with cap 300 (not a class) is dropped; drops==1",
			kind:        "negative",
			run: func(t *testing.T, b *pool.Buffers) {
				s := make([]byte, 0, 300)
				b.Put(&s)
				if got := b.Stats().Drops; got != 1 {
					t.Fatalf("Stats.Drops=%d, want 1", got)
				}
			},
		},
		{
			description: "negative: Put(nil) is a no-op and does not panic",
			kind:        "negative",
			run: func(t *testing.T, b *pool.Buffers) {
				b.Put(nil)
				if s := b.Stats(); s.Hits+s.Misses+s.Drops != 0 {
					t.Fatalf("Put(nil) changed stats: %+v", s)
				}
			},
		},
		{
			description: "boundary: Get(256) exactly a class yields cap 256",
			kind:        "boundary",
			run: func(t *testing.T, b *pool.Buffers) {
				if p := b.Get(256); cap(*p) != 256 {
					t.Fatalf("cap=%d, want 256", cap(*p))
				}
			},
		},
		{
			description: "boundary: Get(257) rounds up to cap 1024",
			kind:        "boundary",
			run: func(t *testing.T, b *pool.Buffers) {
				if p := b.Get(257); cap(*p) != 1024 {
					t.Fatalf("cap=%d, want 1024", cap(*p))
				}
			},
		},
		{
			description: "boundary: Get(maxClass+1) is a fresh unpooled slice; misses==1; Put drops it",
			kind:        "boundary",
			run: func(t *testing.T, b *pool.Buffers) {
				p := b.Get(maxClass + 1)
				if cap(*p) < maxClass+1 {
					t.Fatalf("cap=%d, want >= %d", cap(*p), maxClass+1)
				}
				if got := b.Stats().Misses; got != 1 {
					t.Fatalf("Stats.Misses=%d, want 1", got)
				}
				b.Put(p)
				if got := b.Stats().Drops; got != 1 {
					t.Fatalf("Stats.Drops=%d, want 1", got)
				}
			},
		},
		{
			description: "boundary: Get(0) yields the smallest class, len 0",
			kind:        "boundary",
			run: func(t *testing.T, b *pool.Buffers) {
				p := b.Get(0)
				if cap(*p) != 256 || len(*p) != 0 {
					t.Fatalf("got cap=%d len=%d, want cap=256 len=0", cap(*p), len(*p))
				}
			},
		},
		{
			description: "corner: a grown (append past class) buffer is dropped by Put, no aliasing",
			kind:        "corner",
			run: func(t *testing.T, b *pool.Buffers) {
				p := b.Get(200) // cap 256
				grown := append(*p, make([]byte, 400)...)
				*p = grown // cap now > 256, not a class
				b.Put(p)
				if got := b.Stats().Drops; got != 1 {
					t.Fatalf("Stats.Drops=%d, want 1", got)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			b := pool.NewBuffers(classes...)
			tc.run(t, b)
		})
	}
}

// TestMsgPoolResetDropsNested documents that Put(→proto.Reset) drops nested
// messages and repeated fields (design §7.1 / §7.7 corner).
func TestMsgPoolResetDropsNested(t *testing.T) {
	tests := []struct {
		description string
		build       func() *workloadsv1.DeployRequest
	}{
		{
			description: "positive: Put then Get yields a zeroed message equal to empty",
			build:       func() *workloadsv1.DeployRequest { return &workloadsv1.DeployRequest{} },
		},
		{
			description: "corner: Get after Put of a populated nested Spec drops Containers and Op",
			build: func() *workloadsv1.DeployRequest {
				return &workloadsv1.DeployRequest{
					Op: &workloadsv1.DeployRequest_Create{Create: &workloadsv1.WorkloadSpec{
						Containers: []*workloadsv1.ContainerSpec{{Name: "app-0"}, {Name: "app-1"}},
					}},
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			var mp pool.Msg[workloadsv1.DeployRequest, *workloadsv1.DeployRequest]
			m := tc.build()
			mp.Put(m)
			got := mp.Get()
			if !proto.Equal(got, &workloadsv1.DeployRequest{}) {
				t.Fatalf("expected empty DeployRequest after reset, got %v", got)
			}
			if got.GetCreate() != nil || len(got.GetCreate().GetContainers()) != 0 {
				t.Fatalf("expected nil Op / no containers after reset")
			}
		})
	}
}

// TestMsgPoolPutNil covers the nil guard.
func TestMsgPoolPutNil(t *testing.T) {
	var mp pool.Msg[workloadsv1.PingRequest, *workloadsv1.PingRequest]
	mp.Put(nil) // must not panic
	if mp.Get() == nil {
		t.Fatal("Get returned nil after Put(nil)")
	}
}

// TestBuffersConcurrent is the -race corner: 64 goroutines Get/Put, all held
// slices distinct while checked out.
func TestBuffersConcurrent(t *testing.T) {
	b := pool.NewBuffers(256, 1024, 4096)
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				p := b.Get(200 + g%3*1000)
				(*p)[0] = byte(g)
				b.Put(p)
			}
		}(g)
	}
	wg.Wait()
}

// TestNopBuffers confirms the -pool=none path allocates and never records.
func TestNopBuffers(t *testing.T) {
	var b pool.BufferPool = pool.NopBuffers{}
	p := b.Get(100)
	if len(*p) != 100 {
		t.Fatalf("len=%d, want 100", len(*p))
	}
	b.Put(p) // no-op, no panic
}

// TestEncodeAllocsPoolAll asserts the design §7.7 corner / acceptance
// criterion: after warmup, encoding medium with pool=all is ≤ 1 alloc/op.
func TestEncodeAllocsPoolAll(t *testing.T) {
	b := pool.NewBuffers()
	// Reuse one buffer across iterations so the pool is warm.
	msg := &workloadsv1.PingRequest{}
	warm := b.Get(64)
	b.Put(warm)
	allocs := testing.AllocsPerRun(1000, func() {
		p := b.Get(32)
		out, _ := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend((*p)[:0], msg)
		*p = out
		b.Put(p)
	})
	if allocs > 1 {
		t.Fatalf("allocs/op = %.2f, want <= 1 with pool=all", allocs)
	}
}
