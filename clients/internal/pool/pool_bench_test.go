package pool_test

import (
	"testing"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
)

var bufSink *[]byte

// BenchmarkBuffersGetPut isolates the size-class pool's Get/Put hot path,
// live pool vs the no-op pool, at a few representative sizes.
func BenchmarkBuffersGetPut(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"256", 200},
		{"4k", 3500},
		{"64k", 60000},
	}
	pools := []struct {
		name string
		bp   pool.BufferPool
	}{
		{"all", pool.NewBuffers()},
		{"none", pool.NopBuffers{}},
	}
	for _, p := range pools {
		for _, s := range sizes {
			b.Run(p.name+"/"+s.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					buf := p.bp.Get(s.n)
					bufSink = buf
					p.bp.Put(buf)
				}
			})
		}
	}
}
