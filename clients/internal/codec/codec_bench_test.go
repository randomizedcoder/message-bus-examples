package codec_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
)

// Package-level sinks defeat dead-code elimination in the benchmarks.
var (
	byteSink []byte
	msgSink  proto.Message
)

var poolModes = []struct {
	name string
	mode pool.Mode
}{
	{"all", pool.ModeAll},
	{"none", pool.ModeNone},
}

// BenchmarkCodecMarshal reports ns/op, B/op, allocs/op and MB/s for
// codec × fixture × pool. b.N loops (not b.Loop) per the go-1.25 floor caveat
// in docs/benchmarks.md.
func BenchmarkCodecMarshal(b *testing.B) {
	c := corpus.New(42)
	for _, cc := range codecs {
		for _, f := range corpus.AllFixtures {
			msg, err := c.Message(f)
			if err != nil {
				b.Fatal(err)
			}
			wire, err := cc.c.MarshalAppend(nil, msg)
			if err != nil {
				b.Fatal(err)
			}
			for _, pm := range poolModes {
				bp := pm.mode.NewBufferPool()
				b.Run(cc.name+"/"+string(f)+"/"+pm.name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(wire)))
					for i := 0; i < b.N; i++ {
						buf, err := codec.Encode(cc.c, bp, msg)
						if err != nil {
							b.Fatal(err)
						}
						byteSink = *buf
						bp.Put(buf)
					}
				})
			}
		}
	}
}

// BenchmarkCodecUnmarshal reports decode cost for codec × fixture.
func BenchmarkCodecUnmarshal(b *testing.B) {
	c := corpus.New(42)
	for _, cc := range codecs {
		for _, f := range corpus.AllFixtures {
			msg, err := c.Message(f)
			if err != nil {
				b.Fatal(err)
			}
			wire, err := cc.c.MarshalAppend(nil, msg)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(cc.name+"/"+string(f), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(wire)))
				for i := 0; i < b.N; i++ {
					m := msg.ProtoReflect().New().Interface()
					if err := cc.c.Unmarshal(wire, m); err != nil {
						b.Fatal(err)
					}
					msgSink = m
				}
			})
		}
	}
}
