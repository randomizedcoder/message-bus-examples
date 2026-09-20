// Package grpctransport is the gRPC transport for proto-bench: a pooled
// encoding.CodecV2 over grpc-go's mem.BufferPool, an envelope stats.Handler for
// server-side wire bytes and receive timestamps, and helpers to dial a client
// and build a server for the generated workloads.v1 services (design §7.4).
//
// grpc-go ≥ 1.66 lets a codec hand the transport reference-counted buffers and
// lets the transport hand the codec pooled input buffers; codecV2 wires our
// codec.Codec + pool.Buffers into both directions so the gRPC path allocates as
// little as the in-process codec loop. CodecV2 / ForceCodecV2 /
// ForceServerCodecV2 are stable API in the pinned grpc v1.83.2; the mem package
// and everything under experimental/ are experimental and the bench is their
// only consumer.
package grpctransport

import (
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
)

// codecV2 adapts a codec.Codec to grpc's encoding.CodecV2, marshalling into and
// materialising out of a mem.BufferPool. One instance is forced on both ends of
// a call (ForceCodecV2 on the client, ForceServerCodecV2 on the server), so no
// global RegisterCodecV2 / content-type negotiation is needed.
type codecV2 struct {
	inner codec.Codec
	pool  mem.BufferPool // *pool.Buffers satisfies it structurally; mem.NopBufferPool{} for -pool=none
	name  string         // "proto" for proto AND vtproto (wire-identical); "json" for protojson
}

// newCodecV2 builds the codec for one profile. name is grpc's content-subtype:
// proto and vtproto are byte-identical on the wire so both use "proto"; a run
// keeps both ends on the same profile anyway (design §7.1).
func newCodecV2(inner codec.Codec, p mem.BufferPool) *codecV2 {
	name := "proto"
	if codec.ContentType(inner) == codec.ContentType(codec.ProtoJSON) {
		name = "json"
	}
	return &codecV2{inner: inner, pool: p, name: name}
}

// Marshal encodes v into a pooled buffer for the HTTP/2 write. grpc frees the
// returned buffer after the write, so ownership passes to grpc and we never Put
// it ourselves. Payloads at or below grpc's pooling threshold (≤ 1 KiB) skip
// the pool — grpc's own policy, kept so tiny/small behave like grpc-default.
func (c *codecV2) Marshal(v any) (mem.BufferSlice, error) {
	m := v.(proto.Message)
	size := c.inner.SizeHint(m)
	if mem.IsBelowBufferPoolingThreshold(size) {
		b, err := c.inner.MarshalAppend(nil, m)
		if err != nil {
			return nil, err
		}
		return mem.BufferSlice{mem.SliceBuffer(b)}, nil
	}
	buf := c.pool.Get(size)
	out, err := c.inner.MarshalAppend((*buf)[:0], m)
	if err != nil {
		c.pool.Put(buf)
		return nil, err
	}
	*buf = out
	return mem.BufferSlice{mem.NewBuffer(buf, c.pool)}, nil
}

// Unmarshal materialises the received slice into one pooled buffer and decodes
// from its bytes. The buffer is freed on return; proto/vtproto copy every bytes
// and string field during Unmarshal (no alias mode), so nothing outlives it.
func (c *codecV2) Unmarshal(data mem.BufferSlice, v any) error {
	buf := data.MaterializeToBuffer(c.pool)
	defer buf.Free()
	return c.inner.Unmarshal(buf.ReadOnlyData(), v.(proto.Message))
}

func (c *codecV2) Name() string { return c.name }
