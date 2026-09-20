// Package codec provides the three benchmark codecs — binary Protobuf,
// ProtoJSON, and the opt-in vtprotobuf fast path — behind one interface, plus
// an Encode helper that marshals into a pooled buffer. Validation is
// deliberately NOT part of the codec: protovalidate runs as its own timed
// stage (design §7.3).
package codec

import (
	"fmt"
	"math"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
)

// Codec marshals and unmarshals proto messages. A buffer handed to Unmarshal
// may be returned to its pool once Unmarshal returns (bytes/string fields are
// copied; there is no alias mode).
type Codec interface {
	Name() string
	// SizeHint estimates the marshaled size so Encode can pick a pool class.
	SizeHint(m proto.Message) int
	MarshalAppend(dst []byte, m proto.Message) ([]byte, error)
	Unmarshal(b []byte, m proto.Message) error
}

// ---- binary Protobuf -------------------------------------------------------

type protoCodec struct {
	m proto.MarshalOptions
	u proto.UnmarshalOptions
}

func (protoCodec) Name() string                 { return "proto" }
func (protoCodec) SizeHint(m proto.Message) int { return proto.Size(m) }
func (c protoCodec) MarshalAppend(dst []byte, m proto.Message) ([]byte, error) {
	return c.m.MarshalAppend(dst, m)
}
func (c protoCodec) Unmarshal(b []byte, m proto.Message) error { return c.u.Unmarshal(b, m) }

// Proto is the official binary codec. UseCachedSize is safe because Encode
// calls SizeHint (proto.Size) immediately before MarshalAppend — the same
// contract grpc-go's built-in codec relies on. Deterministic stays off;
// map-order cost only matters for the sha256 integrity mode, which hashes the
// bytes the responder received, not a re-encoding (design §7.3).
var Proto Codec = protoCodec{
	m: proto.MarshalOptions{UseCachedSize: true, Deterministic: false},
	u: proto.UnmarshalOptions{Merge: false, DiscardUnknown: false},
}

// ---- ProtoJSON -------------------------------------------------------------

// sizeCache is a per-message-type EWMA of observed ProtoJSON output sizes.
// ProtoJSON has no Size function, so SizeHint returns the EWMA (seeded from a
// multiple of the binary size on first sight) and MarshalAppend feeds it, so
// steady-state ProtoJSON marshals hit the pool too (design §7.3).
type sizeCache struct {
	m sync.Map // protoreflect.FullName -> *ewma
}

type ewma struct {
	mu   sync.Mutex
	v    float64
	seen bool
}

func (s *sizeCache) hint(m proto.Message) int {
	name := m.ProtoReflect().Descriptor().FullName()
	v, _ := s.m.LoadOrStore(name, &ewma{})
	e := v.(*ewma)
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.seen {
		// Seed from the binary size; ProtoJSON is typically 2–3× larger.
		return proto.Size(m) * 3
	}
	return int(math.Ceil(e.v))
}

func (s *sizeCache) observe(m proto.Message, n int) {
	name := m.ProtoReflect().Descriptor().FullName()
	v, _ := s.m.LoadOrStore(name, &ewma{})
	e := v.(*ewma)
	e.mu.Lock()
	if !e.seen {
		e.v = float64(n)
		e.seen = true
	} else {
		const alpha = 0.2
		e.v = alpha*float64(n) + (1-alpha)*e.v
	}
	e.mu.Unlock()
	_ = name
}

type jsonCodec struct {
	m     protojson.MarshalOptions
	u     protojson.UnmarshalOptions
	sizes *sizeCache
}

func (jsonCodec) Name() string                   { return "protojson" }
func (c jsonCodec) SizeHint(m proto.Message) int { return c.sizes.hint(m) }
func (c jsonCodec) MarshalAppend(dst []byte, m proto.Message) ([]byte, error) {
	out, err := c.m.MarshalAppend(dst, m)
	if err == nil {
		c.sizes.observe(m, len(out)-len(dst))
	}
	return out, err
}
func (c jsonCodec) Unmarshal(b []byte, m proto.Message) error { return c.u.Unmarshal(b, m) }

// ProtoJSON is the official ProtoJSON codec (encoding/protojson). encoding/json
// on generated structs is never used (design rule 3).
var ProtoJSON Codec = jsonCodec{
	m:     protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false},
	u:     protojson.UnmarshalOptions{DiscardUnknown: false},
	sizes: &sizeCache{},
}

// ---- shared helpers --------------------------------------------------------

// Encode marshals m into a pooled buffer. The caller must Put the returned
// buffer once the transport has copied it (design §7.5 says when, per library).
func Encode(c Codec, bp pool.BufferPool, m proto.Message) (*[]byte, error) {
	buf := bp.Get(c.SizeHint(m))
	out, err := c.MarshalAppend((*buf)[:0], m)
	if err != nil {
		bp.Put(buf)
		return nil, err
	}
	*buf = out // if MarshalAppend grew it, cap changed; Put will drop it
	return buf, nil
}

// ByName resolves a -codec flag value to a Codec.
func ByName(name string) (Codec, error) {
	switch name {
	case "", "proto":
		return Proto, nil
	case "protojson":
		return ProtoJSON, nil
	case "vtproto":
		return VT, nil
	default:
		return nil, fmt.Errorf("codec: unknown %q (want proto|protojson|vtproto)", name)
	}
}

// ContentType is the media type carried as transport metadata so a responder
// can decode before seeing the envelope (design §3.9). proto and vtproto are
// byte-identical on the wire, so both report the protobuf media type.
func ContentType(c Codec) string {
	if c != nil && c.Name() == "protojson" {
		return "application/json"
	}
	return "application/protobuf"
}
