package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// vtMessage is the subset of the generated *_vtproto.pb.go API this codec uses.
// It is kept a separate, separately-labelled profile so the official-library
// comparison (proto vs protojson) never links vtproto code paths (design §7.1).
type vtMessage interface {
	proto.Message
	SizeVT() int
	MarshalToSizedBufferVT([]byte) (int, error)
	UnmarshalVT([]byte) error
}

type vtCodec struct{}

func (vtCodec) Name() string { return "vtproto" }

func (vtCodec) SizeHint(m proto.Message) int {
	if v, ok := m.(interface{ SizeVT() int }); ok {
		return v.SizeVT()
	}
	return proto.Size(m)
}

// MarshalAppend appends the vtproto encoding to dst. MarshalToSizedBufferVT
// fills a buffer of exactly SizeVT() bytes from the end, so we grow dst by that
// amount and hand it the tail.
func (vtCodec) MarshalAppend(dst []byte, m proto.Message) ([]byte, error) {
	v, ok := m.(vtMessage)
	if !ok {
		return nil, fmt.Errorf("codec: %T does not implement the vtproto API", m)
	}
	size := v.SizeVT()
	start := len(dst)
	if cap(dst)-start < size {
		grown := make([]byte, start+size)
		copy(grown, dst)
		dst = grown
	} else {
		dst = dst[:start+size]
	}
	if _, err := v.MarshalToSizedBufferVT(dst[start : start+size]); err != nil {
		return nil, err
	}
	return dst, nil
}

func (vtCodec) Unmarshal(b []byte, m proto.Message) error {
	v, ok := m.(vtMessage)
	if !ok {
		return fmt.Errorf("codec: %T does not implement the vtproto API", m)
	}
	return v.UnmarshalVT(b)
}

// VT is the vtprotobuf codec. On the wire it is byte-identical to Proto; a run
// keeps both ends on the same profile. Never used with pooled inputs via the
// unsafe unmarshal path (design §7.1).
var VT Codec = vtCodec{}
