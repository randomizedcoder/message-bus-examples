package rpc

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Pack wraps an application message as a google.protobuf.Any (type_url + binary
// value) for Request/Response.payload (§5). The codec (proto/protojson/vtproto)
// applies when the whole envelope is marshaled for the wire, not here: an
// Any's value is protobuf-binary by definition, and protojson expands the Any
// via the type registry when the enclosing message is marshaled.
func Pack(m proto.Message) (*anypb.Any, error) {
	if m == nil {
		return nil, fmt.Errorf("rpc: Pack: nil message")
	}
	return anypb.New(m)
}

// Unpack resolves the concrete type from the global protobuf registry (populated
// by importing the generated package) and returns a freshly decoded message
// (§5). It allocates a new message; on the hot path prefer UnpackInto.
func Unpack(a *anypb.Any) (proto.Message, error) {
	if a == nil {
		return nil, fmt.Errorf("rpc: Unpack: nil Any")
	}
	m, err := a.UnmarshalNew()
	if err != nil {
		return nil, fmt.Errorf("rpc: Unpack %q: %w", a.GetTypeUrl(), err)
	}
	return m, nil
}

// UnpackInto decodes the Any into a caller-provided message, verifying the type
// matches — no registry lookup and no allocation, for the gateway/service hot
// path where the expected type is known.
func UnpackInto(a *anypb.Any, m proto.Message) error {
	if a == nil {
		return fmt.Errorf("rpc: UnpackInto: nil Any")
	}
	if m == nil {
		return fmt.Errorf("rpc: UnpackInto: nil target")
	}
	if err := a.UnmarshalTo(m); err != nil {
		return fmt.Errorf("rpc: UnpackInto %q: %w", a.GetTypeUrl(), err)
	}
	return nil
}
