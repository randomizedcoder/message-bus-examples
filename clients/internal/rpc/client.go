package rpc

import (
	"context"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// Client is the one API applications see regardless of the transport underneath
// (§17 — the rpc.Call(ctx, request) principle). It is implemented by the gRPC
// gateway client and, for the buses, by a binding that pairs a
// transport.Publisher with a transport.Consumer and a Correlator.
type Client interface {
	// Call issues req and blocks for the Response or an error. Correlation,
	// timeout, and (for buses) reply routing are the binding's responsibility;
	// callers just read req.Timeout / ctx for the deadline.
	Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error)
	// Capabilities reports what this binding actually guarantees (§34).
	Capabilities() Capabilities
	// Close releases the underlying transport.
	Close() error
}

// Handler is the server-side application logic behind gateway-B: given a routed
// Request it returns a Response. The gateway owns transport receive/send,
// correlation, envelope receive/send stamping, and optional validation around
// it — the Handler is pure application code.
type Handler interface {
	Handle(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error)
}

// HandlerFunc adapts an ordinary function to Handler.
type HandlerFunc func(context.Context, *rpcv1.Request) (*rpcv1.Response, error)

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	return f(ctx, req)
}
