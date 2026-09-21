package rpc

import (
	"context"
	"fmt"
	"sync"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// MethodHandler is one backend method: it receives the already-decoded request
// payload and returns the response payload (or an error, which Dispatch maps to
// a Status via StatusOf). It is pure application code — no envelope, no transport.
type MethodHandler func(ctx context.Context, req proto.Message) (proto.Message, error)

// Mux dispatches a routed rpc.v1.Request to a registered MethodHandler by
// "service.method". It owns the server-side envelope work the gateway/service
// share: decode the Any payload into the concrete type, optionally run
// protovalidate (this is where the gRPC validation gap is closed — §7), invoke
// the handler, and pack + stamp the Response. Dispatch never returns a transport
// error: every failure becomes a non-OK Response so the caller sees a Status.
type Mux struct {
	mu       sync.RWMutex
	routes   map[string]route
	validate bool
}

type route struct {
	newReq func() proto.Message
	handle MethodHandler
}

// NewMux returns an empty Mux. If validate is true, decoded requests are checked
// with protovalidate before the handler runs.
func NewMux(validate bool) *Mux {
	return &Mux{routes: make(map[string]route), validate: validate}
}

func muxKey(service, method string) string { return service + "." + method }

// Handle registers h for service.method. newReq allocates a fresh request
// message of the concrete type the payload will decode into (e.g.
// func() proto.Message { return &benchmarkv1.CustomerLookupRequest{} }).
// Registering the same service.method twice panics — a programming error.
func (m *Mux) Handle(service, method string, newReq func() proto.Message, h MethodHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := muxKey(service, method)
	if _, dup := m.routes[k]; dup {
		panic(fmt.Sprintf("rpc: Mux.Handle: duplicate route %q", k))
	}
	m.routes[k] = route{newReq: newReq, handle: h}
}

// Dispatch routes req to its handler and returns the Response. It stamps
// server_received_at on entry and server_sent_at on exit (§20). It is safe for
// concurrent use.
func (m *Mux) Dispatch(ctx context.Context, req *rpcv1.Request) *rpcv1.Response {
	received := timestamppb.Now()

	m.mu.RLock()
	r, ok := m.routes[muxKey(req.GetService(), req.GetMethod())]
	m.mu.RUnlock()

	resp := m.dispatch(ctx, req, r, ok)
	resp.ServerReceivedAt = received
	resp.GatewayBRecv = received
	resp.ServerSentAt = timestamppb.Now()
	return resp
}

func (m *Mux) dispatch(ctx context.Context, req *rpcv1.Request, r route, ok bool) *rpcv1.Response {
	if !ok {
		return ErrorResponse(req, fmt.Errorf("%w: %s.%s", ErrNotFound, req.GetService(), req.GetMethod()))
	}
	msg := r.newReq()
	if err := UnpackInto(req.GetPayload(), msg); err != nil {
		return ErrorResponse(req, fmt.Errorf("%w: decode: %v", ErrInvalidArgument, err))
	}
	if m.validate {
		if err := protovalidate.Validate(msg); err != nil {
			return ErrorResponse(req, fmt.Errorf("%w: %v", ErrInvalidArgument, err))
		}
	}
	invoke := timestamppb.Now()
	out, err := r.handle(ctx, msg)
	done := timestamppb.Now()
	if err != nil {
		return ErrorResponse(req, err)
	}
	resp, perr := NewResponse(req, out)
	if perr != nil {
		return ErrorResponse(req, fmt.Errorf("%w: encode: %v", ErrInvalidArgument, perr))
	}
	resp.GatewayBInvoke = invoke
	resp.ServiceResp = done
	return resp
}
