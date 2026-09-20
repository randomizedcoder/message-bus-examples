package grpctransport

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/stats"
)

// RPCInfo carries the two per-RPC facts the service handler needs but the
// message alone cannot give it: the wall-clock time the request payload was
// received off the wire, and its wire length (framing included). The envelope
// StatsHandler fills these; the handler reads them to stamp
// server_receive_time and request_wire_bytes precisely, rather than
// approximating with handler-entry time and proto.Size (design §7.4).
type RPCInfo struct {
	recvUnixNano atomic.Int64
	wireBytes    atomic.Uint32
}

// RecvTime returns when the request payload arrived, or the zero time if no
// InPayload was seen (e.g. a stream with no message yet).
func (i *RPCInfo) RecvTime() time.Time {
	ns := i.recvUnixNano.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// WireBytes returns the request's on-the-wire length.
func (i *RPCInfo) WireBytes() uint32 { return i.wireBytes.Load() }

type rpcInfoKey struct{}

// RPCInfoFromContext returns the per-RPC info the StatsHandler attached, or nil
// if the server was built without the handler.
func RPCInfoFromContext(ctx context.Context) *RPCInfo {
	v, _ := ctx.Value(rpcInfoKey{}).(*RPCInfo)
	return v
}

// StatsHandler is the envelope-side gRPC stats.Handler: TagRPC puts a fresh
// *RPCInfo in the context, HandleRPC copies InPayload.WireLength / RecvTime
// into it. It is chained ahead of the application handler and (in P3/P4) the
// otelgrpc handler for standard RPC metrics. Conn callbacks are no-ops.
type StatsHandler struct{}

func (StatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, rpcInfoKey{}, &RPCInfo{})
}

func (StatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	in, ok := s.(*stats.InPayload)
	if !ok {
		return
	}
	if info := RPCInfoFromContext(ctx); info != nil {
		info.recvUnixNano.Store(in.RecvTime.UnixNano())
		if in.WireLength > 0 {
			info.wireBytes.Store(uint32(in.WireLength))
		}
	}
}

func (StatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (StatsHandler) HandleConn(context.Context, stats.ConnStats) {}
