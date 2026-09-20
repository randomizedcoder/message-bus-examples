package grpctransport

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip" // register the gzip compressor for -compression=gzip
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// Fixed gRPC transport constants, recorded in run.json (design §7.4): 64 KiB
// HTTP/2 read/write buffers and a shared write buffer. Tracing and binary
// logging stay off — they force full payload materialisation and defeat the pool.
const (
	readWriteBufferBytes = 64 << 10
)

// Dial opens a client connection to target, forcing our pooled codecV2 and
// buffer pool onto every call. compression is "" | "none" | "gzip" (gRPC-only;
// gzip is a separate profile, fairness rule 5). The connection is opened once
// per driver process before warmup and reused for every request.
func Dial(target string, opts transport.Options, compression string) (*grpc.ClientConn, error) {
	cdc := newCodecV2(opts.Codec, poolFor(opts))
	callOpts := []grpc.CallOption{grpc.ForceCodecV2(cdc)}
	if compression == "gzip" {
		callOpts = append(callOpts, grpc.UseCompressor("gzip"))
	}
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(callOpts...),
		experimental.WithBufferPool(poolFor(opts)),
		grpc.WithSharedWriteBuffer(true),
		grpc.WithReadBufferSize(readWriteBufferBytes),
		grpc.WithWriteBufferSize(readWriteBufferBytes),
	)
}

// NewServer builds a *grpc.Server with the same pooled codec + buffer pool the
// clients use; the caller registers the generated service implementations and
// reflection on it. ForceServerCodecV2 makes the server decode/encode with our
// codec regardless of the client's content-subtype (a run keeps both ends on
// one profile), so no global RegisterCodecV2 is needed.
func NewServer(opts transport.Options, extra ...grpc.ServerOption) *grpc.Server {
	cdc := newCodecV2(opts.Codec, poolFor(opts))
	so := []grpc.ServerOption{
		grpc.ForceServerCodecV2(cdc),
		experimental.BufferPool(poolFor(opts)),
		grpc.SharedWriteBuffer(true),
		grpc.ReadBufferSize(readWriteBufferBytes),
		grpc.WriteBufferSize(readWriteBufferBytes),
	}
	return grpc.NewServer(append(so, extra...)...)
}

// poolFor returns the mem.BufferPool for opts: our size-class pool when pooling
// is on, grpc's no-op pool for -pool=none (identical code path, only reuse
// differs). opts.Pool's method set is exactly mem.BufferPool's.
func poolFor(opts transport.Options) mem.BufferPool {
	if opts.Pool == nil {
		return mem.NopBufferPool{}
	}
	return opts.Pool
}

// unaryRequester is the gRPC client side of the request/response transports for
// the unary RPCs (design §2.2: Deploy, Ping, ClockProbe). It selects the method
// from the request's concrete type and calls conn.Invoke, which unmarshals the
// reply directly into the caller's resp (no extra copy) using the forced codec.
type unaryRequester struct {
	cc *grpc.ClientConn
}

// NewRequester wraps an already-dialled connection as a transport.Requester.
// The connection carries the forced codec + pool, so the requester itself is
// codec-agnostic and safe for concurrent use by the driver's in-flight goroutines.
func NewRequester(cc *grpc.ClientConn) transport.Requester { return &unaryRequester{cc: cc} }

func (r *unaryRequester) Request(ctx context.Context, req, resp proto.Message) error {
	method, err := methodFor(req)
	if err != nil {
		return err
	}
	return r.cc.Invoke(ctx, method, req, resp)
}

func (r *unaryRequester) Close() error { return r.cc.Close() }

// methodFor maps a request message to its unary gRPC full-method name. The
// stream RPCs (DeployStream, WatchWorkload, ReportTelemetry, ReportUsage,
// StreamLogs) are driven through dedicated helpers, not this unary path.
func methodFor(req proto.Message) (string, error) {
	switch req.(type) {
	case *workloadsv1.PingRequest:
		return workloadsv1.BenchService_Ping_FullMethodName, nil
	case *workloadsv1.ClockProbeRequest:
		return workloadsv1.BenchService_ClockProbe_FullMethodName, nil
	case *workloadsv1.DeployRequest:
		return workloadsv1.WorkloadService_Deploy_FullMethodName, nil
	case *workloadsv1.FetchLogsRequest:
		return workloadsv1.LogService_FetchLogs_FullMethodName, nil
	default:
		return "", fmt.Errorf("grpctransport: no unary method for %T", req)
	}
}
