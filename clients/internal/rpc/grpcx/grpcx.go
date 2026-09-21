// Package grpcx is the gRPC binding for the RPC lab's routing envelope: a
// GatewayService client that satisfies rpc.Client and a server helper that backs
// GatewayService with an rpc.Handler. It is the reference transport (§10) and
// the always-gRPC hop between a client and a gateway, or between two gateways.
//
// It is intentionally small and standard (no forced codecV2 / pool — that is a
// proto-bench fairness concern, not a correctness one here). The bus bindings in
// later phases reuse clients/internal/transport's Publisher/Consumer instead of
// adding a new backend here.
package grpcx

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
)

// Client is a GatewayService gRPC client that satisfies rpc.Client.
type Client struct {
	cc   *grpc.ClientConn
	c    rpcv1.GatewayServiceClient
	caps rpc.Capabilities
}

// Dial opens a client connection to a GatewayService server at target
// (host:port). The connection is lazy (grpc.NewClient) and reused for every Call.
func Dial(target string) (*Client, error) {
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		cc: cc,
		c:  rpcv1.NewGatewayServiceClient(cc),
		caps: rpc.Capabilities{
			NativeRequestReply: true,
			ServerDiscovery:    false,
			Streaming:          true,
			AtLeastOnce:        false,
		},
	}, nil
}

// Call issues req and returns the Response, respecting ctx's deadline.
func (c *Client) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	return c.c.Call(ctx, req)
}

// Stream is a bidirectional GatewayService.CallStream (§10): the caller Sends
// Requests and Recvs Responses, correlated positionally by the reference gRPC
// transport (one Response per Request, in order). It is not safe for concurrent
// Send from multiple goroutines — gRPC streams are single-writer.
type Stream struct {
	s rpcv1.GatewayService_CallStreamClient
}

// OpenStream starts a CallStream on the connection, bounded by ctx.
func (c *Client) OpenStream(ctx context.Context) (*Stream, error) {
	s, err := c.c.CallStream(ctx)
	if err != nil {
		return nil, err
	}
	return &Stream{s: s}, nil
}

// Send queues req on the stream.
func (s *Stream) Send(req *rpcv1.Request) error { return s.s.Send(req) }

// Recv blocks for the next Response; it returns io.EOF once the server has sent
// its last response after CloseSend.
func (s *Stream) Recv() (*rpcv1.Response, error) { return s.s.Recv() }

// CloseSend signals that no more Requests will be sent; Responses may still
// arrive until io.EOF.
func (s *Stream) CloseSend() error { return s.s.CloseSend() }

// Capabilities reports what the gRPC transport provides (§34).
func (c *Client) Capabilities() rpc.Capabilities { return c.caps }

// Close closes the underlying connection.
func (c *Client) Close() error { return c.cc.Close() }

// server adapts an rpc.Handler to the generated GatewayServiceServer. Both the
// unary Call and the bidi CallStream route through the same h.Handle, so a
// service and a gateway get streaming with no extra handler surface: the stream
// is per-message request/reply (§10), each received Request handled and its
// Response sent back in order.
type server struct {
	rpcv1.UnimplementedGatewayServiceServer
	h rpc.Handler
}

// RegisterServer registers a GatewayService on s that delegates Call and
// CallStream to h.
func RegisterServer(s *grpc.Server, h rpc.Handler) {
	rpcv1.RegisterGatewayServiceServer(s, &server{h: h})
}

func (s *server) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	return s.h.Handle(ctx, req)
}

// CallStream handles each Request on the stream through h and streams back one
// Response apiece, preserving order. A handler error is turned into an
// ErrorResponse (the stream stays open); only a transport Recv/Send failure or
// the client's CloseSend (io.EOF) ends the stream.
func (s *server) CallStream(stream rpcv1.GatewayService_CallStreamServer) error {
	ctx := stream.Context()
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		resp, herr := s.h.Handle(ctx, req)
		if herr != nil {
			resp = rpc.ErrorResponse(req, herr)
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
