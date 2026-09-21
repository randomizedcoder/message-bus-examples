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

// Capabilities reports what the gRPC transport provides (§34).
func (c *Client) Capabilities() rpc.Capabilities { return c.caps }

// Close closes the underlying connection.
func (c *Client) Close() error { return c.cc.Close() }

// server adapts an rpc.Handler to the generated GatewayServiceServer. CallStream
// (bidi) is left to the embedded Unimplemented until the streaming PR (§10).
type server struct {
	rpcv1.UnimplementedGatewayServiceServer
	h rpc.Handler
}

// RegisterServer registers a GatewayService on s that delegates unary Call to h.
func RegisterServer(s *grpc.Server, h rpc.Handler) {
	rpcv1.RegisterGatewayServiceServer(s, &server{h: h})
}

func (s *server) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	return s.h.Handle(ctx, req)
}
