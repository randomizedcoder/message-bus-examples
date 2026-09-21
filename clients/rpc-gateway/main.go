// Command rpc-gateway is gateway-A of the RPC lab (§17): a GatewayService gRPC
// ingress server that receives a routed rpc.v1 envelope from a client and
// forwards it, unchanged, to a backend GatewayService (rpc-service, or another
// gateway) over a transport chosen per service. In this first phase the only
// transport is gRPC (grpcx), so the gateway is a uniform GatewayService proxy:
// client -> gateway-A -> backend, every hop speaking Call.
//
// The gateway is deliberately payload-blind: it never decodes the Any, so it
// routes any service.method without knowing its concrete types. It stamps the
// gateway-A leg of the §20 timeline (T1 receive, T3 publish-to-backend, T10
// response-received) and derives the forward deadline from ctx and req.Timeout.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/timestamppb"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/grpcx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/mqttx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/natsx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/rabbitmqx"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/valkeyx"
)

func main() {
	addr := flag.String("grpc-addr", ":9430", "GatewayService gRPC ingress listen address")
	backend := flag.String("backend", "localhost:9440", "default backend: a GatewayService host:port (grpc), a NATS broker host:port (nats/natsjs), or a full amqp:// URL (rabbitmq*)")
	egress := flag.String("transport", "grpc", "egress transport to the backend: grpc | nats | natsjs | rabbitmq | rabbitmq-direct | mqtt | valkey | valkey-stream")
	mqttQoS := flag.Int("mqtt-qos", 1, "MQTT QoS (0, 1, or 2) for -transport mqtt egress")
	valkeyPass := flag.String("valkey-pass", os.Getenv("VALKEY_PASSWORD"), "Valkey primary password for valkey* egress (default $VALKEY_PASSWORD); -backend is then the comma-separated Sentinel list")
	var routes routeFlags
	flag.Var(&routes, "route", "per-service backend override service=host:port (repeatable; grpc egress only)")
	flag.Parse()

	router, err := newRouter(*egress, *backend, byte(*mqttQoS), *valkeyPass, routes)
	if err != nil {
		log.Fatalf("rpc-gateway: %v", err)
	}
	defer router.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("rpc-gateway: listen %s: %v", *addr, err)
	}
	srv := grpc.NewServer()
	grpcx.RegisterServer(srv, router)
	reflection.Register(srv)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		srv.GracefulStop()
	}()

	log.Printf("rpc-gateway: proxying GatewayService on %s -> default backend %s over %s (%d route override(s))", lis.Addr(), *backend, *egress, len(routes))
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("rpc-gateway: serve: %v", err)
	}
}

// router is the gateway-A Handler: it forwards each request to the backend
// selected by its service, stamping the gateway-A timeline around the hop. It
// caches one rpc.Client per distinct backend target so connections are reused.
// The egress transport (grpc | nats) is chosen once for the gateway; the router
// stays payload-blind either way — over NATS the subject is derived from the
// envelope's service.method, so a single connection serves every service.
type router struct {
	transport     string
	defaultTarget string
	mqttQoS       byte
	valkeyPass    string
	byService     map[string]string // service -> target override

	mu      sync.Mutex
	clients map[string]rpc.Client // target -> client
}

func newRouter(transport, defaultTarget string, mqttQoS byte, valkeyPass string, routes routeFlags) (*router, error) {
	switch transport {
	case "grpc", "nats", "natsjs", "rabbitmq", "rabbitmq-direct", "mqtt", "valkey", "valkey-stream":
	default:
		return nil, fmt.Errorf("unknown -transport %q (grpc|nats|natsjs|rabbitmq|rabbitmq-direct|mqtt|valkey|valkey-stream)", transport)
	}
	if transport == "mqtt" && mqttQoS > 2 {
		return nil, fmt.Errorf("-mqtt-qos must be 0, 1, or 2, got %d", mqttQoS)
	}
	r := &router{
		transport:     transport,
		defaultTarget: defaultTarget,
		mqttQoS:       mqttQoS,
		valkeyPass:    valkeyPass,
		byService:     make(map[string]string, len(routes)),
		clients:       make(map[string]rpc.Client),
	}
	for _, rt := range routes {
		r.byService[rt.service] = rt.target
	}
	return r, nil
}

// target returns the backend for a service (its override, else the default).
func (r *router) target(service string) string {
	if t, ok := r.byService[service]; ok {
		return t
	}
	return r.defaultTarget
}

// client returns a cached rpc.Client for target, dialing lazily on first use.
func (r *router) client(target string) (rpc.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clients[target]; ok {
		return c, nil
	}
	var (
		c   rpc.Client
		err error
	)
	switch r.transport {
	case "nats":
		c, err = natsx.Dial(target)
	case "natsjs":
		c, err = natsx.DialJetStream(target)
	case "rabbitmq":
		// target is a full amqp:// URL (it carries the credentials).
		c, err = rabbitmqx.Dial(target, rabbitmqx.ModeReplyQueue)
	case "rabbitmq-direct":
		c, err = rabbitmqx.Dial(target, rabbitmqx.ModeDirect)
	case "mqtt":
		c, err = mqttx.Dial(target, r.mqttQoS)
	case "valkey":
		c, err = valkeyx.Dial(target, r.valkeyPass)
	case "valkey-stream":
		c, err = valkeyx.DialStream(target, r.valkeyPass)
	default:
		c, err = grpcx.Dial(target)
	}
	if err != nil {
		return nil, err
	}
	r.clients[target] = c
	return c, nil
}

// Handle forwards req to its backend and returns the Response, mapping a
// transport-level forward failure to an ErrorResponse (so the client always sees
// a Status, never a raw gRPC error). It stamps GatewayARecv (T1) on entry,
// GatewayAPub (T3) before the hop, and GatewayARecvResp (T10) on return.
func (r *router) Handle(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	recv := timestamppb.Now()

	// Bound the forward hop by the request's own timeout when the inbound ctx
	// carries no earlier deadline; this is the §20 contract that the deadline
	// travels with the envelope.
	if d := req.GetTimeout().AsDuration(); d > 0 {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}

	c, err := r.client(r.target(req.GetService()))
	if err != nil {
		resp := rpc.ErrorResponse(req, fmt.Errorf("%w: dial backend: %v", rpc.ErrUnavailable, err))
		resp.GatewayARecv = recv
		resp.GatewayARecvResp = timestamppb.Now()
		return resp, nil
	}

	pub := timestamppb.Now()
	resp, err := c.Call(ctx, req)
	got := timestamppb.Now()
	if err != nil {
		resp = rpc.ErrorResponse(req, fmt.Errorf("%w: forward: %v", rpc.ErrUnavailable, err))
	}
	resp.GatewayARecv = recv
	resp.GatewayAPub = pub
	resp.GatewayARecvResp = got
	return resp, nil
}

// Close closes every cached backend client.
func (r *router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var first error
	for _, c := range r.clients {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// routeFlags collects repeated -route service=host:port overrides.
type routeFlags []routeOverride

type routeOverride struct {
	service string
	target  string
}

func (f *routeFlags) String() string {
	parts := make([]string, 0, len(*f))
	for _, r := range *f {
		parts = append(parts, r.service+"="+r.target)
	}
	return strings.Join(parts, ",")
}

func (f *routeFlags) Set(v string) error {
	i := strings.IndexByte(v, '=')
	if i <= 0 || i == len(v)-1 {
		return fmt.Errorf("route must be service=host:port, got %q", v)
	}
	*f = append(*f, routeOverride{service: v[:i], target: v[i+1:]})
	return nil
}
