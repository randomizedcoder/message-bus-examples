// Package transport defines the transport-neutral interfaces the proto-bench
// driver and region-agent talk through, so the harness loop and the agent
// handlers are written once and every transport (gRPC in P2; NATS, RabbitMQ,
// Valkey, MQTT in P3) plugs in behind the same shapes (design §6).
//
// The split mirrors the two message-flow families of the scenario (§2.2):
//   - request/response  → Requester (client) + Responder (server): gRPC unary
//     and bidi, NATS request-reply, RabbitMQ RPC, Valkey stream RPC.
//   - fire-and-forward  → Publisher (client) + Consumer (server): JetStream,
//     RabbitMQ quorum, Valkey streams, MQTT — telemetry / usage / logs.
//
// Every implementation shares one codec + one buffer pool via Options, so a
// run keeps identical bytes on the wire across transports (fairness rule 2).
package transport

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
)

// Options are the per-run knobs every transport is constructed with. They are
// fixed for the life of a run so a cell differs from its neighbour in exactly
// one axis (fairness rule 13).
type Options struct {
	Codec     codec.Codec     // proto | protojson | vtproto — the same instance both ends use
	Pool      pool.BufferPool // *pool.Buffers, or a Nop pool for -pool=none
	Region    string          // logical region this endpoint serves ("us-west-2", …)
	Integrity bool            // -integrity=sha256: stamp + verify envelope.request_sha256
	Validate  bool            // -validate=on: run protovalidate on decoded messages
}

// Requester issues one request and fills resp, blocking for the reply. It is
// the client side of a request/response transport (gRPC unary, NATS
// request-reply, RabbitMQ RPC, Valkey stream RPC). Implementations are safe for
// concurrent use by the driver's in-flight goroutines; the envelope carries the
// per-request correlation, so one Requester multiplexes many outstanding calls.
type Requester interface {
	// Request sends req and unmarshals the reply into resp. Both are already
	// envelope-stamped by the caller; the transport is responsible only for
	// moving bytes and (for gRPC) selecting the method from the message type.
	Request(ctx context.Context, req, resp proto.Message) error
	Close() error
}

// Handler is the server-side application logic: given a decoded request it
// fills and returns the response (or an error). The Responder owns decode,
// envelope receive-stamping, optional validation, encode, and send around it.
type Handler func(ctx context.Context, req proto.Message) (proto.Message, error)

// Responder is the server side of a request/response transport: it serves
// Handler until ctx is cancelled. gRPC registers the generated services;
// the buses subscribe to their request subject/queue/stream.
type Responder interface {
	Serve(ctx context.Context, h Handler) error
	Close() error
}

// Release returns a published message's pooled buffer to the pool. When it may
// be called depends on the client library's copy semantics (design §7.5); the
// Publisher hands it back so the caller never guesses. It is a no-op for
// -pool=none and for libraries that copy synchronously.
type Release func()

// Publisher is the client side of a fire-and-forward transport (JetStream,
// quorum, Valkey stream, MQTT). Publish returns once the library owns the bytes
// (synchronously-copying libraries) or, for paho, once it is safe to reuse the
// buffer; the returned Release frees the pooled buffer at that point.
type Publisher interface {
	Publish(ctx context.Context, m proto.Message) (Release, error)
	Close() error
}

// Msg is one delivered payload on the consumer side: the raw bytes plus the
// ack the transport needs. Codec reports which codec the payload was encoded
// with (from the envelope / content-type) so the consumer decodes correctly.
type Msg interface {
	Bytes() []byte
	Ack() error
	Codec() codec.Codec
}

// Consumer is the server side of a fire-and-forward transport: it calls fn for
// every delivered Msg until ctx is cancelled.
type Consumer interface {
	Consume(ctx context.Context, fn func(Msg) error) error
	Close() error
}
