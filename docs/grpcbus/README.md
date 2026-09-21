# The gRPC-native message bus

This is a fifth bus for the collection — a message bus built **only** from gRPC,
using its server-streaming RPCs as the delivery channel. A central broker
(`grpcbrokerd`) accepts published messages and fans each one out to every client
currently subscribed to that message's topic.

It answers a natural question about the [RPC lab](../rpc/README.md): *gRPC has
streaming, so could we make a pub/sub bus out of it?* Yes — but it is a different
shape of thing, and the contrast is the point.

## How it differs from the RPC lab

The RPC lab (`rpc.v1`) is **request/reply**: a caller sends one `Request` and
waits for one `Response`, routed by a gateway to a backend. Its `CallStream` is a
bidi stream, but still one response per request.

The gRPC bus (`bus.v1`) is **publish/subscribe**: a publisher fires a `Message`
and gets back only a count of how many subscribers it reached; it never waits for
a reply. Subscribers are the ones holding a long-lived stream open, and the broker
pushes to them. One publish becomes *N* deliveries (fan-out), or zero if nobody is
listening.

| | RPC lab (`rpc.v1`) | gRPC bus (`bus.v1`) |
|---|---|---|
| Interaction | request → reply | publish → fan-out (no reply) |
| gRPC shape | unary `Call` (+ bidi `CallStream`) | unary `Publish` + **server-stream** `Subscribe` |
| Who streams | either side, 1:1 | broker → each subscriber, 1:N |
| Routing | by `service`/`method` to one backend | by `topic` to all subscribers |
| Payload | typed `Any` envelope | opaque `bytes` + `content_type`/`headers` |

## How it differs from the other four buses

NATS/RabbitMQ/MQTT/ValKey are real brokers with their own wire protocols; the
clients speak each protocol. Here the "broker" is just a gRPC server we wrote
(`grpcbrokerd`) holding an in-memory `topic → subscribers` map, and every client
speaks gRPC. There is no persistence, clustering, or protocol negotiation — it is
the *simplest possible* broker, which makes it a clean reference for what gRPC
streaming alone gives you.

## Delivery semantics (this version)

- **Ephemeral fan-out.** A subscriber receives only messages published while its
  `Subscribe` stream is open — no history, no replay, no acknowledgements. This is
  NATS-core / MQTT-QoS-0 territory, not JetStream/quorum durability.
- **Exact-match topics.** The broker keys subscribers by the literal topic string;
  there are no wildcards or prefixes.
- **Slow subscribers are dropped, not waited on.** Each subscriber owns a bounded
  buffered channel (`-buffer`, default 256). `Publish` does a non-blocking send to
  each and **drops** (counting `grpcbus_messages_dropped_total`) for any subscriber
  whose buffer is full, so one slow consumer can never stall a publisher or the
  broker. A publisher can detect gaps via the per-message `seq`.

Not exactly-once, not durable, not ordered across publishers — deliberately. Those
would be follow-on work (a replay ring buffer, wildcards, auth).

## Running it

The broker is deployed in-cluster (namespace `grpcbus`) on NodePort **30450**; the
`grpcbuscli` pub/sub clients run on the host and connect to it, exactly like the
other buses.

```bash
# Against the deployed broker (point -addr at any node IP, 10.33.33.10-13):
nix run .#grpcbus-sub -- -addr 10.33.33.10:30450 -subject demo -count 5 -json
nix run .#grpcbus-pub -- -addr 10.33.33.10:30450 -subject demo -msg hello -count 5

# Fully offline: run a broker locally on :9450 and point the clients at it.
nix run .#grpcbus-broker &
nix run .#grpcbus-sub -- -addr 127.0.0.1:9450 -subject demo &
nix run .#grpcbus-pub -- -addr 127.0.0.1:9450 -subject demo -msg hi -count 3
```

`grpcbuscli` shares the flag surface of the other bus CLIs (`-subject`, `-count`,
`-rate`, `-timeout`, `-json`), so `-json` emits one `{ts,subject,data}` line per
received message — the same shape as `natscli sub -json`.

## Metrics

With `-metrics-addr` (the deployed broker uses `:9451`) the broker exposes
`grpcbus_*` in Prometheus text format:

| Series | Meaning |
|---|---|
| `grpcbus_messages_published_total` | messages accepted for fan-out |
| `grpcbus_messages_delivered_total` | per-subscriber deliveries (one publish → N) |
| `grpcbus_messages_dropped_total` | deliveries dropped on a full subscriber buffer |
| `grpcbus_active_subscribers` | currently connected `Subscribe` streams |

The label set is intentionally **empty** — topic/publisher/subscriber are never
labels, because free-form topics would make Prometheus cardinality unbounded (the
same rule the RPC lab's `rpc_*` metrics follow).

## Code map

| Piece | Path |
|---|---|
| Wire contract | `clients/proto/bus/v1/bus.proto` |
| Fan-out core (transport-agnostic, unit-tested) | `clients/internal/grpcbus/` |
| Broker server | `clients/grpcbus/grpcbrokerd/` |
| Pub/sub client | `clients/grpcbus/grpcbuscli/` |
| Image | `nix/images/grpc-broker.nix` |
| Deployment | `nix/gitops/env/grpcbus.nix` → `rendered/grpcbus/` |
