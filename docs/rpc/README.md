# RPC lab

The **RPC lab** turns the four clustered message buses (plus gRPC) into one
apples-to-apples request/reply benchmark. Every hop speaks the same tiny API —

```go
rpc.Call(ctx, *rpcv1.Request) (*rpcv1.Response, error)
```

— over one routing envelope (`rpc.v1`, an `Any` payload), so the *only* thing
that changes between a NATS run and a RabbitMQ run is the transport binding
underneath. That is what makes the comparison fair: the schema, the gateway, the
correlation, the retries, the metrics, and the tracing are identical; the wire is
not.

This is the conceptual guide — it answers the ten questions §37 of the
improvement plan poses about RPC over a message bus. It sits **above** the
transport-and-codec measurement track (proto-bench, see
[proto-bench.md](../proto-bench.md)); the two coexist and share the same
`internal/harness` HDR / metrics machinery.

```text
                  application
                       |
                       v
                 RPC interface            rpc.Call(ctx, req) → resp
                       |
             +---------+---------+
             |                   |
          direct RPC         message bus
             |                   |
            gRPC       +----------+----------+
                       |          |          |
                      NATS    RabbitMQ     MQTT
                                  |
                               Valkey
                                  |
                              Redpanda  (planned, §15)
```

## Components

| Piece | Where | What |
|-------|-------|------|
| Schema | `clients/proto/rpc/v1/*.proto` | `rpc.v1` `Request`/`Response`/`Status`, the `Any` payload, and the §20 hop timestamps; generated Go is checked in and drift-guarded by `nix flake check` |
| Core | `clients/internal/rpc` | the `Client`/`Handler` interfaces, the `Correlator`, `Mux`, `StatusOf`, the idempotency cache, and the `RetryClient` / `TracingClient` / `TracingHandler` decorators |
| Transport bindings | `clients/internal/rpc/{grpcx,natsx,rabbitmqx,mqttx,valkeyx}` | one binding per transport, each a drop-in `rpc.Client` + responder |
| `rpc-service` | `clients/rpc-service` | the backend: dispatches `service.method`, validates, dedups by idempotency key; serves gRPC and, optionally, any bus |
| `rpc-gateway` | `clients/rpc-gateway` | gateway-A: a payload-blind proxy that forwards to a backend over a chosen transport |
| `rpc-client` | `clients/rpc-client` | a one-shot caller |
| `rpc-benchmark` | `clients/rpc-benchmark` | the load driver (closed/open loop, size sweep, `rpc_*` metrics, `run.json`) |
| `k8s-rpc-bench` | `nix/rpc-scripts.nix` | host harness: `rpc-benchmark → host gateway-A → in-cluster gateway-B → rpc-service` |
| `k8s-rpc-chaos` | `nix/rpc-chaos-scripts.nix` | §28 failure injection (kill backend / gateway-B / gateway-A mid-run) |
| Dashboard | `nix/gitops/env/monitoring/dashboards/rpc.nix` | Grafana dashboard uid `rpc` |

## Transport cheat-sheet

| Transport | Binding | Reply routing | Correlation | Durable? | No-responder |
|-----------|---------|---------------|-------------|----------|--------------|
| gRPC | `grpcx` | HTTP/2 response | the call itself | no | connect error, fast |
| NATS Core | `natsx` | `_INBOX` reply subject | NATS inbox | no | `ErrNoResponders` → UNAVAILABLE in ms |
| NATS JetStream | `natsx` | Core inbox, reply-over-Core | `request_id` (Correlator) | **yes** | request persists; caller times out |
| RabbitMQ | `rabbitmqx` | reply queue / `amq.rabbitmq.reply-to` | `CorrelationId` | durable request queue | request queued; caller times out |
| MQTT | `mqttx` | `rpc/response/<client_id>` | `request_id` (Correlator) | QoS 1/2 at-least-once | dropped (QoS 0) / queued (retained session) |
| Valkey Pub/Sub | `valkeyx` | `rpc:response:<client_id>` | `request_id` (Correlator) | no | dropped, no delivery |
| Valkey Streams | `valkeyx` | per-client reply stream | `request_id` (Correlator) | **yes** | request persists (XADD); caller times out |

---

## 1. What is RPC over a message bus?

Ordinary RPC is a direct call: the client opens a socket to the server, sends a
request, and blocks for the response on that same connection (gRPC over HTTP/2 is
the canonical form). **RPC over a message bus** keeps the same *shape* — one
request, one correlated response, synchronous from the caller's view — but the
transport is a broker instead of a socket:

1. the caller **publishes** a request message to a subject/queue/topic;
2. the responder is subscribed there, handles it, and **publishes a reply** to a
   reply address the request carried;
3. the caller **correlates** that reply back to its outstanding request and
   returns it.

The broker now owns routing, buffering, fan-out, and (sometimes) durability.
In this lab every transport implements `rpc.Client.Call`, and a **gateway**
(`rpc-gateway`) routes by `service.method` without ever decoding the payload — so
one binary proxies every service, and switching buses is a flag, not a rewrite.

## 2. How does NATS request/reply work?

`natsx` uses NATS's native request/reply. `Call` publishes the marshalled
envelope to `rpc.<service>.<method>` with `nc.RequestWithContext`; NATS allocates
a private `_INBOX.*` reply subject and delivers the single reply there, so **no
application-level correlator is needed** — the inbox *is* the correlation. The
responder `QueueSubscribe`s `rpc.>` in group `rpc-responders`, so multiple
responders load-balance.

The interesting property is **no-responder fast-fail**: if nothing is subscribed,
NATS Core returns `ErrNoResponders` almost immediately (measured ~10–40 ms),
which maps to `STATUS_UNAVAILABLE` — *not* a timeout. The caller learns "there is
no backend" at once, a distinction gRPC (connection refused) shares but the
durable transports deliberately do not (see §8).

## 3. How is the same pattern implemented with RabbitMQ?

`rabbitmqx` publishes the envelope to a **durable** request queue
(`rpc.requests`) on the default exchange, with AMQP's `CorrelationId` set to the
`request_id` and `ReplyTo` set to a reply destination. A dispatch goroutine
correlates each delivery's `CorrelationId` back to the waiting call via the
shared `Correlator`. Two reply strategies are exposed as distinct transports:

- `rabbitmq` — a server-named exclusive, auto-delete **reply queue** per client;
- `rabbitmq-direct` — RabbitMQ's `amq.rabbitmq.reply-to` pseudo-queue (no
  declaration, auto-ack), the closest analogue to a NATS inbox.

RabbitMQ 4.x refuses transient non-exclusive queues, so the request queue *must*
be durable — which also gives the durability contrast in §8.

## 4. How is it implemented with MQTT?

`mqttx` publishes to `rpc/request/<service>/<method>` and subscribes
`rpc/response/<client_id>`. The vendored Paho client speaks MQTT 3.1.1, which has
**no response-topic / correlation-data** properties (those are MQTT 5), so the
reply route travels in the envelope's `client_id` and correlation in the
`request_id` — the envelope carries what the protocol can't. QoS 0/1/2 are
selectable and are *distinct semantics*, not tuning: QoS 0 is at-most-once (loss
is expected), QoS 1/2 are at-least-once/exactly-once-delivery and may redeliver
(deduped by the idempotency cache). The binding sets `OrderMatters(false)` so a
QoS-2 reply published from inside the subscribe callback cannot deadlock the Paho
message pump.

## 5. How is it implemented with Valkey?

`valkeyx` offers two modes that mirror the ephemeral/durable split:

- **Pub/Sub** (like NATS Core): `Call` `PUBLISH`es to `rpc:request:<svc>:<method>`
  and `SUBSCRIBE`s `rpc:response:<client_id>`; the responder `PSUBSCRIBE`s the
  request pattern and publishes the reply to the caller's channel. A request with
  no subscriber is simply dropped — no discovery, no durability.
- **Streams** (like JetStream): `Call` `XADD`s to `rpc:stream:requests`; the
  responder is a durable consumer **group** (`rpc-responders`) that drains
  pending entries then new ones and `XACK`s each; replies go to a per-client
  reply stream read from id `0`.

Both always connect through **Sentinels** to reach the primary — the round-robin
NodePort can land on a read-only replica, and requests/replies are writes.

## 6. Why is Redpanda different?

Redpanda (Kafka-compatible) is **log-structured**, not a queue. Messages go to an
append-only, partitioned, replayable log; consumers track *offsets* rather than
acking individual messages, and records are retained for everyone, not consumed
away by one reader. That makes naive request/reply awkward: there is no
per-request reply subject, so you correlate over a shared response topic by key,
and the log keeps every request and reply. Its strengths are throughput,
per-partition ordering, and replay — the opposite end of the spectrum from a NATS
inbox. It is the planned Kafka-family track (§15) and is called out here so the
comparison is complete; the binding is not yet implemented.

## 7. How does this compare with gRPC?

gRPC (`grpcx`) is the **direct RPC** reference: HTTP/2, no broker, the response
comes back on the same stream, and it uniquely supports client/bidi streaming
(`CallStream`). It fast-fails when the backend is down (connection error) and has
no durability or replay. In this lab gRPC is also the *ingress*: `rpc-gateway`
always presents a gRPC `GatewayService` to callers and chooses a bus only for the
*egress* hop, so "client → gateway → backend" is uniform whether the backend hop
is gRPC or a bus. Practically, gRPC and Valkey Pub/Sub are the low-latency end;
the durable transports trade latency for the guarantees in §8.

## 8. What does durability change?

Durability changes **what happens when the responder is absent or fails
mid-flight** (the §28/§30 question the chaos harness measures):

- **Ephemeral** (NATS Core, Valkey Pub/Sub, MQTT QoS 0): a request with no
  responder is *dropped*. The caller fails fast and knows the work never
  happened. There is nothing to replay.
- **Durable** (NATS JetStream, RabbitMQ, Valkey Streams, MQTT QoS 1/2): the
  request is *persisted* on publish. If no responder is up, the caller still
  times out, but the request survives and is delivered when a responder returns —
  so the operation eventually runs (**at-least-once**). Redelivery is then deduped
  by the service-side idempotency cache so it runs *effectively once*.

The measured contrast: NATS Core answers a no-responder call in ~37 ms, while
JetStream persists the request and the caller times out — same "no backend"
situation, opposite caller experience. Run it yourself with `k8s-rpc-chaos`.

## 9. Why are retries not exactly-once?

Because a lost reply is indistinguishable from a lost request. If a retry re-sends
after the operation already ran (the reply was lost on the way back), the
operation would run twice — so a bare retry is **at-least-once**, never
exactly-once (the two-generals problem; true exactly-once is unattainable across
an unreliable network). The lab gets **effectively-once** by separating two ideas
that are easy to conflate:

- `request_id` — one *attempt* (fresh per retry);
- `idempotency_key` — one *logical operation* (stable across retries).

`RetryClient` retries only transient failures (`TIMEOUT`/`UNAVAILABLE`) and
re-sends under a **new `request_id`** but the **same `idempotency_key`**. The
service keeps an idempotency cache: the first attempt executes and is cached under
the key; any later attempt with that key returns the cached response, tagged
`idempotent-replay`, without re-running the work. So the operation happens once
even though the request was delivered more than once.

## 10. How are the benchmarks run?

The host harness drives the full two-gateway path
(`rpc-benchmark → host gateway-A → in-cluster gateway-B → rpc-service`):

```bash
nix run .#k8s-rpc-bench                         # default run (report, don't assert)
nix run .#k8s-rpc-bench -- --dry-run            # print the plan, offline
nix run .#k8s-rpc-bench -- --sizes default --metrics --out run.json
```

`rpc-benchmark` itself is transport-agnostic behind `rpc.Client`:

- `-mode closed` (concurrency + request budget) or `-mode open` (rate + duration,
  coordinated-omission corrected);
- `-transport grpc | nats | natsjs | rabbitmq | rabbitmq-direct | mqtt | valkey | valkey-stream`;
- `-sizes 100B,1KiB,…,1MiB` for the §26 payload sweep (echo round-trip);
- `-retries N` + `-idempotency-key K` to exercise §29;
- `-metrics-addr :9310` to expose the `rpc_*` series (Prometheus scrape job
  `rpc-benchmark`, Grafana dashboard uid `rpc`);
- `-trace stdout` to emit spans for §23 (enable it on the gateway and service too
  to capture the whole path);
- `-out run.json` for a reproducible record (git rev, host, per-cell summary).

Failure behaviour is a separate harness:

```bash
nix run .#k8s-rpc-chaos -- --scenario backend    # kill rpc-service mid-run
nix run .#k8s-rpc-chaos -- --scenario gateway-b --retries 3
nix run .#k8s-rpc-chaos -- --scenario gateway-a --duration 30s
```

For the distinction between this and the other benchmark tracks
(`clients-bench`, proto-bench), see [benchmarks.md](../benchmarks.md).

## Tracing (§23)

Every hop can emit an OpenTelemetry span; the W3C trace context rides the
envelope `metadata` map, so one trace spans
`client → gateway-A → broker → gateway-B → rpc-service` across *any* transport and
shows where latency was introduced. It is opt-in (`-trace stdout`, default off);
there is no trace backend in the cluster yet, so spans print as JSON to stderr
(`kubectl logs`) — the design is backend-ready (swap the exporter for OTLP once a
Tempo/Jaeger/collector endpoint exists).
