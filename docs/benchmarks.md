# Benchmarks

Two complementary ways to benchmark the message-bus clients:

1. **Hermetic Go micro-benchmarks** (`nix run .#clients-bench`) — `testing.B`
   benchmarks over the shared per-message hot paths in `clients/internal/{cli,
   metrics}`. No cluster; fresh ns/op + allocs/op each run.
2. **Live-bus throughput/latency harness** (`nix run .#k8s-client-bench`) — for
   each bus, publish N messages unthrottled to a live subscriber, time the run,
   and read consume rate + latency percentiles from the client OTel metrics via
   Prometheus.

A third track — Protobuf/ProtoJSON codecs over gRPC and the buses with a
`sync.Pool` allocation strategy — is specified in
[protobuf-grpc-benchmark-design.md](protobuf-grpc-benchmark-design.md) and driven
by the `k8s-proto-bench` harness; see [proto-bench.md](proto-bench.md) for the
operator guide. It uses the `proto-bench` / `workloads` names so it does not
collide with the two above.

## Micro-benchmarks

```bash
nix run .#clients-bench                                  # all, default benchtime
nix run .#clients-bench -- -bench BenchmarkPubLoop -benchtime 2s
```

It copies the client module + the same vendored deps the build uses into a temp
dir and runs `go test -bench=. -benchmem -run '^$' ./...` there, so it is
offline and reproducible. The benchmarks are **compile-checked** by `nix flake
check` (they build under `go test ./...`) but **not executed** there — timing is
noisy and machine-dependent, so run them on one host and compare deltas.

Covered hot paths:

| Benchmark | Path | What it isolates |
|-----------|------|------------------|
| `BenchmarkPubLoop` / `…Single` | `cli.PubLoop` | per-message body construction (multi vs single) |
| `BenchmarkEmitText` / `…JSON` | `cli.Emit` | subscriber render (`time.Format`, `json.Marshal`) |
| `BenchmarkParseSeq` | `metrics.ParseSeq` | trailing-sequence extraction |
| `BenchmarkGapObserve` | `metrics.GapTracker.Observe` | mutex-guarded gap accounting |
| `BenchmarkRecordReceived` | `metrics.RecordReceived` | full per-message subscriber wrapper (live + nop) |
| `BenchmarkIncPublished` | `metrics.IncPublished` | per-message counter `Add` (live + nop) |

## Live-bus harness

```bash
nix run .#k8s-client-bench                               # all four buses, 200k msgs each
nix run .#k8s-client-bench -- --count 500000 --buses nats,rabbitmq
nix run .#k8s-client-bench -- --msg-size 256
```

Publish throughput is `count / wall-clock publish time`; consume rate
(`mbclient_received_total`) and latency p50/p99
(`mbclient_request_latency_seconds`) come from Prometheus, queried from the cp0
host over its NodePort. Results land in `bench-logs/bench.md` + `bench.tsv`, one
row per bus. NATS is benchmarked in **core** (fire-and-forget) mode — the pure
publish path where the flush change below applies. Needs the cluster up with the
monitoring stack and SSH to cp0 for credentials.

### Reference run (4-node MicroVM cluster, unthrottled)

Absolute numbers are cluster/hardware-specific — the point is the shape of each
bus's publish path, not the exact rate.

| bus | mode | count | publish msgs/s | received | p50 / p99 confirm |
|-----|------|------:|---------------:|---------:|------------------:|
| nats | core | 200000 | **1,036,269** | — | — |
| rabbitmq | durable | 200000 | **850** | 200000 | 2500 / 4950 ms |
| valkey | sentinel | 200000 | **6,130** | 171704 | — |
| mqtt | core | 5000 | **65** | 154191 | — |

- **NATS** publishes the whole 200k batch in ~0.19s — the single end-of-loop
  `nc.Flush()` (below) turns per-message round-trips into one. `received` is
  blank only because the subscriber lived far shorter than a Prometheus scrape
  interval, so it was never scraped; not a delivery loss.
- **RabbitMQ**'s synchronous per-message confirm caps throughput and, under an
  unthrottled backlog, drives confirm latency to seconds — the concrete
  before-numbers for the deferred confirm-pipelining change (below).
- **Valkey** pub/sub is fire-and-forget; the subscriber dropped ~14% under
  unthrottled load (171704/200000).

> **MQTT is benchmarked at a lower `--count`.** Against the 3-broker **bridged**
> Mosquitto, unthrottled QoS-1 publishing waits a PUBACK per message *and* the
> bridge fans every message out ~30× (154191 received for 5000 published),
> saturating the brokers — a full 200k run projects to hours. This is a property
> of the bridged QoS-1 topology, not a client hot-path. For a mixed run prefer
> `--buses nats,rabbitmq,valkey` (or give MQTT a small `--count`); the default
> `--count` is tuned for the other three.

## Findings & optimizations applied

Ranked from the micro-benchmarks (AMD Ryzen Threadripper PRO 3945WX; absolute
numbers are machine-specific — compare the before/after deltas):

| Path | Before | After | Change |
|------|--------|-------|--------|
| `PubLoop` (per 1000 msgs) | 165453 ns, 2748 allocs, 37 KB | **45674 ns, 1005 allocs, 15 KB** | reused buffer + `strconv.AppendInt` instead of per-message `fmt.Sprintf` |
| `ParseSeq` | 79 ns, 1 alloc, 32 B | **67 ns, 0 allocs, 0 B** | trailing-token scan (`TrimRightFunc`/`LastIndexFunc`) instead of `strings.Fields` (which allocates a slice) |

Both are on every-message paths (`PubLoop` per publish; `ParseSeq` per received
message that carries a sequence) and preserve behavior — the existing
table-driven tests, including every boundary/corner row, still pass.

**natscli core publish — flush batching.** `runCore` previously called
`nc.Flush()` after every `nc.Publish`, forcing a server round-trip per message
and capping throughput at the connection RTT. Core NATS is fire-and-forget, so
the client now publishes the whole bounded loop and flushes **once** at the end
(guaranteeing the batch reached the server before `Drain`). This is the standard
NATS batching idiom; verify the throughput gain with `nix run
.#k8s-client-bench -- --buses nats`.

Left as-is (measured, not worth changing):

- **`Emit` render** (`time.Format` + `json.Marshal`, ~4–5 allocs) — the
  subscriber *output* path, not the publish hot path; low volume relative to the
  bus round-trip. `Emit` now writes to an injected `io.Writer` (stdout by
  default) so it can be benchmarked/asserted, but the formatting is unchanged.
- **`GapTracker.Observe`** — already 0-alloc; the mutex is not a bottleneck at
  realistic rates.
- **`RecordReceived`/`IncPublished` live cost** (~260–320 ns, 1 alloc) — that
  cost is inside the OTel SDK's counter `Add`, only paid when `-metrics-addr` is
  set, and identical to the `Nop{}` no-op otherwise.

**Deferred (semantic change):** `rabbitmqcli` publishes with a synchronous
confirm (`…Wait()`) per message. Pipelining deferred confirmations would raise
throughput but changes confirm/latency accounting and the failure semantics the
chaos/soak harnesses rely on — a separate change, not a trivial win.
