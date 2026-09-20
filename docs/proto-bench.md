# Proto-bench

The **proto-bench** subsystem measures Protobuf/ProtoJSON codec cost, transport
behaviour, and GC/allocation pressure for a managed container-workload scenario
across the 4-node MicroVM cluster — gRPC plus the four message buses (NATS,
RabbitMQ, Valkey, MQTT), each codec (`proto`, `protojson`, `vtproto`), and a
`sync.Pool` allocation strategy.

This is the operator guide. The full specification — the `workloads.v1` schema
field tables, the pooling internals, the metrics catalogue, and the acceptance
criteria — lives in
[protobuf-grpc-benchmark-design.md](protobuf-grpc-benchmark-design.md); this doc
points into it rather than repeating it.

It is the **third** benchmark track and is deliberately named to avoid colliding
with the other two (see [benchmarks.md](benchmarks.md)): its code lives under
`clients/workloads/`, the harness is `k8s-proto-bench`, and logs go to
`proto-bench-logs/` (vs. `clients-bench` / `k8s-client-bench` / `bench-logs/`).

## Components

| Piece | Where | What |
|-------|-------|------|
| Schema | `clients/proto/workloads/v1/*.proto` | `workloads.v1` messages + `BenchService` gRPC; generated Go (`clients/gen/`) is checked in, drift-guarded by `nix flake check` |
| Codec / pool library | `clients/internal/{codec,pool,envelope,corpus,harness}` | codecs (incl. vtproto), pooled buffers, the request envelope + timestamps, the payload fixtures, and the `mbbench_*` instruments |
| `region-agent` | `clients/workloads/region-agent` | the server: one Deployment per region (gRPC) and bus responder/consumer; Nix-built image, deployed by `nix/gitops/env/workloads.nix` |
| `benchcli` | `clients/workloads/benchcli` | the driver + offline tools (codec loop, correctness, clockprobe, report) |
| `k8s-proto-bench` | `nix/proto-bench-scripts.nix` | the host harness that drives `benchcli` across the agents and assembles the run |
| Dashboard | `nix/gitops/env/monitoring/dashboards/protobench.nix` | Grafana dashboard uid `protobench` (its own ConfigMap) |

## Quick start

The cluster must be up with the monitoring stack and region-agents deployed, and
you need SSH to cp0 for the bus credentials (handled by the `k8s-vm-ssh`
wrapper). Then:

```bash
nix run .#k8s-proto-bench                         # curated default matrix
nix run .#k8s-proto-bench -- --dry-run            # print the plan and exit (offline)
nix run .#k8s-proto-bench -- \
  --transports grpc,nats --codecs proto,vtproto \
  --pools none,all --modes latency,openloop --repeats 3
```

The default matrix (`grpc,nats` × `proto,vtproto` × `small,medium` × `none,all`,
`gc=default`, `mode=latency`, 5 repeats) is curated to finish inside the ~90-min
budget. `--dry-run` builds and prints the matrix **before** any cluster contact,
so it is the fast way to check what a set of flags will run.

The harness follows a strict **"report, don't assert"** rule: an unreachable
Prometheus, a missing credential, or an errored cell yields blank columns — it
never aborts the run.

## Matrix axes

Each is a comma-separated subset (defaults in `nix/constants.nix`
`protoBench.defaults` and the harness header):

- `--transports` `grpc,nats,rabbitmq,valkey,mqtt`
- `--codecs` `proto,protojson,vtproto`
- `--fixtures` `tiny,small,medium,large,…` (the corpus; see design §3.8)
- `--pools` `none,messages,buffers,all`
- `--gc` `default,limit` (the harness patches the driven agent's `GOGC`/`GOMEMLIMIT` per profile)
- `--modes` see below
- `--regions` region label(s); **cells drive the FIRST region, all listed regions are clock-probed**

Run knobs: `--rate` (`2000/s`, openloop/saturation), `--duration` (`30s`,
open-loop budget), `--count` (`10000`, closed-loop), `--inflight` (`64`,
windowed/openloop cap), `--warmup` (`5s`, settle after each GC rollout),
`--repeats` (`5`), `--order-seed` (`7`, cell-shuffle seed), `--cpuset` (taskset
core list for the driver), `--integrity` (`none|sha256`), `--log-dir`
(`./proto-bench-logs`).

## Run modes

Every request/reply transport supports every mode except where noted; the
one-way telemetry drivers (`mqtt` fire-and-forget, `jetstream`/`quorum` durable)
are latency-only. See design §8.2/§8.4.

| Mode | Shape | Headline output |
|------|-------|-----------------|
| `latency` | closed loop, inflight 1 | RTT percentiles (the latency floor) |
| `windowed` | closed loop, `-inflight` N workers | throughput at a fixed concurrency |
| `openloop` | fixed `-rate`, coordinated-omission-free | RTT at an offered rate + `late_sends` |
| `saturation` | rate-doubling ramp every `-step` until p99 > 10×floor or late > 1% | the **knee** (last sustainable rate), not the failing peak |
| `coldstart` | `-conns` fresh connections, first-request RTT each | connection-setup latency |
| `fault` | open-loop rate held straight through a **pod kill** | `missing` = requests that got no valid reply |

**Fault mode** is opt-in (only when `fault` is in `--modes`). For a fault cell
the harness runs `benchcli` in the background, kills the target pod ~1/3 into the
window (the driven region's agent for gRPC; the broker's `pod-0` for a bus),
lets the driver keep sending — it never aborts — then waits for the driver to
finish and for the target to recover before the next repeat. Every lost request
becomes the `missing` integrity counter. In unary request/reply only loss is
observable, so `dup`/`reord`/`redeliv` stay 0 (streaming-tier counters); the
report shows what it can measure.

## `benchcli` subcommands

`benchcli` is the driver the harness calls, and also a set of offline tools:

- `codec` — the offline codec/pool micro-loop (marshal/unmarshal cost, allocs).
- `grpc` / `nats` / `rabbitmq` / `valkey` / `mqtt` — the transport drivers (the
  harness invokes these; each takes `-mode`, `-codec`, `-fixture`, `-pool`,
  `-rate`/`-duration`/`-count`/`-inflight`, `-out`/`-hgrm`, …).
- `jetstream` — the NATS JetStream **durable telemetry** driver (tier B): a
  one-way `publish→persist-ack` loop on `wl.<region>.telemetry`, backed by the
  R3 file stream `WL_TELEMETRY`. Each publish blocks until JetStream acks the
  message as replicated, so its latency is the durable-write cost (contrast
  `mqtt`'s fire-and-forget). Delivery + redelivery are consumer-side: the
  region-agent runs a durable pull consumer per region, acks every message, and
  records `mbbench_messages_total{role="server",result="redelivered"}` for any
  `NumDelivered > 1` (design §2.3 tier B, §3.9).
- `quorum` — the RabbitMQ **quorum-queue durable telemetry** driver (tier B): a
  one-way `publish→confirm` loop to the durable quorum queue
  `wl.telemetry.<region>` (`x-queue-type: quorum`), persistent delivery +
  publisher confirms. Each publish blocks until the broker confirms the message
  committed to the quorum (Raft-replicated across the RabbitMQ nodes). The
  region-agent consumes its own region's queue with manual ack, acks every
  message, and records `result="redelivered"` for the AMQP redelivered flag
  (design §2.3 tier B, §3.9).
- `correctness` — the §9.4 pass: in-process `proto.Equal` per codec×fixture,
  corpus determinism, protovalidate valid/invalid, and (over `-transport`) a
  live round trip asserting the `message_id` echo.
- `clockprobe` — NTP four-timestamp probe over `BenchService.ClockProbe`
  (`-json` for the harness); never clamps.
- `report` — reduce a `run.json` to `results.tsv` / `results.md` / `hgrm/`.

## Outputs

A run writes a timestamped directory under `--log-dir`:

- `run.json` — the full run record (schema in `clients/internal/runrecord`,
  design §8.5): matrix, per-cell summaries, clock probes, corpus + memstats.
  Flat JSON tags so the shell harness assembles it incrementally with `jq`
  (crash-safe).
- `results.tsv` — a flat dump of every numeric column, one row per cell.
- `results.md` — human-readable: a **Clock** table, then per-tier → per-mode
  tables (the columns vary by mode, e.g. fault leads with
  `msgs/missing/dup/reord/redeliv`), then a **Scorecard** aggregating repeats
  (median ± MAD, flagged `unstable` when the headline metric's MAD > 20 %).
- `hgrm/<cell>.hgrm` — standard HdrHistogram percentile distributions (µs).

## Grafana dashboard and annotations

The **Proto-bench** dashboard (Grafana uid `protobench`, NodePort 30300) has
panels for transport RTT p50/p99, server-side + codec-only time, heap-allocation
rate, allocated-objects-per-message (the pooling proof), GC-pause p99, `GOGC` by
job, and integrity/throughput (messages by result, errors by kind, wire bytes by
direction). Series come from `mbbench_*` (labels
`transport,codec,fixture,pool,gc,mode,region,role,tier`; `job=workloads-agents`
for agents, `job=proto-bench-driver` for the host driver) and the Go runtime
collectors (`go_gc_*`).

The harness posts Grafana annotations bound to this uid: a region annotation per
cell×repeat, and a point annotation at each fault kill (tag `fault`). The
dashboard's built-in annotation query (tag `protobench`) renders them as
markers. Because a POST to a missing `dashboardUID` returns HTTP 500, those
annotations only land once this dashboard exists — it is part of the monitoring
GitOps module and syncs with the rest of the stack.

## Clocks

One-way latency needs µs-quality clocks. The VMs run `chrony` disciplined to the
KVM PTP refclock (`/dev/ptp0`, `ptp_kvm`) — see `nix/k8s-module.nix` and design
§8.1. `benchcli clockprobe` records each region's offset/uncertainty into
`run.json.clock`; when the clock gate is not met the harness still reports RTT
(monotonic, always valid) and simply omits the one-way figure.

## Cluster access

The host reaches the cluster only via the VM IPs `10.33.33.10-13`. gRPC uses
per-region NodePorts with `externalTrafficPolicy: Local`, so a gRPC cell must
hit the region's **own** node; bus NodePorts use the Cluster policy, so bus
cells hit cp0 and select the responder with `-region`. The harness derives the
right endpoint per cell — you do not pass IPs. For manual probing, reach
Prometheus at `10.33.33.10:30900` and Grafana at `10.33.33.10:30300`.

## Regenerating the schema

Generated Go is checked in and guarded by `nix flake check`
(`proto-lint`/`proto-breaking`/`proto-gen-drift`). To change the schema, edit the
`.proto` files and:

```bash
nix run .#regen-protos          # buf generate into clients/gen/ (impure, host-side)
nix flake check                 # gates incl. the gen-drift diff
```

On a clean tree `regen-protos` is a no-op. After changing any monitoring GitOps
source (the dashboard included) run `nix run .#k8s-render-manifests` and commit
`nix/` + `rendered/` together, as with the buses.
