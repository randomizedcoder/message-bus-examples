# Protobuf / gRPC / message-bus benchmark — design

> Status: **design, not yet implemented**. Supersedes the draft
> `protobuf-grpc-message-bus-benchmark-design.md` that used a generic
> "order event" schema. Implementation is phased (see [§13](#13-implementation-phases)).

## 1. Purpose

This subsystem demonstrates and measures, on the existing cluster in this
repo:

- **Protobuf schema features** — scalars, enums, nested messages, repeated
  fields, maps, `optional` presence, `oneof`, well-known types (`Timestamp`,
  `Duration`, `FieldMask`), `bytes`, and **Protovalidate** rules including
  message-level CEL expressions.
- **Generated Go code** — `protoc-gen-go`, `protoc-gen-go-grpc`, and an
  opt-in `protoc-gen-go-vtproto` fast path, all generated offline by
  Nix-pinned plugins through `buf`.
- **Codec cost** — binary Protobuf (`proto.Marshal`) versus ProtoJSON
  (`protojson.Marshal`) on the identical generated message instances:
  bytes on the wire, ns/op, allocations, and **CPU and GC cost on every
  party** (client, server, broker).
- **Transport behaviour** — the same message over gRPC unary / server-stream /
  client-stream / bidi, NATS request-reply and JetStream, RabbitMQ RPC and
  quorum queues, Valkey Streams, and MQTT (telemetry only): RTT, one-way
  latency, throughput, backpressure, loss / duplicate / reorder behaviour,
  and recovery under a node kill.
- **Low memory pressure by design** — `sync.Pool` for messages and byte
  buffers at both ends, with metrics that prove the effect
  ([§7](#7-syncpool-strategy-for-low-memory-pressure)).

Everything is built and run the way the rest of the repo is: Nix-pinned
toolchain, Nix-built OCI images preloaded into containerd, GitOps-rendered
manifests, `nix run .#…` harnesses on the host, Prometheus + Grafana in the
cluster, and table-driven tests gated by `nix flake check`.

## 2. Scenario: a managed container-workload service

The schema models a **managed service that hosts customers' OCI container
workloads on globally distributed Kubernetes clusters**. It has three planes,
each of which maps naturally onto a different messaging pattern:

| Plane | Direction | Shape | Realistic transport candidates |
|---|---|---|---|
| **Control** — deploy / update / delete a workload | customer → global API → one regional cluster | request/response, must be routed to the right region, idempotent | gRPC unary or bidi; NATS request-reply; RabbitMQ RPC; Valkey stream request/reply |
| **Telemetry + billing** — container operational state and metered usage | regional cluster agent → global | high-volume one-way, at-least-once, batched, replayable | gRPC client-stream; JetStream; RabbitMQ quorum queue; Valkey Streams; MQTT |
| **Logs** — customers tail their container logs | regional Go log service → customer | long-lived server push, backpressure-sensitive, large chunks | gRPC server-stream; JetStream fan-out |

### 2.1 Lab topology

The four MicroVMs play four **regions**; each node hosts one `region-agent`
pod (the regional cluster's agent + log service + bus responders). The host
`benchcli` plays the customer and the global API.

```
Host (benchcli, HDR histograms, /metrics :9300-9307)
  │ NodePorts
  ├─ cp0  10.33.33.10  region us-west-2   region-agent  gRPC :30710  (+ bench driver's stable endpoint)
  ├─ cp1  10.33.33.11  region us-east-1   region-agent  gRPC :30711
  ├─ cp2  10.33.33.12  region eu-west-1   region-agent  gRPC :30712
  └─ w3   10.33.33.13  region ap-south-1  region-agent  gRPC :30713
       + the existing NATS / RabbitMQ / Mosquitto / Valkey StatefulSets
       + Prometheus :30900, Grafana :30300 (pinned to cp0)
```

Each `region-agent` is deployed with `nodeSelector: kubernetes.io/hostname:
k8s-<node>` and a `REGION` env var; its NodePort Service uses
`externalTrafficPolicy: Local` so a request to `10.33.33.11:30711` is served by
the `us-east-1` agent and nothing else. The bus transports address a region by
subject / routing key / stream name (see [§3.9](#39-subjects-queues-and-topics)).

### 2.2 Flow × transport matrix

| Flow | gRPC | NATS | RabbitMQ | Valkey | MQTT |
|---|---|---|---|---|---|
| Deploy request / response | `Deploy` unary, `DeployStream` bidi | request-reply on `wl.<region>.deploy` (queue group) | RPC: reply queue + `correlation_id` | `XADD` to `wl:<region>:deploy`, consumer group, reply stream | — |
| Telemetry / usage records | `ReportTelemetry` / `ReportUsage` client-stream | JetStream `WL_TELEMETRY` (durable pull consumer) | quorum queue + publisher confirms + manual ack | stream + consumer group + `XACK` | QoS 0 / QoS 1 publish, **one-way only** |
| Container logs | `StreamLogs` server-stream, `FetchLogs` unary | JetStream subject per workload (fan-out profile) | — | — | — |
| Codec floor / clock | `BenchService.Ping`, `ClockProbe` | ping subject | — | — | — |

Every cell of the matrix uses the **same generated Go types, the same
validation, the same fixture bytes, and the same envelope**, so differences
are attributable to codec and transport rather than to application logic.

### 2.3 Delivery-semantics tiers

Results are only ranked *within* a tier; across tiers the report shows the
numbers side by side with the semantics column, never a single ranking.

| Tier | Members | Guarantee | What "loss" means in the report |
|---|---|---|---|
| **A — at-most-once** | core NATS pub/sub, MQTT QoS 0 | fire-and-forget | expected under disruption; reported as data |
| **B — at-least-once, persistent** | JetStream (R3, file), RabbitMQ quorum (confirms + manual ack), Valkey Streams (consumer group + `XACK`), MQTT QoS 1 | acked and replicated before ack | must be ~0; duplicates/redeliveries are counted |
| **C — RPC** | gRPC (all four forms), NATS request-reply, RabbitMQ RPC, Valkey stream request/reply | correlated response or a timeout | timeouts/errors counted; nothing is "lost" silently |

## 3. Protobuf schema (`clients/proto/workloads/v1/`)

Package `workloads.v1`, proto3, generated with the **open struct** Go API
(vtprotobuf 0.6 does not support the opaque API yet). Every RPC request and
response, and every bus payload, carries `Envelope envelope = 1`
([§3.7](#37-benchproto-full-text)); there is no separate bus frame — the
subject / queue / topic identifies the message type.

```
go_package = "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1;workloadsv1"
```

Import graph (acyclic):

```
common.proto ─┐
bench.proto  ─┼─▶ telemetry.proto ─┐
              ├─▶ billing.proto    │
              ├─▶ logs.proto       │
              └─▶ workload.proto ◀─┘   (workload imports telemetry for ContainerState)
```

Field-number policy: 1 = envelope, 2–15 = hot fields (1-byte tags), 16+ =
cold / optional fields; renumbered fields are `reserved` by number and name.
Closed sets are enums; open sets (regions, cluster ids) are validated strings.

### 3.1 `common.proto`

| Message / enum | Fields (number: type) | Protovalidate rules |
|---|---|---|
| `enum Protocol` | 0 UNSPECIFIED, 1 TCP, 2 UDP | `enum.defined_only` at use sites |
| `Tenant` | 1 `string customer_id`, 2 `string project_id` | both `string.uuid = true` |
| `ClusterRef` | 1 `string region`, 2 `string cluster_id` | region `pattern: "^[a-z]{2}-[a-z]+-[0-9]$"`; cluster_id `min_len: 1, max_len: 63` |
| `ResourceQuantity` | 1 `int64 cpu_millis`, 2 `int64 memory_bytes`, 3 `int64 ephemeral_storage_bytes` | each `int64.gte = 0`; cpu `lte = 256000` |
| `Resources` | 1 `ResourceQuantity requests`, 2 `ResourceQuantity limits` | message CEL `resources.limits_ge_requests` (below) |
| `SecretRef` | 1 `string name`, 2 `string key` | DNS-1123 pattern, `max_len: 253` |
| `EnvVar` | 1 `string name`; `oneof value { 2 string literal; 3 SecretRef secret; }` | name `pattern: "^[A-Za-z_][A-Za-z0-9_]*$"`; oneof `required = true`; literal `max_len: 32768` |
| `Port` | 1 `string name`, 2 `uint32 container_port`, 3 `Protocol protocol` | port `uint32 = {gte: 1, lte: 65535}` |
| `Problem` | 1 `string code`, 2 `string message`, 3 `map<string,string> details` | code `pattern: "^[A-Z_]{3,64}$"`; map `max_pairs: 16` |

```proto
message Resources {
  ResourceQuantity requests = 1;
  ResourceQuantity limits = 2;
  option (buf.validate.message).cel = {
    id: "resources.limits_ge_requests"
    message: "limits must be >= requests for cpu and memory"
    expression: "!has(this.limits) || !has(this.requests) || (this.limits.cpu_millis >= this.requests.cpu_millis && this.limits.memory_bytes >= this.requests.memory_bytes)"
  };
}
```

### 3.2 `workload.proto` (full text)

```proto
syntax = "proto3";

package workloads.v1;

import "buf/validate/validate.proto";
import "google/protobuf/duration.proto";
import "google/protobuf/field_mask.proto";
import "google/protobuf/timestamp.proto";
import "workloads/v1/bench.proto";
import "workloads/v1/common.proto";
import "workloads/v1/telemetry.proto";

option go_package = "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1;workloadsv1";

// Closed sets are enums; open sets (regions, cluster ids) are validated strings.
enum WorkloadKind {
  WORKLOAD_KIND_UNSPECIFIED = 0;
  WORKLOAD_KIND_SERVICE = 1; // long-running, replicas >= 1
  WORKLOAD_KIND_JOB = 2;     // run to completion
  WORKLOAD_KIND_CRON = 3;    // JOB on a schedule
}

enum RestartPolicy {
  RESTART_POLICY_UNSPECIFIED = 0;
  RESTART_POLICY_ALWAYS = 1;
  RESTART_POLICY_ON_FAILURE = 2;
  RESTART_POLICY_NEVER = 3;
}

enum DeployStatus {
  DEPLOY_STATUS_UNSPECIFIED = 0;
  DEPLOY_STATUS_ACCEPTED = 1; // validated + persisted by the global API
  DEPLOY_STATUS_ROUTED = 2;   // handed to a regional agent
  DEPLOY_STATUS_APPLIED = 3;  // agent applied it to its cluster
  DEPLOY_STATUS_REJECTED = 4;
}

enum WorkloadPhase {
  WORKLOAD_PHASE_UNSPECIFIED = 0;
  WORKLOAD_PHASE_PENDING = 1;
  WORKLOAD_PHASE_PROGRESSING = 2;
  WORKLOAD_PHASE_AVAILABLE = 3;
  WORKLOAD_PHASE_DEGRADED = 4;
  WORKLOAD_PHASE_COMPLETED = 5;
  WORKLOAD_PHASE_FAILED = 6;
  WORKLOAD_PHASE_DELETED = 7;
}

// oneof: image pinned by a mutable tag or by an immutable raw sha256 digest.
message ImageSource {
  string repository = 1 [(buf.validate.field).string = {
    min_len: 1
    max_len: 255
    pattern: "^[a-z0-9][a-z0-9._/-]*$"
  }];
  oneof ref {
    option (buf.validate.oneof).required = true;
    string tag = 2 [(buf.validate.field).string = {
      min_len: 1
      max_len: 128
      pattern: "^[A-Za-z0-9_][A-Za-z0-9._-]*$"
    }];
    bytes digest = 3 [(buf.validate.field).bytes.len = 32];
  }
}

message ContainerSpec {
  string name = 1 [(buf.validate.field).string = {
    min_len: 1
    max_len: 63
    pattern: "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"
  }];
  ImageSource image = 2 [(buf.validate.field).required = true];
  repeated string command = 3 [(buf.validate.field).repeated.max_items = 32];
  repeated string args = 4 [(buf.validate.field).repeated.max_items = 64];
  repeated EnvVar env = 5 [(buf.validate.field).repeated.max_items = 128];
  repeated Port ports = 6 [(buf.validate.field).repeated.max_items = 16];
  Resources resources = 7 [(buf.validate.field).required = true];
  optional string working_dir = 8 [(buf.validate.field).string = {
    max_len: 4096
    prefix: "/"
  }];
  option (buf.validate.message).cel = {
    id: "container_spec.port_names_unique"
    message: "port names must be unique"
    expression: "this.ports.map(p, p.name).unique()"
  };
}

message RegionAffinity {
  repeated string preferred = 1 [(buf.validate.field).repeated = {
    min_items: 1
    max_items: 8
    unique: true
    items: {string: {pattern: "^[a-z]{2}-[a-z]+-[0-9]$"}}
  }];
  repeated string excluded = 2 [(buf.validate.field).repeated = {
    max_items: 8
    unique: true
    items: {string: {pattern: "^[a-z]{2}-[a-z]+-[0-9]$"}}
  }];
  bool strict = 3; // true: fail rather than fall back outside `preferred`
  option (buf.validate.message).cel = {
    id: "region_affinity.disjoint"
    message: "preferred and excluded must not overlap"
    expression: "!this.preferred.exists(r, r in this.excluded)"
  };
}

message LatencyTarget {
  string from_region = 1 [(buf.validate.field).string.pattern = "^[a-z]{2}-[a-z]+-[0-9]$"];
  google.protobuf.Duration max_rtt = 2 [(buf.validate.field).duration = {
    gt: {seconds: 0}
    lte: {seconds: 5}
  }];
}

// oneof placement policy: exactly one of three shapes.
message Placement {
  oneof policy {
    option (buf.validate.oneof).required = true;
    ClusterRef cluster = 1;
    RegionAffinity region_affinity = 2;
    LatencyTarget latency_target = 3;
  }
}

message WorkloadSpec {
  WorkloadKind kind = 1 [(buf.validate.field).enum = {
    defined_only: true
    not_in: [0]
  }];
  repeated ContainerSpec containers = 2 [(buf.validate.field).repeated = {
    min_items: 1
    max_items: 16
  }];
  uint32 replicas = 3 [(buf.validate.field).uint32.lte = 1000];
  RestartPolicy restart_policy = 4 [(buf.validate.field).enum.defined_only = true];
  Placement placement = 5 [(buf.validate.field).required = true];
  map<string, string> labels = 6 [(buf.validate.field).map = {
    max_pairs: 64
    keys: {string: {min_len: 1, max_len: 63}}
    values: {string: {max_len: 63}}
  }];
  map<string, string> annotations = 7 [(buf.validate.field).map = {
    max_pairs: 64
    values: {string: {max_len: 4096}}
  }];
  optional google.protobuf.Duration ttl_after_finished = 8
      [(buf.validate.field).duration.lte = {seconds: 604800}];
  optional string schedule = 9 [(buf.validate.field).string.max_len = 128]; // cron expression
  option (buf.validate.message).cel = {
    id: "workload_spec.container_names_unique"
    message: "container names must be unique"
    expression: "this.containers.map(c, c.name).unique()"
  };
  option (buf.validate.message).cel = {
    id: "workload_spec.schedule_iff_cron"
    message: "schedule is required for CRON and forbidden otherwise"
    expression: "(this.kind == 3) == has(this.schedule)"
  };
  option (buf.validate.message).cel = {
    id: "workload_spec.service_replicas"
    message: "SERVICE workloads need replicas >= 1"
    expression: "this.kind != 1 || this.replicas >= 1"
  };
}

message WorkloadRef {
  Tenant tenant = 1 [(buf.validate.field).required = true];
  string workload_id = 2 [(buf.validate.field).string.uuid = true];
}

// FieldMask earns its place: "scale to N replicas" is the most common update
// and must not resend / replace the whole spec. It also exercises the WKT's
// special ProtoJSON form ("replicas,labels").
message UpdateWorkload {
  WorkloadSpec spec = 1 [(buf.validate.field).required = true];
  google.protobuf.FieldMask update_mask = 2; // empty = full replace
  option (buf.validate.message).cel = {
    id: "update_workload.mask_paths"
    message: "update_mask paths must be top-level WorkloadSpec fields"
    expression: "!has(this.update_mask) || this.update_mask.paths.all(p, p in ['kind', 'containers', 'replicas', 'restart_policy', 'placement', 'labels', 'annotations', 'ttl_after_finished', 'schedule'])"
  };
}

message DeleteWorkload {
  bool force = 1;
  optional google.protobuf.Duration grace_period = 2 [(buf.validate.field).duration = {
    gte: {seconds: 0}
    lte: {seconds: 3600}
  }];
}

message DeployRequest {
  Envelope envelope = 1 [(buf.validate.field).required = true];
  WorkloadRef ref = 2 [(buf.validate.field).required = true];
  oneof op {
    option (buf.validate.oneof).required = true;
    WorkloadSpec create = 3;
    UpdateWorkload update = 4;
    DeleteWorkload delete = 5;
  }
  string idempotency_key = 6 [(buf.validate.field).string.uuid = true];
  google.protobuf.Timestamp requested_at = 7 [(buf.validate.field).required = true];
}

message DeployResponse {
  Envelope envelope = 1 [(buf.validate.field).required = true];
  WorkloadRef ref = 2;
  DeployStatus status = 3 [(buf.validate.field).enum = {
    defined_only: true
    not_in: [0]
  }];
  ClusterRef assigned_cluster = 4; // set once ROUTED / APPLIED
  uint64 generation = 5;
  repeated Problem problems = 6 [(buf.validate.field).repeated.max_items = 32];
  option (buf.validate.message).cel = {
    id: "deploy_response.cluster_when_routed"
    message: "assigned_cluster is required when status is ROUTED or APPLIED"
    expression: "!(this.status in [2, 3]) || has(this.assigned_cluster)"
  };
  option (buf.validate.message).cel = {
    id: "deploy_response.problems_when_rejected"
    message: "REJECTED responses must carry at least one problem"
    expression: "this.status != 4 || this.problems.size() > 0"
  };
}

message WatchWorkloadRequest {
  Envelope envelope = 1 [(buf.validate.field).required = true];
  WorkloadRef ref = 2 [(buf.validate.field).required = true];
  optional uint64 since_generation = 3;
  // Bench knob: bound the server stream (unset = until cancelled).
  optional uint32 max_events = 4 [(buf.validate.field).uint32 = {
    gte: 1
    lte: 10000000
  }];
}

message WorkloadEvent {
  Envelope envelope = 1 [(buf.validate.field).required = true];
  WorkloadRef ref = 2;
  ClusterRef cluster = 3;
  uint64 generation = 4;
  WorkloadPhase phase = 5 [(buf.validate.field).enum.defined_only = true];
  google.protobuf.Timestamp observed_at = 6;
  repeated ContainerState containers = 7 [(buf.validate.field).repeated.max_items = 16];
  optional string reason = 8 [(buf.validate.field).string.max_len = 1024];
}

service WorkloadService {
  rpc Deploy(DeployRequest) returns (DeployResponse);
  rpc DeployStream(stream DeployRequest) returns (stream DeployResponse);
  rpc WatchWorkload(WatchWorkloadRequest) returns (stream WorkloadEvent);
}
```

### 3.3 `telemetry.proto`

| Message / enum | Fields | Rules |
|---|---|---|
| `enum ContainerPhase` | 0 UNSPECIFIED, 1 PENDING, 2 PULLING, 3 RUNNING, 4 SUCCEEDED, 5 FAILED, 6 OOM_KILLED, 7 TERMINATING | |
| `ContainerState` | 1 `string name`, 2 `ContainerPhase phase`, 3 `int32 restart_count`, 4 `optional int32 exit_code`, 5 `Timestamp started_at`, 6 `optional bytes image_digest` | restart_count `gte 0`; exit_code `gte -128, lte 255`; digest `bytes.len = 32`; CEL `container_state.exit_code_iff_terminal`: `has(this.exit_code) == (this.phase in [4, 5, 6])` |
| `ResourceSample` | 1 `double cpu_cores`, 2 `uint64 memory_working_set_bytes`, 3 `uint64 rx_bytes`, 4 `uint64 tx_bytes`, 5 `uint64 fs_used_bytes` | cpu `double = {gte: 0, finite: true}` |
| `TelemetrySample` | 1 `Envelope envelope`, 2 `WorkloadRef ref`, 3 `ClusterRef cluster`, 4 `string pod_name`, 5 `Timestamp sampled_at`, 6 `repeated ContainerState containers`, 7 `ResourceSample resources`, 8 `map<string,double> custom_gauges` | containers `max_items: 16`; gauges `max_pairs: 32`, values `finite`; CEL container names unique |
| `TelemetryBatch` | 1 `repeated TelemetrySample samples` | `min_items: 1, max_items: 1000` (MQTT and JetStream payload) |
| `TelemetrySummary` | 1 `Envelope`, 2 `uint64 samples_accepted`, 3 `uint64 samples_rejected`, 4 `Timestamp first_sample_at`, 5 `Timestamp last_sample_at`, 6 `map<string,uint64> rejected_by_reason` | CEL `telemetry_summary.last_ge_first` |
| `service TelemetryService` | `rpc ReportTelemetry(stream TelemetrySample) returns (TelemetrySummary)`; `rpc ReportUsage(stream UsageRecord) returns (UsageAck)` | |

### 3.4 `billing.proto`

| Message / enum | Fields | Rules |
|---|---|---|
| `enum Meter` | 0 UNSPECIFIED, 1 CPU_CORE_SECONDS, 2 MEMORY_GIB_SECONDS, 3 EGRESS_BYTES, 4 STORAGE_GIB_HOURS, 5 REQUESTS | |
| `MeterReading` | 1 `Meter meter`; `oneof value { 2 uint64 count; 3 double quantity; }`; 4 `string unit` | oneof required; quantity `gte: 0, finite: true`; meter `not_in: [0]` |
| `UsageRecord` | 1 `Envelope`, 2 `WorkloadRef ref`, 3 `ClusterRef cluster`, 4 `string record_id`, 5 `Timestamp window_start`, 6 `Timestamp window_end`, 7 `repeated MeterReading readings`, 8 `optional string sku` | record_id `uuid` (the idempotency key for billing dedup); readings `min_items: 1, max_items: 16`; CEL `usage_record.window_ordered`: `this.window_end > this.window_start`; CEL `usage_record.window_max_1h`: `this.window_end - this.window_start <= duration('1h')`; CEL `usage_record.meters_unique`: `this.readings.map(r, r.meter).unique()` |
| `UsageBatch` | 1 `repeated UsageRecord records` | `min_items: 1, max_items: 500` |
| `UsageAck` | 1 `Envelope`, 2 `uint64 records_accepted`, 3 `uint64 records_duplicate`, 4 `uint64 records_rejected`, 5 `bytes batch_sha256` | sha `bytes.len = 32` |

### 3.5 `logs.proto`

| Message / enum | Fields | Rules |
|---|---|---|
| `enum LogStream` | 0 UNSPECIFIED, 1 STDOUT, 2 STDERR | |
| `LogSelector` | 1 `WorkloadRef ref`, 2 `optional string pod_name`, 3 `optional string container` | DNS-1123 patterns |
| `StreamLogsRequest` | 1 `Envelope`, 2 `LogSelector selector`, 3 `bool follow`, 4 `optional int64 tail_lines`, 5 `optional Timestamp since`, 6 `optional Duration max_duration`, 7 `optional uint32 max_chunks` | tail `gte: 0, lte: 100000`; max_duration `lte: 1h`; CEL `stream_logs.tail_or_since_or_follow`: `this.follow \|\| has(this.tail_lines) \|\| has(this.since)` |
| `FetchLogsRequest` | 1 `Envelope`, 2 `LogSelector`, 3 `Timestamp since`, 4 `Timestamp until`, 5 `int32 limit`, 6 `string page_token` | limit `gte: 1, lte: 10000`; CEL `fetch_logs.until_after_since` |
| `LogChunk` | 1 `Envelope`, 2 `string pod_name`, 3 `string container`, 4 `LogStream stream`, 5 `Timestamp first_ts`, 6 `Timestamp last_ts`, 7 `uint32 line_count`, 8 `bytes data`, 9 `bool truncated` | data `bytes.max_len = 1048576`; CEL `log_chunk.ts_ordered` |
| `FetchLogsResponse` | 1 `Envelope`, 2 `repeated LogChunk chunks`, 3 `string next_page_token` | chunks `max_items: 1000` |
| `service LogService` | `rpc StreamLogs(StreamLogsRequest) returns (stream LogChunk)`; `rpc FetchLogs(FetchLogsRequest) returns (FetchLogsResponse)` | |

`LogChunk.data` is raw `bytes` on purpose: a log chunk is the one payload
where ProtoJSON's base64 expansion (×1.33) and the binary codec's zero-copy
size advantage are visible at scale.

### 3.6 Feature-coverage matrix

| Protobuf feature | Where it is exercised | What the benchmark measures |
|---|---|---|
| Scalars (`string`, `bool`, `int32/64`, `uint32/64`, `double`, `fixed32`) | throughout; `fixed32 request_wire_bytes` (constant-size tag), `double cpu_cores` | encoded bytes; varint vs fixed width |
| Enums with `defined_only` / `not_in: [0]` | `WorkloadKind`, `DeployStatus`, `ContainerPhase`, `Meter`, `Codec`, `Transport` | validation rejects unknown / unspecified |
| Nested messages (3 deep) | `DeployRequest.create.containers[].image` | codec cost vs nesting |
| Repeated scalar and repeated message | `command`, `containers`, `readings`, `chunks` | cost vs item count (fixtures `small`…`large`) |
| Maps with three value types | `labels` (`string`), `custom_gauges` (`double`), `rejected_by_reason` (`uint64`) | map encoding cost; deterministic-order cost |
| `optional` presence | `working_dir`, `exit_code`, `tail_lines`, `since_generation`, `ttl_after_finished` | absent vs present in binary and JSON (`sparse` vs `dense`) |
| `oneof` (five) | `ImageSource.ref`, `Placement.policy`, `DeployRequest.op`, `EnvVar.value`, `MeterReading.value` | generated type safety; `oneof.required` validation |
| `Timestamp` / `Duration` / `FieldMask` | envelope; `max_rtt`, `grace_period`; `UpdateWorkload.update_mask` | one-way latency; JSON special forms (`"1.5s"`, `"replicas,labels"`) |
| `bytes` fixed-length and bounded | `digest` (32), `message_id` (16), `LogChunk.data` (≤ 1 MiB) | binary vs base64 expansion |
| Message-level CEL | `Resources`, `WorkloadSpec` ×3, `RegionAffinity`, `UsageRecord` ×3, `DeployResponse` ×2, `Envelope` | validation cost; structured error output |
| Field-level CEL | `Envelope.request_sha256` | "empty or exactly 32 bytes" |
| Unknown-field preservation | schema-evolution demo ([§10](#10-best-practices-for-fair-comparison)) | binary keeps, ProtoJSON rejects |

### 3.7 `bench.proto` (full text)

```proto
syntax = "proto3";

package workloads.v1;

import "buf/validate/validate.proto";
import "google/protobuf/timestamp.proto";

option go_package = "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1;workloadsv1";

enum Codec {
  CODEC_UNSPECIFIED = 0;
  CODEC_PROTO = 1;     // google.golang.org/protobuf/proto
  CODEC_PROTOJSON = 2; // google.golang.org/protobuf/encoding/protojson
  CODEC_VTPROTO = 3;   // planetscale/vtprotobuf MarshalVT/UnmarshalVT (separate profile)
}

enum Transport {
  TRANSPORT_UNSPECIFIED = 0;
  TRANSPORT_GRPC_UNARY = 1;
  TRANSPORT_GRPC_SERVER_STREAM = 2;
  TRANSPORT_GRPC_CLIENT_STREAM = 3;
  TRANSPORT_GRPC_BIDI = 4;
  TRANSPORT_NATS_REQUEST_REPLY = 5;
  TRANSPORT_NATS_JETSTREAM = 6;
  TRANSPORT_RABBITMQ_RPC = 7;
  TRANSPORT_RABBITMQ_QUORUM = 8;
  TRANSPORT_VALKEY_STREAM = 9;
  TRANSPORT_MQTT = 10;
}

// Envelope is field 1 of every request / response and every bus payload.
// The client fills 1-4 and 10-13; the responder fills 5-9 and echoes the rest.
message Envelope {
  string run_id = 1 [(buf.validate.field).string = {
    min_len: 1
    max_len: 64
    pattern: "^[A-Za-z0-9._-]+$"
  }];
  bytes message_id = 2 [(buf.validate.field).bytes.len = 16]; // UUIDv7, raw
  uint64 sequence = 3;
  google.protobuf.Timestamp client_send_time = 4 [(buf.validate.field).required = true];
  google.protobuf.Timestamp server_receive_time = 5;
  google.protobuf.Timestamp server_send_time = 6;
  // fixed32 so the envelope's wire size does not depend on the value.
  fixed32 request_wire_bytes = 7;
  // Empty unless the run enables integrity checking (-integrity=sha256).
  bytes request_sha256 = 8 [(buf.validate.field).cel = {
    id: "envelope.sha256_len"
    message: "request_sha256 must be empty or exactly 32 bytes"
    expression: "size(this) == 0 || size(this) == 32"
  }];
  string responder_id = 9 [(buf.validate.field).string.max_len = 128]; // "<region>/<pod>"
  Codec codec = 10 [(buf.validate.field).enum = {
    defined_only: true
    not_in: [0]
  }];
  Transport transport = 11 [(buf.validate.field).enum = {
    defined_only: true
    not_in: [0]
  }];
  string fixture = 12 [(buf.validate.field).string = {
    in: ["tiny", "small", "medium", "large", "max", "sparse", "dense"]
  }];
  uint32 attempt = 13 [(buf.validate.field).uint32.lte = 16]; // > 0 only under fault mode
  option (buf.validate.message).cel = {
    id: "envelope.server_times_ordered"
    message: "server_send_time must not precede server_receive_time"
    expression: "!has(this.server_send_time) || !has(this.server_receive_time) || this.server_send_time >= this.server_receive_time"
  };
}

// Smallest possible round trip: the "codec floor" measurement.
message PingRequest {
  Envelope envelope = 1 [(buf.validate.field).required = true];
}

message PingResponse {
  Envelope envelope = 1 [(buf.validate.field).required = true];
}

// NTP-style four-timestamp probe:
//   offset      = ((t2 - t1) + (t3 - t4)) / 2
//   uncertainty = (t4 - t1) - (t3 - t2)
message ClockProbeRequest {
  Envelope envelope = 1 [(buf.validate.field).required = true];
  google.protobuf.Timestamp t1 = 2 [(buf.validate.field).required = true];
}

message ClockProbeResponse {
  Envelope envelope = 1 [(buf.validate.field).required = true];
  google.protobuf.Timestamp t1 = 2;
  google.protobuf.Timestamp t2 = 3; // server receive
  google.protobuf.Timestamp t3 = 4; // server send
  string clock_source = 5;          // /sys/devices/system/clocksource/clocksource0/current_clocksource
  bool chrony_synced = 6;           // chronyc tracking: "Leap status: Normal"
}

service BenchService {
  rpc Ping(PingRequest) returns (PingResponse);
  rpc ClockProbe(ClockProbeRequest) returns (ClockProbeResponse);
}
```

### 3.8 Fixtures (payload corpus)

All fixtures are deterministic (`math/rand/v2` PCG, fixed seed) and built
once per run outside every timed loop. Sizes are approximate binary-proto
wire sizes; ProtoJSON is ≈ 2.2–3× for `medium` / `large` and ≈ 1.35× for
`max` (base64 of the log bytes).

| Fixture | Message | Shape | ≈ bytes (proto) | Purpose |
|---|---|---|---|---|
| `tiny` | `PingRequest` | envelope only | 70 | per-operation floor |
| `small` | `DeployRequest{delete}` | ref + `DeleteWorkload` | 180 | typical control command |
| `medium` | `DeployRequest{create}` | 1 container, 4 env, 2 ports, 4 labels | 650 | nested + repeated + map |
| `large` | `DeployRequest{create}` | 8 containers, 32 env, 16 labels + 16 annotations | 7 000 | large repeated / map |
| `max` | `LogChunk` | 960 KiB `data` | 983 000 | large-message behaviour (NATS default `max_payload` is 1 MB; keep under it or raise it in `nix/gitops/env/nats.nix`) |
| `sparse` | `medium` with every `optional` / map unset | | 400 | presence / omission |
| `dense` | `medium` with every `optional` / map populated | | 1 100 | full JSON mapping |

Telemetry and usage fixtures (`TelemetrySample` with 1 / 4 / 16 containers,
`UsageRecord` with 1 / 5 readings) follow the same naming and are used for the
one-way transports.

### 3.9 Subjects, queues, and topics

| Transport | Address | Request → response |
|---|---|---|
| NATS request-reply | `wl.<region>.deploy`, responders in queue group `agents`, reply on the NATS-supplied inbox | `DeployRequest` → `DeployResponse` |
| NATS JetStream | stream `WL_TELEMETRY` (R3, file), subjects `wl.<region>.telemetry`, `wl.<region>.usage`; durable pull consumer per agent | `TelemetrySample` / `UsageRecord`; ack = JetStream ack |
| NATS JetStream (logs fan-out profile) | stream `WL_LOGS`, subject `wl.<region>.logs.<workload_id>`; ephemeral push consumers per subscriber | `LogChunk` |
| RabbitMQ RPC | direct exchange `wl`, routing key `deploy.<region>`, `reply_to` = per-client exclusive queue (or direct reply-to), `correlation_id` = `message_id` | `DeployRequest` → `DeployResponse` |
| RabbitMQ quorum | queues `wl.telemetry.<region>`, `wl.usage.<region>` (`x-queue-type: quorum`), persistent delivery mode, publisher confirms, manual ack | `TelemetrySample` / `UsageRecord` |
| Valkey Streams | `XADD wl:<region>:deploy`, consumer group `agents`; reply via `XADD wl:reply:<client_id>`; telemetry `wl:<region>:telemetry`; `MAXLEN ~` trimming | `DeployRequest` → `DeployResponse` |
| MQTT (one-way) | `wl/<region>/telemetry` (QoS 0 and QoS 1 as separate profiles), `/json` suffix for ProtoJSON | `TelemetryBatch`; one-way latency only |
| gRPC | `WorkloadService`, `TelemetryService`, `LogService`, `BenchService` on one h2c listener (`:9090`) | per service |

The codec is carried in `Envelope.codec` **and** as transport metadata so a
responder can decode before it has seen the envelope: NATS header
`Content-Type`, AMQP `content_type`, Valkey stream field `ct`, MQTT topic
suffix, gRPC content-subtype (`application/grpc+proto` / `+json`).

## 4. Buf configuration and code generation

Protos and buf configuration live **inside the Go module** so `nix/clients.nix`'s
`src = ../clients` continues to see everything and the module stays
self-contained:

```
clients/
├── buf.yaml
├── buf.gen.yaml
├── buf.lock
├── proto/workloads/v1/{common,bench,telemetry,billing,logs,workload}.proto
└── gen/
    ├── go/workloads/v1/*.pb.go, *_grpc.pb.go, *_vtproto.pb.go     # checked in
    └── descriptors/workloads.binpb                                # buf breaking baseline
```

`clients/buf.yaml`:

```yaml
version: v2
modules:
  - path: proto
deps:
  - buf.build/bufbuild/protovalidate
lint:
  use:
    - STANDARD
  # Deliberate: Deploy (unary) and DeployStream (bidi) share DeployRequest /
  # DeployResponse so the same bytes go over both, and WatchWorkload streams
  # WorkloadEvent, which is also a bus payload. STANDARD would otherwise
  # demand DeployStreamRequest / WatchWorkloadResponse wrapper types.
  except:
    - RPC_REQUEST_RESPONSE_UNIQUE
    - RPC_REQUEST_STANDARD_NAME
    - RPC_RESPONSE_STANDARD_NAME
breaking:
  use:
    - FILE
```

`clients/buf.gen.yaml` — **every plugin is `local:`** (Nix-pinned binaries
on `PATH`), so `buf generate` never contacts the Buf Schema Registry for
plugins and cannot be rate-limited:

```yaml
version: v2
managed:
  enabled: true
  disable:
    - file_option: go_package_prefix
      module: buf.build/bufbuild/protovalidate
plugins:
  - local: protoc-gen-go
    out: gen/go
    opt:
      - paths=source_relative
  - local: protoc-gen-go-grpc
    out: gen/go
    opt:
      - paths=source_relative
  # Opt-in fast path, benchmarked as its own codec profile (§7.1).
  - local: protoc-gen-go-vtproto
    out: gen/go
    opt:
      - paths=source_relative
      - features=marshal+unmarshal+size+pool
      - pool=github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1.DeployRequest
      - pool=github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1.TelemetrySample
      - pool=github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1.UsageRecord
      - pool=github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1.LogChunk
```

### 4.1 Generated code is checked in; drift is a check

As in xtcp2, the generated Go code is committed so `gopls`, `go test`, and
`buildGoModule` work without running protoc. What xtcp2 lacks — and this
design adds — is a hermetic **gen-drift** check: `nix flake check`
regenerates into a temp dir inside the sandbox and `diff -r`s against
`clients/gen/go`. Regenerate with `nix run .#regen-protos`.

### 4.2 Hermetic buf module cache (fixed-output derivation)

`buf lint` / `buf breaking` / `buf generate` need the `buf.build/bufbuild/protovalidate`
module. xtcp2 keeps it only in a gitignored on-disk cache, so its lint cannot
run under `nix flake check`. Here the cache is a **fixed-output derivation**
keyed on `buf.lock`:

```nix
# nix/protos/buf-deps.nix
{ pkgs, versions, src }:   # src = ../../clients; only buf.yaml, buf.lock, proto/ matter
pkgs.stdenvNoCC.mkDerivation {
  name = "buf-module-cache";
  inherit src;
  nativeBuildInputs = [ versions.buf pkgs.cacert ];
  outputHashAlgo = "sha256";
  outputHashMode = "recursive";
  outputHash = versions.bufDepsHash;   # refresh like goVendorHash when buf.lock or buf changes
  SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
  buildPhase = ''
    export HOME=$TMPDIR BUF_CACHE_DIR=$out
    # buf.lock is committed: this resolves the pinned commits into the cache
    # without `buf dep update` (which would rewrite the lock file).
    buf build -o $TMPDIR/image.binpb
    # drop cache-internal lock/marker files so the NAR hash is content-only
    find $out -type f -name '*.lock' -delete
  '';
  dontInstall = true;
  dontFixup = true;
}
```

Consumers share one helper:

```nix
# nix/protos/default.nix
mkBufCheck = { name, extraInputs ? [ ], cmd }:
  pkgs.runCommand name { nativeBuildInputs = [ versions.buf ] ++ extraInputs; } ''
    cp -r ${bufDeps} $TMPDIR/bufcache && chmod -R u+w $TMPDIR/bufcache   # buf wants a writable cache
    export BUF_CACHE_DIR=$TMPDIR/bufcache HOME=$TMPDIR
    cd ${src}
    ${cmd}
    touch $out
  '';
# proto-lint:      cmd = "buf lint"
# proto-breaking:  cmd = "buf breaking --against gen/descriptors/workloads.binpb"
# proto-gen-drift: extraInputs = [ versions.protoc-gen-go versions.protoc-gen-go-grpc versions.protoc-gen-go-vtproto ];
#                  cmd = "buf generate -o $TMPDIR/out && diff -ru gen/go $TMPDIR/out/gen/go"
```

The sandbox has no network, so a missing module fails loudly instead of
silently fetching. `regen-protos` (impure, host-side) is the only place
`buf dep update` runs; it refreshes `gen/descriptors/workloads.binpb` only
under `--accept-breaking`, so `proto-breaking` in `nix flake check` is a real
gate.

### 4.3 Go dependencies to add (`clients/go.mod`)

| Module | Version | Why |
|---|---|---|
| `google.golang.org/protobuf` | v1.36.12 (already indirect → direct) | codec |
| `google.golang.org/grpc` | v1.83.2 | gRPC; `encoding.CodecV2`, `mem.BufferPool` |
| `buf.build/go/protovalidate` | v0.14.0 | `protovalidate.Validate` |
| `buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go` | matching | generated `buf/validate` package imported by `*.pb.go` |
| `github.com/planetscale/vtprotobuf` | v0.6.0 | `protohelpers` runtime for the vtproto profile |
| `github.com/HdrHistogram/hdrhistogram-go` | v1.1.2 | latency histograms in the harness |
| `github.com/google/uuid` | v1.6.0 (indirect → direct) | UUIDv7 `message_id` |
| `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` | latest | standard RPC metrics (§11) |

One `vendorHash` bump in `nix/versions.nix`. Go floor stays `go 1.25.0` (the
`b.Loop()` / `stdversion` caveat in `docs/benchmarks.md` still applies).

## 5. Modular Nix layout

Rule: **one concern per file, each ≤ ~150 lines**, `nix/versions.nix` is the
only file that names a tool or a hash, `flake.nix` stays an orchestrator.
New or changed files:

```
nix/versions.nix                 go, buf, protobuf, protoc-gen-go{,-grpc,-vtproto}, grpcurl, goVendorHash, bufDepsHash
nix/lib/goModules.nix            { src = ../../clients } → { goModules; vendoredSource }  (xtcp2 pattern)
nix/lib/mkGoBinary.nix           buildGoModule wrapper: static, -trimpath, -s -w, CGO_ENABLED=0, version ldflags
nix/protos/default.nix           { bufDeps; regenProtos; checks = { lint; breaking; genDrift; }; mkBufCheck }
nix/protos/buf-deps.nix          FOD buf module cache (§4.2)
nix/protos/buf-lint.nix          mkBufCheck "proto-lint"
nix/protos/buf-breaking.nix      mkBufCheck "proto-breaking"
nix/protos/buf-generate.nix      writeShellApplication regen-protos (impure; --accept-breaking)
nix/protos/gen-drift.nix         mkBufCheck "proto-gen-drift"
nix/checks/default.nix           merges cli-tests (existing) + proto-* + go-vet + gofmt
nix/checks/go-vet.nix            runCommand over vendoredSource: GOFLAGS=-mod=vendor go vet ./...
nix/checks/gofmt.nix             gofmt -l on clients/ excluding gen/
nix/images/default.nix           becomes an aggregator: imports lib.nix + per-image files; exports { images; imageList }
nix/images/lib.nix               mkImage (moved verbatim from today's default.nix)
nix/images/{nats,rabbitmq,mosquitto,valkey,monitoring}.nix   one image (or group) per file, unchanged content
nix/images/region-agent.nix      first Nix-built Go-binary image: busybox + mkGoBinary region-agent, Entrypoint, ExposedPorts
nix/gitops/env/workloads.nix     namespace, 4 Deployments, 4 NodePort Services, headless Service, ArgoCD Application
nix/gitops/env/monitoring/…      monitoring.nix split per concern (§12.1); + scrape jobs workloads-agents, proto-bench-driver
nix/constants.nix                + messageBus.workloads, + protoBench (below)
nix/proto-bench-scripts.nix      k8s-proto-bench host harness → proto-bench-logs/<run-id>/
nix/clients.nix                  + subPackages workloads/benchcli, workloads/region-agent; + apps
nix/shell.nix                    + buf protobuf protoc-gen-go protoc-gen-go-grpc protoc-gen-go-vtproto grpcurl; shell fns regen-protos, proto-lint
flake.nix                        + versions / protos / checks imports; packages.region-agent-image; apps regen-protos, k8s-proto-bench
```

Import graph:

```
flake.nix
 ├─ nix/versions.nix ◀────────────────────────────┐
 ├─ nix/clients.nix ─ nix/lib/goModules.nix ───────┤
 │                  └ nix/lib/mkGoBinary.nix ──────┤
 ├─ nix/protos/default.nix ─ buf-deps.nix ─────────┤
 │                         ├ buf-lint.nix          │
 │                         ├ buf-breaking.nix      │
 │                         ├ buf-generate.nix      │
 │                         └ gen-drift.nix         │
 ├─ nix/checks/default.nix ─ go-vet.nix, gofmt.nix ┘  (+ clients.tests, protos.checks)
 ├─ nix/images/default.nix ─ lib.nix + {nats,…,region-agent}.nix   (region-agent.nix imports clients.nix)
 ├─ nix/gitops/default.nix ─ env/workloads.nix, env/monitoring/…
 └─ nix/proto-bench-scripts.nix ─ clients.nix, microvm-scripts.nix, constants.nix
```

`nix/constants.nix` additions (names chosen to avoid the existing
`constants.bench` / `bench-logs/` used by `k8s-client-bench`):

```nix
messageBus.workloads = {
  namespace   = "workloads";
  image       = "messagebus.local/region-agent";
  tag         = "0.1.0";
  grpcPort    = 9090;      # plaintext h2c inside the cluster
  metricsPort = 9464;
  # node → region label; cp0 is also the driver's stable endpoint
  regions = { cp0 = "us-west-2"; cp1 = "us-east-1"; cp2 = "eu-west-1"; w3 = "ap-south-1"; };
  nodePortGrpcBase  = 30710;  # + node index: cp0 30710, cp1 30711, cp2 30712, w3 30713
  nodePortGlobalApi = 30700;  # reserved: a later "global API" pod on cp0
};

protoBench = {
  hostMetricsBasePort = 9300;   # driver-side OTel /metrics; soak uses 9200-9211
  hostMetricsCount    = 8;
  defaults = {
    duration = "30s"; warmup = "5s"; rate = "2000/s"; inflight = 64; repeats = 5;
    logDir = "./proto-bench-logs";
  };
};
```

`nix/versions.nix` (single source of truth, mirrors xtcp2):

```nix
{ pkgs }:
rec {
  go = pkgs.go;                                  # 1.26.x in the pinned nixpkgs
  buf = pkgs.buf;                                # 1.72.0
  protoc = pkgs.protobuf;                        # 36.1 (protoc + WKT includes)
  protoc-gen-go = pkgs.protoc-gen-go;            # 1.36.12
  protoc-gen-go-grpc = pkgs.protoc-gen-go-grpc;  # 1.6.2
  protoc-gen-go-vtproto = pkgs.protoc-gen-go-vtproto;
  grpcurl = pkgs.grpcurl;
  goVendorHash = "sha256-…";                     # nix build .#message-bus-clients 2>&1 | grep 'got:'
  bufDepsHash  = "sha256-…";                     # nix build .#buf-deps 2>&1 | grep 'got:'
}
```

`regen-protos` (`nix/protos/buf-generate.nix`):

```nix
pkgs.writeShellApplication {
  name = "regen-protos";
  runtimeInputs = [ versions.buf versions.protoc-gen-go versions.protoc-gen-go-grpc versions.protoc-gen-go-vtproto ];
  text = ''
    cd "$(git rev-parse --show-toplevel)/clients"
    buf dep update        # the only network touch; pins buf.lock
    buf lint
    buf build
    buf generate
    if [ "''${1:-}" = "--accept-breaking" ]; then
      buf build -o gen/descriptors/workloads.binpb
    else
      buf breaking --against gen/descriptors/workloads.binpb
    fi
    echo "regen-protos: done — review and commit gen/ drift; bump bufDepsHash if buf.lock changed"
  '';
}
```

## 6. Go code outline (`clients/`)

Everything lives in the existing module. Folder-per-domain follows the
folder-per-bus convention (leaf directory basename == binary name).

| Path | Package | Responsibility | Key exports | ~LoC |
|---|---|---|---|---|
| `clients/proto/workloads/v1/*.proto` | — | schema (§3) | — | 600 |
| `clients/gen/go/workloads/v1/` | `workloadsv1` | checked-in generated code | generated | gen |
| `clients/internal/pool` | `pool` | typed message pool + size-class byte-buffer pool (§7.2) | `Msg[T, PT]`, `Buffers`, `NewBuffers`, `BufferPool` (interface, structurally = `mem.BufferPool`), `NopBuffers`, `NopMsg`, `Stats`, `ParseMode` | 220 + 250 tests |
| `clients/internal/codec` | `codec` | codec interface + the three implementations (§7.3) | `Codec`, `Proto`, `ProtoJSON`, `VT` (in `vt.go`), `Encode(c, bp, m)`, `ByName`, `ContentType` | 260 + 300 tests/bench |
| `clients/internal/envelope` | `envelope` | fill / stamp / verify envelopes; correlation; integrity | `Fill(env, runID, seq, codec, transport, fixture)` (UUIDv7 via `uuid.NewV7()`), `StampReceive`, `StampSend`, `Sum256(b) [32]byte`, `RTT(env, recvMono)`, `OneWay(env, offset)`, `Anomaly` classification | 150 + 200 |
| `clients/internal/corpus` | `corpus` | deterministic fixtures (§3.8) | `Fixture`, `New(seed) *Corpus`, `Deploy(f)`, `Telemetry(f)`, `Usage(f)`, `LogChunk(f)`, `Sizes()` | 350 + 120 |
| `clients/internal/transport` | `transport` | transport-neutral interfaces | `Requester{Request(ctx, req, resp proto.Message) error; Close()}`, `Responder{Serve(ctx, Handler) error}`, `Publisher{Publish(ctx, m) (Release, error)}`, `Consumer{Consume(ctx, func(Msg) error) error}`, `Msg{Bytes(); Ack(); Codec()}`, `Options{Codec, Pool, Region, Integrity, Validate}` | 120 |
| `clients/internal/transport/grpc` | `grpctransport` | `codecV2`, envelope `stats.Handler`, `Dial`, `NewServer`, four requesters, service registration, reflection | §7.4 | 500 |
| `clients/internal/transport/nats` | `natstransport` | request-reply requester / responder (queue group); JetStream publisher (`PublishAsync` + `PublishAsyncComplete`) and pull consumer | | 350 |
| `clients/internal/transport/rabbitmq` | `rmqtransport` | RPC (reply queue + `correlation_id`); quorum publisher with `PublishWithDeferredConfirmWithContext` + batched `Wait`; consumer with manual ack | | 400 |
| `clients/internal/transport/valkey` | `valkeytransport` | `XADD` / `XREADGROUP` / `XACK` requester + responder, reply streams, `MAXLEN ~` trimming; Sentinel `FailoverClient` reused from `valkeycli` | | 350 |
| `clients/internal/transport/mqtt` | `mqtttransport` | QoS 0/1 publisher with bounded in-flight ring + reaper (§7.5); subscriber with `OrderMatters=false` | | 250 |
| `clients/internal/harness` | `harness` | run modes, HDR histograms, run records, `mbbench_*` instruments (§8, §11) | `Mode`, `Run(ctx, cfg, Requester) (*Result, error)`, `Result.Write{TSV,MD,HGRM,JSON}`, `Cell` | 700 + 300 |
| `clients/internal/metrics` (change) | `metrics` | expose the provider/registry, register Go runtime + process collectors, explicit latency buckets (§11) | `NewProvider(addr string, views ...sdkmetric.View) (*sdkmetric.MeterProvider, *prometheus.Registry, error)`; existing `Setup` unchanged | +60 |
| `clients/internal/cli` (change) | `cli` | export `ParseRate` for `-rate` | | +5 |
| `clients/workloads/benchcli` | `main` | host driver | `benchcli codec\|grpc\|nats\|rabbitmq\|valkey\|mqtt\|clockprobe\|report [flags]` | 450 |
| `clients/workloads/region-agent` | `main` | in-cluster server | gRPC listener (all four services + reflection), bus responders / consumers for its region, synthetic telemetry / usage / log emitters, `/metrics`, `/healthz` | 600 |

Driver flags (shared by `benchcli` subcommands, parsed with the
`internal/cli` conventions): `-region`, `-codec proto|protojson|vtproto`,
`-pool none|messages|buffers|all`, `-fixture`, `-mode latency|windowed|openloop|saturation|coldstart|fault`,
`-rate`, `-duration`, `-warmup`, `-inflight`, `-integrity none|sha256`,
`-validate on|off`, `-compression none|gzip` (gRPC only), `-run-id`, `-out`,
`-metrics-addr`. Region-agent flags: `-region`, `-grpc-addr`, `-metrics-addr`,
`-nats`, `-amqp`, `-valkey-sentinels`, `-mqtt`, `-codec`, `-pool`, plus
`GOGC` / `GOMEMLIMIT` from the Deployment env.

The `codec` subcommand is an in-process loop (no transport) that exercises
exactly the same `codec` + `pool` code the transports use, so the codec-only
numbers and the end-to-end numbers are directly comparable. `go test -bench`
sub-benchmarks under `internal/codec` and `internal/pool` run through the
existing `nix run .#clients-bench`.

## 7. `sync.Pool` strategy for low memory pressure

### 7.1 What actually allocates, and what pooling can and cannot fix

With the official library, `proto.Unmarshal` into a message with `Merge=false`
first calls `Reset` (`proto/decode.go`), and generated `Reset()` is
`*x = T{}`: every nested message pointer, repeated slice, and map is dropped
and re-allocated on the next decode. Pooling a `*DeployRequest` therefore
saves exactly **one** allocation (the top-level struct) out of the ~20–60 a
`medium` / `large` decode performs. `proto.Reset(m)` keeps sub-messages only
if you retain the pointers yourself and re-attach them after the reset, which
is fragile and not worth doing generically.

Consequences:

| Layer | Official-library profile (`proto`, `protojson`) | `vtproto` profile |
|---|---|---|
| Top-level message structs | `pool.Msg[T]` (generic `sync.Pool` + `proto.Reset` on `Put`) for flat, envelope-heavy messages (`PingRequest`, `UsageAck`, `TelemetrySample` without containers) and as the `RecvMsg` target in stream loops | `<Msg>FromVTPool()` / `ReturnToVTPool()`; `ResetVT()` **keeps** nested slices and sub-messages so `UnmarshalVT` reuses them |
| Output byte buffers | `pool.Buffers` size-class pool + `MarshalAppend` | same buffers + `MarshalToSizedBufferVT` |
| Input buffers | returned to the pool immediately after `Unmarshal` (`bytes` / `string` fields are copied; `UnmarshalOptions` has no alias mode) | same with `UnmarshalVT`; **never** `UnmarshalVTUnsafe` with pooled inputs |
| gRPC transport buffers | grpc-go `mem.BufferPool` (default tiered pool or ours) | same |

**When vtprotobuf is worth it:** messages with repeated / nested fields
decoded at high rate on the agent (telemetry client-streams, JetStream and
quorum consumers), where `ResetVT` turns ~40 allocs/msg into ~0 in steady
state, and marshal paths where `SizeVT` + `MarshalToSizedBufferVT` avoid the
reflection-driven table walk. It is not worth it for `tiny` / `small` (the
buffer pool dominates) and it has no JSON path. It is kept as a **separately
labelled profile** — `Codec.CODEC_VTPROTO`, `-codec=vtproto`, its own
`*_vtproto.pb.go` files and `internal/codec/vt.go` — so the
official-library comparison (`proto` vs `protojson`) never links vtproto
code. On the wire vtproto and proto are byte-identical; a run keeps both ends
on the same profile anyway.

### 7.2 Pool primitives (`clients/internal/pool`)

```go
// Msg is a typed sync.Pool of generated messages. PT is the pointer type.
type Msg[T any, PT interface {
	*T
	proto.Message
}] struct{ p sync.Pool }

func (mp *Msg[T, PT]) Get() PT {
	if v := mp.p.Get(); v != nil {
		return v.(PT)
	}
	return PT(new(T))
}

func (mp *Msg[T, PT]) Put(m PT) {
	if m == nil {
		return
	}
	proto.Reset(m) // drops nested pointers; see §7.1
	mp.p.Put(m)
}

// Buffers is a size-class []byte pool with the same method set as grpc-go's
// mem.BufferPool, so one instance serves every transport.
type Buffers struct {
	classes []int       // e.g. 256, 1<<10, 4<<10, 16<<10, 64<<10, 256<<10, 1<<20
	pools   []sync.Pool // pools[i] holds *[]byte with cap == classes[i]
	stats   [][3]atomic.Uint64
}

func NewBuffers(classes ...int) *Buffers
func (b *Buffers) Get(n int) *[]byte // smallest class >= n; n > max class: fresh, never pooled
func (b *Buffers) Put(p *[]byte)     // bucket by cap(*p); cap not a class (grown slice): dropped
func (b *Buffers) Stats() Stats      // hits / misses / drops per class, for the run report
```

Rules baked into `Buffers`:

- `*[]byte`, not `[]byte`: avoids the interface-boxing allocation on `Put`.
- **Exact-capacity classes**: a pooled buffer is never larger than its class,
  which is the fix for the classic "one huge buffer poisons the pool"
  retention problem (Go issue #23199).
- Buffers larger than the top class are never pooled; the `max` fixture sits
  exactly at the top class so `-pool=all` vs `none` can be compared on it.
- `Put` drops anything whose `cap` is not exactly a class: a `MarshalAppend`
  that grew the slice returns a fresh backing array of arbitrary cap.
- `-pool=none|messages|buffers|all` (default `all`) swaps in `NopBuffers`
  (`Get` = `make`, `Put` = no-op) and `NopMsg`, so the code paths are identical
  and only reuse differs.

### 7.3 Codec (`clients/internal/codec`)

```go
type Codec interface {
	Name() string                                  // "proto" | "protojson" | "vtproto"
	SizeHint(m proto.Message) int                  // proto.Size / SizeVT; protojson: EWMA per (fixture, type)
	MarshalAppend(dst []byte, m proto.Message) ([]byte, error)
	Unmarshal(b []byte, m proto.Message) error     // b may be returned to its pool after this returns
}

var Proto = protoCodec{
	m: proto.MarshalOptions{UseCachedSize: true, Deterministic: false},
	u: proto.UnmarshalOptions{Merge: false, DiscardUnknown: false},
}

var ProtoJSON = jsonCodec{
	m: protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false},
	u: protojson.UnmarshalOptions{DiscardUnknown: false},
}

// Encode marshals into a pooled buffer. The caller must Put(buf) once the
// transport has copied it (see §7.5 for when that is, per library).
func Encode(c Codec, bp pool.BufferPool, m proto.Message) (*[]byte, error) {
	buf := bp.Get(c.SizeHint(m))
	out, err := c.MarshalAppend((*buf)[:0], m)
	if err != nil {
		bp.Put(buf)
		return nil, err
	}
	*buf = out // if MarshalAppend grew it, cap changed; Put will drop it
	return buf, nil
}
```

- `UseCachedSize: true` is safe only because `SizeHint` calls `proto.Size`
  immediately before — the same contract grpc-go's built-in codec relies on.
- ProtoJSON has no size function; `SizeHint` returns an EWMA of observed
  output sizes keyed by (fixture, type), rounded up to the next class, so
  steady-state ProtoJSON marshals hit the pool too.
- `Deterministic` stays off: map-ordering cost only matters for the sha256
  integrity mode, which hashes the bytes the responder actually received, not
  a re-encoding.
- Codec options are fixed per run and written to `run.json`.
- **Validation is a separate stage, never inside the codec**:
  `protovalidate.Validate(m)` (package-level API, `buf.build/go/protovalidate`)
  runs on the responder after `Unmarshal` and on the driver before `Marshal`,
  timed as its own histogram and switchable with `-validate=off`, so
  decode-only and decode-plus-validate are always reported separately.

### 7.4 gRPC: `encoding.CodecV2` over a `mem.BufferPool`

grpc-go ≥ 1.66 lets a codec hand the transport reference-counted buffers, and
lets the transport hand the codec pooled input buffers. `codecV2` wraps any
`codec.Codec`:

```go
// clients/internal/transport/grpc/codec.go
type codecV2 struct {
	inner codec.Codec
	pool  mem.BufferPool // *pool.Buffers satisfies it structurally; mem.NopBufferPool{} for -pool=none
	name  string         // "proto" for proto AND vtproto (wire-identical); "json" for protojson
}

func (c *codecV2) Marshal(v any) (mem.BufferSlice, error) {
	m := v.(proto.Message)
	size := c.inner.SizeHint(m)
	if mem.IsBelowBufferPoolingThreshold(size) { // <= 1 KiB: grpc's own policy, keep it
		b, err := c.inner.MarshalAppend(nil, m)
		return mem.BufferSlice{mem.SliceBuffer(b)}, err
	}
	buf := c.pool.Get(size)
	out, err := c.inner.MarshalAppend((*buf)[:0], m)
	if err != nil {
		c.pool.Put(buf)
		return nil, err
	}
	*buf = out
	return mem.BufferSlice{mem.NewBuffer(buf, c.pool)}, nil // freed by grpc after the HTTP/2 write
}

func (c *codecV2) Unmarshal(data mem.BufferSlice, v any) error {
	buf := data.MaterializeToBuffer(c.pool)
	defer buf.Free()
	return c.inner.Unmarshal(buf.ReadOnlyData(), v.(proto.Message))
}

func (c *codecV2) Name() string { return c.name }
```

Wiring per profile:

| Side | `proto` / `vtproto` | `protojson` | `grpc-default` baseline |
|---|---|---|---|
| server | `grpc.ForceServerCodecV2(c)` + `experimental.BufferPool(p)` | `encoding.RegisterCodecV2(jsonCodec)` at init (so `application/grpc+json` is negotiable) + `experimental.BufferPool(p)` | no options: grpc's built-in codec + `mem.DefaultBufferPool()` |
| client | `grpc.WithDefaultCallOptions(grpc.ForceCodecV2(c))` + `experimental.WithBufferPool(p)` | same with the json codec (content-subtype `json`) | none |
| `-pool=none` | `experimental.WithBufferPool(mem.NopBufferPool{})` / `experimental.BufferPool(mem.NopBufferPool{})` | same | n/a |

Also: stream loops call `stream.RecvMsg(m)` with a pooled `m` (grpc unmarshals
into the message you pass; `Merge=false` resets it); server-streaming
handlers reuse one response message per stream (`SendMsg` marshals
synchronously, so mutating afterwards is safe); `grpc.SharedWriteBuffer(true)`
/ `grpc.WithSharedWriteBuffer(true)` and 64 KiB read / write buffers are set
and recorded as run constants; tracing and binary logging stay off (they
force full materialisation and defeat the pool).

**Stability (grpc-go 1.83.x):** `encoding.CodecV2` / `RegisterCodecV2` /
`ForceCodecV2` / `ForceServerCodecV2` are stable API. The whole `mem` package
and everything under `experimental/` are marked experimental; the bench is
their only consumer and pins `google.golang.org/grpc v1.83.2`.

`server_receive_time` and `request_wire_bytes` on gRPC come from a
`stats.Handler` whose `TagRPC` puts a per-RPC struct in the context and whose
`HandleRPC` copies `stats.InPayload.WireLength` and `RecvTime` into it; the
service handler reads them from `ctx`. On the buses the responder holds the
raw payload and fills these directly. sha256 on gRPC is computed in
`codecV2.Unmarshal` over `buf.ReadOnlyData()` only when `-integrity=sha256`.

### 7.5 When a pooled buffer may be returned, per client library

| Library | Copy point (verified in vendored source) | Pooled buffer may be `Put` when |
|---|---|---|
| nats.go 1.39.1 `Publish` / `RequestWithContext` | `nc.publish` → `nc.bw.appendBufs(...)` appends into the writer's `bufs` (or a pending `bytes.Buffer` during reconnect) synchronously | immediately after `Publish` returns. Inbound `msg.Data` is owned by the `*nats.Msg` (already copied in `processMsg`): decode, then drop. |
| amqp091 1.10.0 `PublishWithContext` / `PublishWithDeferredConfirmWithContext` | `ch.send` → `sendOpen` writes header + body frames synchronously into the connection's buffered writer under the send mutex | immediately after the call returns (before the confirm arrives). `Delivery.Body` is owned by the delivery. |
| go-redis 9.7.3 `XAdd` | `proto.Writer.WriteArg` `case []byte` copies into the bufio writer during the synchronous command write | immediately after `XAdd` returns. **Not** inside `Pipeline` / `TxPipeline` until `Exec` returns (args are held until then). |
| paho 1.5.0 `Publish` | **none**: `pub.Payload = p`, the packet is queued on an outbound channel and written later by the writer goroutine; QoS > 0 packets are also held by the store until `PUBACK` | only after `<-token.Done()`. The MQTT publisher keeps a bounded in-flight ring of `{token, *[]byte}` and a reaper goroutine returns buffers as tokens complete; a full ring blocks the publisher, which is the natural QoS 1 backpressure. With `-pool=none` the ring is bypassed. |
| grpc-go | codec returns `mem.NewBuffer(buf, pool)`; grpc frees it after the HTTP/2 write | never manually — ownership passes to grpc. |

### 7.6 Proving it

- **Micro-benchmarks** (`internal/codec/codec_bench_test.go`,
  `internal/pool/pool_bench_test.go`, via `nix run .#clients-bench`):
  sub-benchmarks `codec × fixture × pool`, `b.ReportAllocs()`,
  `b.SetBytes(wire)`, package-level sinks against dead-code elimination,
  `for i := 0; i < b.N; i++` loops, `-args -pool=none|all`.
- **Runtime metrics on both ends** (§11): `metrics.NewProvider` registers
  `collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(...))`
  and `collectors.NewProcessCollector(...)`. The proof panels are
  `rate(go_gc_heap_allocs_bytes_total) / rate(mbbench_messages_total)` (bytes
  allocated per message), `rate(go_gc_cycles_automatic_gc_cycles_total)`,
  `histogram_quantile(0.99, go_gc_pauses_seconds)`, and the **GC CPU fraction**
  `go_cpu_classes_gc_total_cpu_seconds_total / go_cpu_classes_total_cpu_seconds_total`.
- **Exact deltas**: driver and agent call `runtime.GC()` and snapshot
  `runtime.ReadMemStats` (`Mallocs`, `TotalAlloc`, `NumGC`, `PauseTotalNs`) at
  the warmup boundary and at the end of each cell; the deltas go into
  `run.json` and are exact, unlike scrape-rate estimates.
- **GC configuration is part of the matrix**: (a) `GOGC=100`; (b) `GOGC=off`
  `GOMEMLIMIT=384MiB` with the agent Deployment's `limits.memory: 512Mi`.
  Pools should show fewer GC cycles under (a) and a flat `heap_inuse` plateau
  under (b). Both are echoed into the run report.
- **Pool statistics**: `mbbench_pool_ops_total{class, result=hit|miss|drop}`.

### 7.7 Table-driven tests for the pool helpers

`internal/pool/pool_test.go`, one row per case, each with `description` and
`expected`:

| Kind | Row | Expected |
|---|---|---|
| positive | `Get(100)` from classes `[256, 1024]` | cap 256, len 100 |
| positive | `Put` then `Get` in the same class | same backing array (`&(*p)[0]` equal); `Stats.hits == 1` |
| positive | `Msg[DeployRequest].Put` then `Get` | `proto.Equal(got, &DeployRequest{})`; oneof nil |
| negative | `Put` of a slice with cap 300 (not a class) | dropped; `Stats.drops == 1` |
| negative | `Put(nil)` | no panic, no-op |
| negative | `Msg.Put(nil)` | no-op |
| boundary | `Get(256)` exactly a class | cap 256 |
| boundary | `Get(257)` | cap 1024 |
| boundary | `Get(maxClass + 1)` | fresh unpooled slice; `Stats.misses == 1`; `Put` drops it |
| boundary | `Get(0)` | smallest class, len 0 |
| corner | `MarshalAppend` grows the buffer past its class | `Encode` returns the grown buf; `Put` drops it; no aliasing with a pooled one |
| corner | 64 goroutines `Get` / `Put` under `-race` | no data race; all held slices distinct |
| corner | `testing.AllocsPerRun(1000, encode(medium))` after warmup with `pool=all` | ≤ 1 alloc (the `mem.Buffer` wrapper on gRPC; 0 on bus paths) |
| corner | `Msg.Get` after `Put` of a message with populated nested `Spec.Containers` | `len(Containers) == 0` and `Spec == nil` (documents the drop) |

Codec tests (`internal/codec/codec_test.go`) follow the same style:
round-trip equality for every fixture × codec, ProtoJSON special forms
(`Duration` → `"1.5s"`, `FieldMask` → `"replicas,labels"`, `bytes` → base64,
`int64` → string), unknown-field preservation (binary) vs rejection
(ProtoJSON), invalid input → typed error, `vtproto` output byte-identical to
`proto` for every fixture (the vtprotobuf conformance pattern from xtcp2).

## 8. Measurement model

### 8.1 Clocks

The MicroVMs run `systemd-timesyncd` only (no chrony, no `ptp_kvm`), which
is fine for RTT but not for one-way latency: timesyncd against a network NTP
source is typically off by hundreds of µs to milliseconds, and a one-way p50
on this LAN is ~100–300 µs. Rules:

- **RTT is always from the monotonic clock** on the driver
  (`time.Now()` retains a monotonic reading; the harness subtracts start /
  end `time.Time`s, never envelope timestamps).
- **Envelope wall-clock timestamps** (`client_send_time`,
  `server_receive_time`, `server_send_time`) are used for one-way estimates,
  correlation, and anomaly detection only.
- **Phase P0 adds a µs-quality clock to the VMs**: `boot.kernelModules =
  [ "ptp_kvm" ]` plus chrony with `refclock PHC /dev/ptp0 poll 2 dpoll -2
  offset 0` in `nix/k8s-module.nix` (host clock exposed through KVM, usually
  < 10 µs). timesyncd is disabled on the VMs when chrony is enabled.
- **Every run starts with a `ClockProbe` round per region**: 200 probes,
  NTP-style four-timestamp calculation, the sample with the minimum RTT wins,
  `offset_ns` and `uncertainty_ns = rtt_min / 2` are stored in `run.json`
  and exported as `mbbench_clock_offset_seconds{region}` /
  `mbbench_clock_uncertainty_seconds{region}`. The agent reports
  `clock_source` (`chrony-phc`, `timesyncd`, `unknown`) and
  `chrony_synced` from `chronyc tracking` at startup.
- **One-way percentiles are published only when
  `uncertainty < 0.1 × one_way_p50`**; otherwise the report prints the
  RTT numbers and a "one-way not reported: clock uncertainty X µs" line.
- **Never clamp.** Negative one-way values are kept in the histogram (HDR
  histograms take a signed offset via `hdrhistogram.New(-1e9, 60e9, 3)` for
  the one-way series) and counted in
  `mbbench_clock_anomalies_total{kind=negative_one_way|server_times_reversed|future_send}`.

### 8.2 Run modes

| Mode (`-mode`) | Loop | What it measures | Notes |
|---|---|---|---|
| `latency` | closed, `-inflight=1` | floor RTT per transport × codec × fixture | the headline latency number |
| `windowed` | closed, `-inflight=N` (16, 64, 256) | RTT vs concurrency, throughput knee | bidi / request-reply / RPC only |
| `openloop` | open, `-rate=R` | latency at a fixed offered load, **coordinated-omission-free** | sends at intended instants; a send later than 1 ms after its instant increments `late_sends_total`; latency is measured from the intended instant |
| `saturation` | open, ramp | max sustainable rate: rate doubles every 10 s until p99 > 10 × floor or `late_sends` > 1 % | the knee is reported, not the peak |
| `coldstart` | closed | first-request latency after connect / channel open / stream open | 20 fresh connections, reported as its own distribution |
| `fault` | open, fixed rate | behaviour during a broker / agent pod kill (reuses the chaos rotation from `nix/chaos-scripts.nix`) | integrity counters are the result; latency is reported but labelled |

Closed-loop histograms are additionally reported with a coordinated-omission
correction (`hdrhistogram`'s `RecordCorrectedValue` with the expected
interval) under the name `rtt_co_corrected`; the raw and corrected series
are both in the `.hgrm` files so the reader can see the difference rather
than trust the correction.

### 8.3 Warmup, windows, repeats

- Warmup is time-based (`-warmup=5s`) **and** condition-based: the cell does
  not start until the transport reports ready (gRPC connection `READY`,
  NATS connected + JetStream stream info ok, AMQP channel open + confirms
  enabled, Valkey consumer group exists, MQTT `token.Wait()` on subscribe)
  and 100 warmup messages have round-tripped.
- At the warmup boundary both driver and agent (via a `BenchService.Mark`
  call, or the agent's `/debug/memstats` endpoint) run `runtime.GC()` and
  snapshot `runtime.MemStats`; the same at cell end. Deltas go into
  `run.json`.
- Every cell is split into 1-second sub-windows so p99 over time is available
  as a series (this is what shows a GC pause or a Raft election as a spike
  instead of hiding it in the aggregate).
- `repeats` (default 5) per cell, **interleaved** (all cells once, then all
  cells again) rather than back-to-back, with the cell order shuffled per
  repeat from a seed recorded in `run.json`. The summary is the median across
  repeats with the median absolute deviation (MAD); a cell whose MAD > 20 % of
  the median is flagged "unstable" in `results.md`.
- The driver is pinned with `taskset` to host cores outside the VM vCPU set
  (from `constants.microvm` in the harness); the CPU set is recorded.
- Integrity (`-integrity=sha256`) is off in `latency`, `windowed`,
  `openloop`, `saturation` and on in `coldstart`, `fault`, and the
  correctness pass.

### 8.4 What is reported per cell

| Column | Source |
|---|---|
| `tier`, `transport`, `codec`, `fixture`, `pool`, `gc`, `mode`, `inflight`/`rate` | cell definition |
| `msgs`, `errors{kind}`, `throughput_msg_s`, `throughput_MiB_s` | driver counters |
| `rtt_p50/p90/p99/p999/max_us`, `rtt_co_corrected_p99_us` | driver HDR |
| `one_way_fwd_p50/p99_us`, `one_way_rev_p50/p99_us`, `clock_uncertainty_us`, `one_way_published` (bool) | envelope + clock probe |
| `server_duration_p50/p99_us` | agent HDR, fetched at cell end over `BenchService.Report` (or scraped from `mbbench_server_duration_seconds`) |
| `wire_bytes_per_msg` (req, resp) | `request_wire_bytes` / driver counters |
| `alloc_bytes_per_msg`, `allocs_per_msg`, `gc_cycles_per_s`, `gc_pause_p99_us`, `gc_cpu_fraction` — **for driver and agent** | `runtime.MemStats` deltas (exact) + Prometheus (`go_*`) |
| `cpu_us_per_msg` — driver, agent, broker | `process_cpu_seconds_total` deltas; broker from exporter CPU metrics (§11.5) |
| `broker_node_cpu_busy_pct`, `broker_throttled` (bool) | `node_cpu_seconds_total`, `container_cpu_cfs_throttled_periods_total` |
| `pool_hit_ratio` | `mbbench_pool_ops_total` |
| `missing`, `duplicate`, `reordered`, `redelivered`, `late_sends` | integrity counters |
| `hgrm` | path to `hgrm/<cell>-<repeat>.hgrm` |
| `grafana` | dashboard URL with `from`/`to`/`var-*` prefilled (§12.4) |

### 8.5 Run record (`run.json`)

```json
{
  "run_id": "01J9…",            // UUIDv7, also the Envelope.run_id
  "started_at": "…", "finished_at": "…",
  "git": { "rev": "…", "dirty": false },
  "versions": { "go": "1.26.7", "grpc": "v1.83.2", "protobuf": "v1.36.12",
                "protovalidate": "v0.14.0", "vtprotobuf": "v0.6.0", "buf": "1.72.0",
                "nats-server": "…", "rabbitmq": "…", "valkey": "…", "mosquitto": "…" },
  "host": { "cpu": "…", "driver_cpuset": "8-11", "vm_vcpus": { "cp0": "0-1", … }, "kernel": "…" },
  "codec_options": { "proto": { "UseCachedSize": true, "Deterministic": false, "DiscardUnknown": false },
                     "protojson": { "UseProtoNames": false, "EmitUnpopulated": false, "DiscardUnknown": false } },
  "grpc": { "SharedWriteBuffer": true, "ReadBufferSize": 65536, "WriteBufferSize": 65536, "compression": "none" },
  "gc_profiles": { "default": { "GOGC": "100" }, "limit": { "GOGC": "off", "GOMEMLIMIT": "384MiB" } },
  "clock": { "us-west-2": { "offset_ns": 1234, "uncertainty_ns": 4100, "source": "chrony-phc", "synced": true }, … },
  "corpus": { "seed": 42, "fixtures": { "medium": { "proto_bytes": 652, "protojson_bytes": 1710, "sha256": "…" }, … } },
  "matrix": { "order_seed": 7, "repeats": 5, "cells": [ … ] },
  "cells": [ { "id": "grpc_unary/proto/medium/all/default/latency", "repeat": 0,
               "summary": { … §8.4 columns … },
               "memstats": { "driver": { "Mallocs": …, "TotalAlloc": …, "NumGC": …, "PauseTotalNs": … },
                             "agent":  { … } },
               "hgrm": "hgrm/grpc_unary-proto-medium-all-default-latency-0.hgrm" } ]
}
```

`results.tsv` is the flat per-cell view of `cells[].summary`; `results.md`
is the human report (median ± MAD tables per mode, one table per tier,
scorecard, links). This mirrors the `bench-logs/` layout of
`docs/benchmarks.md` so the two harnesses read alike.

## 9. Deployment and harness

### 9.1 `region-agent` image (first Nix-built Go binary image)

`nix/images/region-agent.nix` uses the same `mkImage` helper as the bus
images (`nix/images/lib.nix` after the split), with `contents = [
pkgs.busybox regionAgent pkgs.cacert ]`, `config.Entrypoint =
[ "/bin/region-agent" ]`, `Env = [ "GOMAXPROCS=2" ]`, and the binary from
`nix/lib/mkGoBinary.nix` (static, `-trimpath`, `-ldflags "-s -w -X
main.version=…"`). Image name `messagebus.local/region-agent:<tag>` from
`constants.messageBus.workloads`, appended to `imageList` so the existing
preload module and `k8s-image-import` pick it up unchanged.

### 9.2 GitOps module `nix/gitops/env/workloads.nix`

- Namespace `workloads` (added to `env/base.nix`).
- **Four Deployments** `region-agent-<region>` (replicas 1) with
  `nodeSelector: kubernetes.io/hostname: k8s-<node>`, env `REGION`,
  `GOGC`, `GOMEMLIMIT` (the harness patches the last two per GC profile with
  `kubectl set env` and waits for rollout), `resources.limits: {cpu: "2",
  memory: 512Mi}`, `imagePullPolicy: Never`, readiness on `/healthz`,
  args from `constants` (`-grpc-addr=:9090 -metrics-addr=:9464 -nats=…
  -amqp=… -valkey-sentinels=… -mqtt=…`). Bus credentials come from the
  existing per-bus Secrets via `envFrom`.
- **Four NodePort Services** `region-agent-<region>` with
  `externalTrafficPolicy: Local` and `nodePort =
  nodePortGrpcBase + index` (30710–30713) so a host connection to
  `10.33.33.1x:3071x` always lands on that node's pod. One extra
  `region-agent-global` NodePort (30700, default policy) is the "global
  API" endpoint for the `WatchWorkload` and log demos.
- A headless Service `region-agents` for per-pod scraping
  (`region-agent-<region>.region-agents.workloads.svc:9464`).
- ArgoCD Application `workloads`, registered in `nix/gitops/default.nix`.
- `env/monitoring.nix` (→ `monitoring/prometheus.nix`) adds jobs
  `workloads-agents` (headless per-pod targets) and `proto-bench-driver`
  (`hostBridgeIP:9300–9307`, one port per concurrent `benchcli` process),
  both at `scrape_interval: 5s`.

### 9.3 `k8s-proto-bench` (host harness, `nix/proto-bench-scripts.nix`)

`writeShellApplication` following `bench-scripts.nix`: `kexec` through
`k8s-vm-ssh`, credentials via `kubectl get secret … | base64 -d`, Prometheus
over host `curl` on cp0:30900, "report, don't assert".

```
k8s-proto-bench [--transports=grpc,nats,…] [--codecs=proto,protojson,vtproto]
                [--fixtures=tiny,small,medium,large,max] [--pools=none,all]
                [--gc=default,limit] [--modes=latency,openloop] [--regions=us-west-2,…]
                [--rate=2000/s] [--duration=30s] [--warmup=5s] [--repeats=5]
                [--integrity=none|sha256] [--log-dir=./proto-bench-logs] [--dry-run]
```

Flow: (1) preflight — cluster reachable, agents Ready, brokers Ready, image
tag matches the binary's build; (2) clock probes per region; (3) build the
matrix, shuffle, write `run.json` skeleton; (4) per GC profile: patch agent
env, wait rollout; per cell: post a Grafana annotation (§12.4), run
`benchcli` under `taskset`, collect `memstats` from the agent, append to
`run.json`; (5) pull Prometheus range queries for the broker-side columns;
(6) render `results.tsv`, `results.md`, `hgrm/`; (7) print the summary and
the `proto-bench-logs/<run-id>/` path. Defaults in `constants.protoBench`.

### 9.4 Correctness pass

`k8s-proto-bench --modes=correctness` runs every fixture through every
transport × codec once with `-integrity=sha256 -validate=on`, and asserts
(this is the one place the harness *does* assert): decoded message
`proto.Equal` to the fixture, sha256 matches, validation passes for valid
fixtures and fails with the expected constraint id for each entry of the
invalid corpus (§14 demo step 3). It is fast (< 1 min) and is what CI would
run against a live cluster.

## 10. Best practices for fair comparison

Each rule names the mechanism that enforces it, so "fair" is a property of
the harness rather than of the reader's goodwill.

| # | Rule | Enforced by |
|---|---|---|
| 1 | **Compare within a semantics tier; only label across tiers.** Core NATS vs JetStream vs gRPC are different products, not different speeds. | every row carries `tier` (§2.3); `results.md` renders one table per tier and never a cross-tier ranking; the scorecard (durability, replication factor, ordering, redelivery, ack model, max payload, flow control) sits next to each table |
| 2 | **Identical bytes on every transport.** | the corpus is encoded once per codec at run start; the same slices (and their sha256, printed in `run.json`) go on every transport; the codec is never re-run per transport |
| 3 | **Official ProtoJSON only.** `encoding/json` on generated structs is shown once in the demo as an anti-pattern (wrong `oneof`, `int64` as number, `Timestamp` struct, `FieldMask` struct) and never benchmarked. `encoding/json/v2` is out of scope. | `internal/codec` has no `encoding/json` import; the demo step is a separate `benchcli codec --antipattern` print |
| 4 | **Codec options fixed and recorded.** | `run.json.codec_options` (§8.5); the `Codec` values are package-level `var`s, not flags |
| 5 | **Compression is a separate profile.** The main matrix runs uncompressed everywhere; a gRPC `gzip` profile shows JSON's size gap narrowing and its CPU gap widening. | `-compression` is gRPC-only and defaults to `none`; the report puts compressed cells in their own table |
| 6 | **CPU per message on all three parties** (driver, agent, broker) is the headline codec metric, not `ns/op` alone. | `process_cpu_seconds_total` deltas per cell for driver and agent; broker CPU from exporters (§11.5); broker-node network bytes per message from `node_network_*_bytes_total` |
| 7 | **Schema evolution is demonstrated, not asserted.** | a `v1+field` agent build (one extra field in `DeployResponse`) run against the `v1` driver: binary preserves unknown fields, ProtoJSON with `DiscardUnknown=false` rejects; `proto-breaking` is shown failing on a renamed field |
| 8 | **Reflection and human-readable tooling are part of the story.** | region-agent registers gRPC reflection; the devshell has `grpcurl`; `nats sub wl.*.deploy.json` shows ProtoJSON payloads |
| 9 | **Broker headroom is checked.** CFS throttling flattens tails silently. | broker StatefulSets already have CPU limits; each cell records broker-node CPU busy % and `container_cpu_cfs_throttled_periods_total`; cells over 80 % busy or with throttling are flagged in `results.md` |
| 10 | **Histogram buckets match the latency range.** The existing `mbclient_request_latency_seconds` uses the OTel SDK default boundaries (0, 5, 10, 25 … 10000). In seconds, every sub-5 s sample lands in one bucket, so the soak dashboard's p99 panel is interpolated noise. | `metrics.NewProvider` applies an explicit view (§11.1) to `mbbench_*` and `mbclient_*`; HDR files remain the source of exact percentiles, Prometheus is for trends and cross-party correlation |
| 11 | **No hidden warm caches or PGO.** | PGO is off (`-pgo=off` in `mkGoBinary`); cell order is shuffled and repeats interleaved (§8.3); connection reuse is the same across transports (one connection per driver process, opened before warmup) |
| 12 | **Ordering, duplicates, redelivery are data, not failures.** | integrity counters per transport (§8.4); the fault window is reported separately from steady state |
| 13 | **One variable at a time.** | a cell differs from its neighbour in exactly one of transport, codec, fixture, pool, gc, mode; `results.md` renders pairwise deltas for each axis |
| 14 | **Validation cost is its own column.** | `protovalidate` runs as a separate stage (§7.3) with `mbbench_validate_seconds`; `-validate=off` cells exist for every transport |
| 15 | **Same Go, same flags, same host.** | region-agent and `benchcli` build from the same `buildGoModule`; `versions.nix` is the only place a toolchain is named; `run.json.versions` records the result |

## 11. Metrics catalogue

### 11.1 Changes to `clients/internal/metrics`

`metrics.Setup` keeps its signature. A new constructor exposes what the
bench needs:

```go
// NewProvider builds the OTel MeterProvider + Prometheus registry used by
// Setup, registers the Go runtime and process collectors, applies views, and
// serves /metrics on addr. Existing callers keep using Setup.
func NewProvider(addr string, views ...sdkmetric.View) (*sdkmetric.MeterProvider, *prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(
			collectors.MetricsGC, collectors.MetricsMemory, collectors.MetricsScheduler,
		)),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	exp, err := otelprom.New(otelprom.WithRegisterer(reg))
	…
	views = append(views, LatencyBucketsView) // explicit boundaries for *_seconds histograms
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp), sdkmetric.WithView(views...))
	…
}

// LatencyBucketsView: 25 µs → 30 s, ×2 per step (≈ 21 buckets), applied to
// every histogram whose name ends in "_seconds" under mbclient_* and mbbench_*.
var LatencyBucketsView = sdkmetric.NewView(
	sdkmetric.Instrument{Name: "*_seconds", Kind: sdkmetric.InstrumentKindHistogram},
	sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
		Boundaries: []float64{25e-6, 50e-6, 100e-6, 200e-6, 400e-6, 800e-6, 1.6e-3, 3.2e-3,
			6.4e-3, 12.8e-3, 25.6e-3, 51.2e-3, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 30},
	}},
)
```

The view fixes the existing soak p99 panel as a side effect (rule 10).

### 11.2 Go runtime metrics (driver and agent)

From `collectors.NewGoCollector` with the runtime/metrics sets enabled:

| Metric | Use |
|---|---|
| `go_gc_heap_allocs_bytes_total`, `go_gc_heap_allocs_objects_total` | ÷ `rate(mbbench_messages_total)` = bytes / objects allocated per message (the pooling proof) |
| `go_gc_cycles_automatic_gc_cycles_total` | GC cycles/s |
| `go_gc_pauses_seconds` (histogram) | `histogram_quantile(0.99, …)` STW pause; correlate with the RTT heatmap |
| `go_cpu_classes_gc_total_cpu_seconds_total` / `go_cpu_classes_total_cpu_seconds_total` | **GC CPU fraction**, the single clearest pool-on vs pool-off signal |
| `go_memstats_heap_inuse_bytes`, `go_memstats_heap_idle_bytes`, `go_memstats_stack_inuse_bytes` | heap plateau vs `GOMEMLIMIT` |
| `go_gc_gogc_percent`, `go_gc_gomemlimit_bytes` | confirms the GC profile actually applied to the pod |
| `go_sched_latencies_seconds` (histogram) | scheduler latency p99: goroutine wake-up delay under load, visible in windowed mode |
| `go_sched_goroutines_goroutines`, `go_threads` | leak / fan-out sanity |

### 11.3 Process metrics

`collectors.NewProcessCollector`: `process_cpu_seconds_total` (CPU µs/msg),
`process_resident_memory_bytes`, `process_open_fds`, and on Linux
`process_network_receive_bytes_total` / `process_network_transmit_bytes_total`
(wire bytes per process, ÷ messages = bytes/msg including framing and
TLS-free HTTP/2 overhead, which the envelope's `request_wire_bytes` does not
include).

### 11.4 `mbbench_*` (driver `role=client`, agent `role=server`)

Labels on every instrument: `transport`, `codec`, `fixture`, `pool`,
`region`, `role`, `tier`; per-metric `kind` / `result` / `direction` /
`method` / `op` / `class`. Never `run_id` or `message_id` (the run is
selected by time range and annotations, §12.4).

| Metric | Type | Purpose |
|---|---|---|
| `mbbench_messages_total{result=sent\|received\|ok\|error}` | counter | throughput and error rate per party |
| `mbbench_errors_total{kind}` | counter | `timeout`, `nack`, `decode`, `validate`, `transport`, `late` |
| `mbbench_rtt_seconds` | histogram | driver monotonic RTT |
| `mbbench_one_way_seconds{direction=forward\|reverse}` | histogram | wall-clock one-way; only recorded when the clock gate passes |
| `mbbench_server_duration_seconds{method}` | histogram | receive → send on the agent |
| `mbbench_codec_seconds{op=marshal\|unmarshal}` | histogram | codec-only time per message |
| `mbbench_validate_seconds`, `mbbench_validate_failures_total{constraint_id}` | histogram, counter | protovalidate cost and which rule fired (constraint ids are a fixed small set) |
| `mbbench_wire_bytes_total{direction}` | counter | `rate/rate` = bytes per message |
| `mbbench_inflight` | gauge | outstanding requests / open window |
| `mbbench_late_sends_total`, `mbbench_intended_rate` | counter, gauge | open-loop coordinated-omission accounting |
| `mbbench_missing_total`, `mbbench_duplicate_total`, `mbbench_reordered_total`, `mbbench_redelivered_total` | counter | integrity per transport |
| `mbbench_ack_seconds` | histogram | publisher confirm / JetStream `PubAck` / `XADD` reply / MQTT `PUBACK` latency |
| `mbbench_clock_offset_seconds{region}`, `mbbench_clock_uncertainty_seconds{region}`, `mbbench_clock_anomalies_total{kind}` | gauge, gauge, counter | §8.1 |
| `mbbench_pool_ops_total{class,result=hit\|miss\|drop}` | counter | buffer-pool effectiveness |
| `mbbench_stream_active{service}` | gauge | open server / client / bidi streams on the agent |
| `mbbench_cell_info{transport,codec,fixture,pool,gc,mode}` | gauge = 1 | the active cell, for state-timeline panels and joins |

### 11.5 gRPC and broker-side metrics

- **gRPC standard RPC metrics**: `otelgrpc.NewServerHandler()` /
  `otelgrpc.NewClientHandler()` are chained with the envelope
  `stats.Handler` (§7.4) and give `rpc_server_duration`,
  `rpc_server_request_size`, `rpc_server_response_size`,
  `rpc_server_requests_per_rpc` (stream sizes) by `rpc_method` and
  `rpc_grpc_status_code`. Tracing stays off (rule: no full materialisation).
- **NATS** (existing per-pod `nats-exporter`): `gnatsd_varz_in_msgs`,
  `out_msgs`, `in_bytes`, `out_bytes`, `gnatsd_varz_cpu`, `gnatsd_varz_mem`,
  `jetstream_stream_total_bytes`, `jetstream_consumer_num_pending`,
  `jetstream_consumer_num_ack_pending`.
- **RabbitMQ** (built-in Prometheus plugin): `rabbitmq_channel_messages_published_total`,
  `rabbitmq_channel_messages_confirmed_total`, `rabbitmq_queue_messages_ready`,
  `rabbitmq_queue_messages_unacked`, `rabbitmq_process_resident_memory_bytes`,
  `erlang_vm_statistics_run_queues_length`, `rabbitmq_raft_log_commit_latency_seconds`.
- **Valkey** (redis-exporter): `redis_commands_total{cmd=xadd|xreadgroup|xack}`,
  `redis_commands_duration_seconds_total{cmd}`, `redis_stream_length`,
  `redis_memory_used_bytes`, `redis_used_cpu_sys`, `redis_used_cpu_user`.
- **Mosquitto has no exporter today.** Add `clients/mqtt/sysexporter`, a
  ~150-line Go bridge subscribing to `$SYS/#` and exposing
  `mosquitto_messages_received_total`, `mosquitto_messages_sent_total`,
  `mosquitto_bytes_received_total`, `mosquitto_bytes_sent_total`,
  `mosquitto_clients_connected`, `mosquitto_load_messages_received_1min`,
  `mosquitto_heap_current_bytes`, run as a sidecar in
  `nix/gitops/env/mqtt.nix` (same shape as the NATS exporter sidecar) and
  scraped by a `mqtt` job.
- **Node** (existing node-exporter): `node_cpu_seconds_total`,
  `node_network_receive_bytes_total` / `transmit_bytes_total` on the broker's
  node, `container_cpu_cfs_throttled_periods_total` from the kubelet cAdvisor
  endpoint for the broker pods.

## 12. Grafana visualisation

### 12.1 Nix-generated dashboards, own ConfigMap

The community dashboards ConfigMap is already ~926 KB against etcd's 1 MB
object limit, so the new dashboard goes in **its own ConfigMap**
(`grafana-dashboard-protobench`), mounted through a second provisioning
entry. `nix/gitops/env/monitoring.nix` is split into small files (the ≤ 150
line rule from §5):

```
nix/gitops/env/monitoring/
  default.nix            imports + list concatenation; same export as today
  prometheus.nix         ConfigMap + Deployment + Service; mkTargets; job list
  grafana.nix            Deployment + Service + provisioning ConfigMaps (datasource, dashboard providers)
  dashboards/lib.nix     mkDashboard { uid; title; vars; rows; } / mkRow / mkPanel / mkStat / mkHeatmap → attrset
  dashboards/soak.nix    today's soak dashboard, ported to the helpers (same uid, same panels)
  dashboards/protobench.nix   the new dashboard (§12.3)
```

`mkPanel { title; exprs = [ { expr; legend; } … ]; unit ? "s"; type ?
"timeseries"; w ? 12; h ? 8; }` produces a Grafana panel attrset; `mkRow`
assigns `gridPos` sequentially; `mkDashboard` emits `builtins.toJSON`.
Dashboards therefore diff as Nix rather than as a 30 KB JSON string, and
`k8s-render-manifests --check` covers them.

### 12.2 Template variables

Multi-select, `includeAll`, from `label_values(mbbench_messages_total, <label>)`:
`transport`, `codec`, `fixture`, `pool`, `region`, `tier`, plus `gc` and
`mode` from `mbbench_cell_info`. Every expression carries
`{transport=~"$transport", codec=~"$codec", fixture=~"$fixture",
pool=~"$pool", region=~"$region"}`.

### 12.3 Rows and key panels

| Row | Panels (type) | Expression sketch |
|---|---|---|
| 1 Run overview | msgs/s (stat); RTT p50 / p99 (stat); wire bytes/msg (stat); alloc bytes/msg (stat); CPU µs/msg client / agent / broker (stat); **active cell** (state-timeline) | `sum(rate(mbbench_messages_total{result="ok"}[10s]))`; `histogram_quantile(0.99, sum by (le,transport)(rate(mbbench_rtt_seconds_bucket[10s])))`; `sum(rate(mbbench_wire_bytes_total[10s]))/sum(rate(mbbench_messages_total{result="sent"}[10s]))`; `sum(rate(go_gc_heap_allocs_bytes_total{job="proto-bench-driver"}[10s]))/…`; `1e6*rate(process_cpu_seconds_total[10s])/rate(mbbench_messages_total{result="ok"}[10s])`; `mbbench_cell_info == 1` |
| 2 Latency | RTT p50 / p99 / p99.9 by transport; **RTT heatmap** over time; one-way forward / reverse by region; server duration p99 by method; ack / confirm p99 by transport | `histogram_quantile(...)` on the four `*_seconds_bucket` series; heatmap from `sum by (le)(rate(mbbench_rtt_seconds_bucket[5s]))` with `format: heatmap` |
| 3 Codec comparison | encoded bytes/msg by codec × fixture (bar gauge); marshal / unmarshal µs by codec; CPU µs/msg per party by codec; broker-node network bytes per message by codec | `… by (codec, fixture)`; `histogram_quantile(0.5, sum by (le,codec,op)(rate(mbbench_codec_seconds_bucket[30s])))`; `rate(node_network_transmit_bytes_total{instance=~"$broker_node"}[10s]) / on() group_left sum(rate(mbbench_messages_total{result="sent"}[10s]))` |
| 4 Memory & GC (pooling proof) | alloc bytes/s and objects/s by `pool`; GC cycles/s; GC pause p99; **GC CPU fraction**; heap in-use vs `GOMEMLIMIT`; pool hit ratio by class; scheduler latency p99 | `rate(go_gc_cycles_automatic_gc_cycles_total[10s])`; `histogram_quantile(0.99, rate(go_gc_pauses_seconds_bucket[30s]))`; `rate(go_cpu_classes_gc_total_cpu_seconds_total[30s]) / rate(go_cpu_classes_total_cpu_seconds_total[30s])`; `go_memstats_heap_inuse_bytes` and `go_gc_gomemlimit_bytes` on one panel; `sum by (class)(rate(mbbench_pool_ops_total{result="hit"}[30s])) / sum by (class)(rate(mbbench_pool_ops_total[30s]))` |
| 5 Correctness & flow control | errors by kind; late sends vs intended rate; in-flight; missing / duplicate / reordered / redelivered; clock offset and uncertainty per region; clock anomalies | counters as `rate(...)`, gauges raw |
| 6 Broker side | one sub-row per bus from §11.5 plus node CPU busy % for the broker's node and CFS throttling | `1 - avg by (instance)(rate(node_cpu_seconds_total{mode="idle"}[10s]))`; `rate(container_cpu_cfs_throttled_periods_total{namespace=~"nats\|rabbitmq\|valkey\|mqtt"}[10s])` |
| 7 Semantics scorecard | text panel | the §2.3 tier table in markdown |

All histogram panels use the explicit buckets from §11.1; the RTT heatmap
is the panel that shows GC pauses and Raft elections as horizontal bands
without any per-message tracing.

### 12.4 Harness ↔ Grafana integration

Grafana runs with anonymous Admin, so the harness can write without a token:

- At every cell start / stop and every fault event the harness posts
  `POST http://10.33.33.10:30300/api/annotations` with `{"dashboardUID":
  "protobench", "time": …, "timeEnd": …, "tags": ["protobench", "cell:<id>",
  "run:<run_id>"], "text": "<cell id> repeat <n>"}` (region annotations, one
  per cell); fault events use the tag `fault`.
- `results.md` links every cell to
  `http://10.33.33.10:30300/d/protobench/?from=<ms>&to=<ms>&var-transport=…&var-codec=…&var-fixture=…&var-pool=…&var-region=…`
  so a row in the report opens exactly that window with the variables set.
- The soak harness gets the same annotation helper for its `events.tsv`
  (optional, small).
- Prometheus scrape interval is 5 s for the `workloads-agents` and
  `proto-bench-driver` jobs (cells last 30 s); retention unchanged.

## 13. Implementation phases

Each phase is one PR, independently reviewable, and leaves `nix flake check`
green.

| Phase | Scope | Adds | Exit criteria |
|---|---|---|---|
| **P0 scaffold** | `nix/versions.nix`, `nix/lib/{goModules,mkGoBinary}.nix`, `nix/protos/*.nix` (FOD, lint, breaking, gen-drift, `regen-protos`), `nix/checks/*.nix`, `clients/buf.yaml`, `clients/buf.gen.yaml`, all six `.proto` files, checked-in `gen/go/…`, `gen/descriptors/workloads.binpb`, devshell tools, chrony + `ptp_kvm` in `nix/k8s-module.nix` | `checks.proto-lint`, `proto-breaking`, `proto-gen-drift`, `go-vet`, `gofmt` | `nix flake check` passes; `nix run .#regen-protos` is a no-op on a clean tree; `chronyc tracking` on a VM shows the PHC source |
| **P1 codec + pool** | `internal/{pool,codec,envelope,corpus}`, `vt.go` profile, micro-benchmarks, table-driven tests (§7.7), `benchcli codec` | `nix run .#clients-bench` rows `codec × fixture × pool` | `AllocsPerRun` ≤ 1 with `pool=all`; vtproto bytes == proto bytes for every fixture |
| **P2 gRPC + agent** | `internal/transport/{grpc}`, `region-agent` (gRPC services, reflection, `/metrics`, `/healthz`), `nix/images/region-agent.nix`, `nix/gitops/env/workloads.nix`, Prometheus jobs, `metrics.NewProvider` + collectors + bucket view | `packages.region-agent-image`, `benchcli grpc`, `benchcli clockprobe` | `grpcurl -plaintext 10.33.33.10:30710 list` works; `go_gc_*` scraped from four agents; `latency` mode produces `.hgrm` |
| **P3 bus transports** | `internal/transport/{nats,rabbitmq,valkey,mqtt}`, agent responders / consumers, `clients/mqtt/sysexporter` sidecar, NATS `max_payload` bump if needed | `benchcli nats\|rabbitmq\|valkey\|mqtt` | correctness pass (§9.4) green on all transports × codecs |
| **P4 harness + dashboards + docs** | `nix/proto-bench-scripts.nix` (`k8s-proto-bench`), `monitoring/` split + `dashboards/{lib,soak,protobench}.nix`, annotations, `results.md` renderer, `docs/proto-bench.md` (how to run and read), README pointers | `apps.k8s-proto-bench`, dashboard `protobench` | a full default matrix run completes and `results.md` renders with Grafana links; soak dashboard renders identically after the port |

Stretch (not scheduled): `encoding/json/v2` codec profile, gRPC `gzip`
profile run in the default matrix, TLS on gRPC, a `v1+field` agent build for
the schema-evolution demo as a flake variant.

## 14. Demo sequence

Each step is a command and the thing to point at. Steps 1–5 need no cluster.

1. **Schema tour** — `cat clients/proto/workloads/v1/workload.proto`; then
   `nix run .#regen-protos` and `git status` (no drift). Point at: `oneof`
   placement policy, `FieldMask` on update, CEL rules on `WorkloadSpec`.
2. **Generated types** — `sed -n '/type WorkloadSpec struct/,/^}/p'
   clients/gen/go/workloads/v1/workload.pb.go`; the `isPlacement_Policy`
   interface; `DeployRequestFromVTPool` in `*_vtproto.pb.go`.
3. **Validation** — `benchcli codec --validate-demo`: a dense `DeployRequest`
   passes; then mutations printed with their constraint ids:
   `limits < requests` (`resources.limits_ge_requests`), duplicate container
   name, `CRON` without `schedule`, `update_mask` with an unknown path,
   `request_sha256` of length 31, `max_rtt` of 6 s. Point at: the failure
   message includes the field path and the CEL id.
4. **Wire size** — `benchcli codec --sizes`: table of fixture × codec bytes
   (§3.8) plus the ProtoJSON forms of `Duration`, `FieldMask`, `bytes`,
   `int64`, `Timestamp` next to the binary field bytes. Then
   `--antipattern`: `encoding/json` on the same struct (broken `oneof`,
   `int64` as number, `Timestamp` as object).
5. **Codec micro-benchmarks** — `nix run .#clients-bench -- -run=NONE
   -bench='Codec/.*/medium'` with `-pool=none` then `-pool=all`: `ns/op`,
   `B/op`, `allocs/op` side by side; then the `vtproto` rows.
6. **Clock check** — `benchcli clockprobe -region=all`: offset and
   uncertainty per region; whether one-way numbers will be published.
7. **Request/response floor** — `benchcli grpc -mode=latency` vs
   `benchcli nats -mode=latency` (same tier C, same fixture, same codec).
   Open the Grafana link printed for each: RTT heatmap and the active-cell
   timeline.
8. **Concurrency** — `benchcli grpc -transport=bidi -inflight=1` vs
   `-inflight=64` vs `-mode=openloop -rate=20000/s`; point at
   `late_sends` and the coordinated-omission-corrected column.
9. **Telemetry fan-in (tier B)** — one command per transport:
   gRPC client-stream, JetStream, RabbitMQ quorum, Valkey stream, MQTT QoS 1,
   all with `-fixture=medium -codec=proto`; then the same with `protojson`.
   Open the Codec-comparison row: bytes/msg and CPU µs/msg per party.
10. **Pooling proof** — `-pool=none` vs `-pool=all` on gRPC client-stream
    with `large`; open the Memory & GC row: alloc bytes/msg, GC cycles/s,
    GC CPU fraction; then repeat under `--gc=limit` and show the heap
    plateau against `GOMEMLIMIT`.
11. **Logs** — `benchcli grpc -transport=server_stream -fixture=max`
    (960 KiB `LogChunk`s) and the JetStream fan-out profile; then
    `grpcurl -d '{"selector":{…},"follow":true}' … LogService/StreamLogs`
    for the human-readable path.
12. **Schema evolution** — run the `v1+field` agent against the `v1` driver
    with `proto` (unknown field preserved, shown via `protoreflect`) and
    `protojson` (rejected); then `git mv` a field name and watch
    `nix build .#checks.x86_64-linux.proto-breaking` fail.
13. **Faults** — `k8s-proto-bench --modes=fault --transports=nats,rabbitmq`
    while the chaos rotation kills a broker pod: integrity counters per
    transport, annotations on the dashboard, the fault window in
    `results.md`.

## 15. Acceptance criteria

- `nix flake check` gates: `proto-lint`, `proto-breaking`, `proto-gen-drift`,
  `go-vet`, `gofmt`, `cli-tests` (all table-driven tests incl. §7.7), and
  `k8s-render-manifests --check` for the gitops output.
- `nix run .#regen-protos` on a clean tree changes nothing; the checked-in
  `buf.lock` resolves through the FOD with no network.
- `nix build .#region-agent-image` produces an image the existing preload
  and `k8s-image-import` load; four agents Ready, one per node, reachable on
  30710–30713 with `externalTrafficPolicy: Local` confirmed by
  `responder_id` matching the targeted region.
- The correctness pass (§9.4) is green on every transport × codec × fixture.
- A default matrix run (`k8s-proto-bench` with defaults) completes in under
  90 minutes and produces `run.json`, `results.tsv`, `results.md`, `hgrm/`,
  with every cell carrying `tier` and a working Grafana link.
- With `pool=all`, `allocs_per_msg` on the agent's gRPC client-stream path
  for `medium` is ≤ 2 (official codec) and ≤ 1 (vtproto) in steady state,
  and GC CPU fraction is lower than `pool=none` in every cell; the report
  shows the deltas.
- One-way latency is reported only when the clock gate passes, and the
  gate's inputs are in `run.json`.
- Dashboard `protobench` renders from a Nix-generated ConfigMap; the soak
  dashboard, ported to the same helpers, renders the same panels as today;
  the soak p99 panel shows real percentiles after the bucket view.
- No new `nix/*.nix` file exceeds ~150 lines; `docs/proto-bench.md`
  explains how to run and read a result in one page.

## 16. References

- Protobuf language guide (proto3, `optional`, `oneof`, maps, well-known
  types): <https://protobuf.dev/programming-guides/proto3/>
- Protobuf-Go API (`proto.MarshalOptions.MarshalAppend`,
  `UnmarshalOptions.Merge`, `protojson`):
  <https://pkg.go.dev/google.golang.org/protobuf@v1.36.12/proto>,
  <https://pkg.go.dev/google.golang.org/protobuf@v1.36.12/encoding/protojson>
- ProtoJSON mapping: <https://protobuf.dev/programming-guides/json/>
- Protovalidate (CEL rules, Go runtime):
  <https://buf.build/docs/protovalidate/>,
  <https://pkg.go.dev/buf.build/go/protovalidate@v0.14.0>
- Buf v2 config, lint, breaking, managed mode:
  <https://buf.build/docs/configuration/v2/buf-yaml/>,
  <https://buf.build/docs/configuration/v2/buf-gen-yaml/>
- grpc-go: `encoding.CodecV2` and `mem`:
  <https://pkg.go.dev/google.golang.org/grpc@v1.83.2/encoding>,
  <https://pkg.go.dev/google.golang.org/grpc@v1.83.2/mem>,
  <https://pkg.go.dev/google.golang.org/grpc@v1.83.2/experimental>;
  stats handler: <https://pkg.go.dev/google.golang.org/grpc@v1.83.2/stats>;
  reflection: <https://github.com/grpc/grpc-go/blob/master/Documentation/server-reflection-tutorial.md>
- vtprotobuf (`features=pool`, `ResetVT`, unsafe unmarshal caveat):
  <https://github.com/planetscale/vtprotobuf>
- otelgrpc stats handlers:
  <https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc>
- Prometheus Go collectors and `runtime/metrics` names:
  <https://pkg.go.dev/github.com/prometheus/client_golang/prometheus/collectors>,
  <https://pkg.go.dev/runtime/metrics>
- HdrHistogram for Go: <https://github.com/HdrHistogram/hdrhistogram-go>
- Coordinated omission (Gil Tene): <https://www.youtube.com/watch?v=lJ8ydIuPFeU>
- `sync.Pool` large-buffer retention (Go issue #23199):
  <https://github.com/golang/go/issues/23199>
- chrony with a KVM PTP clock: <https://chrony-project.org/doc/4.5/chrony.conf.html#refclock>
- NATS JetStream, RabbitMQ quorum queues, Valkey Streams, MQTT 5 QoS:
  <https://docs.nats.io/nats-concepts/jetstream>,
  <https://www.rabbitmq.com/docs/quorum-queues>,
  <https://valkey.io/topics/streams-intro/>,
  <https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html>
- Repo docs this builds on: `docs/benchmarks.md` (existing client
  benchmarks), `README.md` (cluster access, GitOps workflow),
  `nix/chaos-scripts.nix` (fault rotation).
