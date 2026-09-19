# message-bus-examples

Clustered **message buses** — NATS, RabbitMQ, MQTT (Mosquitto), and
ValKey — running on a self-contained, HA Kubernetes cluster built from
Nix, with small **Go pub/sub CLI clients** you run from the host.

The cluster (3 control planes + 1 worker) runs as lightweight QEMU
MicroVMs. PKI is generated at build time and baked into the VM images;
Cilium replaces kube-proxy; workloads are deployed GitOps-style via
ArgoCD from rendered manifests. Every broker runs from a **Nix-built OCI
image preloaded into containerd** — no upstream vendor images, no
in-cluster registry, no network pull.

> Adapted from [`nix-k8s-examples`](https://github.com/randomizedcoder/nix-k8s-examples):
> the cluster/PKI/GitOps scaffolding is reused; the database/app workloads
> were replaced with the four message buses and their clients.

---

## Architecture

```
Host ── k8sbr0 (bridge) ─┬─ k8stap0 → cp0  10.33.33.10  (etcd, apiserver, scheduler, CM)
   haproxy:6443 ──┐       ├─ k8stap1 → cp1  10.33.33.11  (etcd, apiserver, scheduler, CM)
   (LB → 3 CPs)   │       ├─ k8stap2 → cp2  10.33.33.12  (etcd, apiserver, scheduler, CM)
                  └───────┴─ k8stap3 → w3   10.33.33.13  (kubelet, containerd)

Each node's containerd is preloaded at boot with the 4 Nix-built bus
images (from the 9p-shared /nix/store). Each bus is a 3-pod StatefulSet
spread across nodes with podAntiAffinity, exposed to the host on a
NodePort.
```

| Bus | Deployment | HA mechanism | Host NodePort |
|-----|-----------|--------------|---------------|
| **NATS** | 3-node JetStream cluster (hub) + 1 leaf node | Raft re-election + client reconnect | `30422` (client 4222), `30423` (leaf 4222) |
| **RabbitMQ** | 3-node cluster (k8s peer discovery, quorum queues) | `pause_minority` + queue leader re-election | `30567` (AMQP), `30672` (mgmt UI) |
| **MQTT** | 3× Mosquitto, full-mesh bridged (MQTT 3.1.1) | surviving brokers keep serving; client reconnect | `30883` (MQTT 1883) |
| **ValKey** | 1 primary + 2 replicas + 3 Sentinels | Sentinel auto-failover | `30637` (client 6379) |

An in-cluster **observability stack** (pinned to cp0) rounds this out:

| Service | URL | Notes |
|---------|-----|-------|
| **Grafana** | <http://10.33.33.10:30300> | anonymous Admin (no login); dashboards below |
| **Prometheus** | <http://10.33.33.10:30900> | scrape targets + PromQL |

(NodePorts, so any node IP works — cp0 `10.33.33.10` is the stable one, never
killed by the soak's fault rotation.)

Prometheus scrapes node_exporter on every VM (`:9100`), a
`prometheus-nats-exporter`, RabbitMQ's `rabbitmq_prometheus` plugin, a
`redis_exporter` sidecar in each ValKey pod, Cilium/Hubble, and the soak
clients' OTel `/metrics`. Grafana ships a provisioned Prometheus datasource and
these dashboards (the soak one is custom; the rest are pinned community
dashboards fetched from grafana.com by revision + sha256):

| Dashboard | Source | Covers |
|-----------|--------|--------|
| **Message-bus Soak** (uid `soak`) | custom | per-client publish/receive/loss/reconnects/latency + infra |
| **NATS Servers** | [2279](https://grafana.com/grafana/dashboards/2279) | NATS hub/leaf server metrics |
| **NATS JetStream** | [14725](https://grafana.com/grafana/dashboards/14725) | streams, consumers, R3 state |
| **RabbitMQ** | [10991](https://grafana.com/grafana/dashboards/10991) | queues, messages, cluster |
| **Valkey** | [24733](https://grafana.com/grafana/dashboards/24733) | Valkey/Redis instance metrics |
| **Redis Exporter** | [14091](https://grafana.com/grafana/dashboards/14091) | redis_exporter overview |
| **Node Exporter Full** | [1860](https://grafana.com/grafana/dashboards/1860) | per-VM CPU/mem/net/disk + systemd units & processes |

See [Soak test](#soak-test).

---

## Design decision: Nix-built OCI images

Where nixpkgs provides the broker, we **build the container image with
Nix (`dockerTools`)** instead of pulling a vendor image
(`nix/images/*.nix`). All four buses are packaged in nixpkgs
(`nats-server`, `rabbitmq-server`, `mosquitto`, `valkey`), so all four run
Nix-built images. Why:

- **Determinism / reproducibility** — the image is a pure function of the
  pinned `nixpkgs`. Same inputs → byte-identical closure, every time.
- **Customisation** — each image carries exactly the broker binary + its
  runtime closure + a tiny `busybox` userland for the init scripts, and
  nothing else. No package manager, no shell we didn't ask for.
- **Supply chain** — we build from source-pinned packages rather than
  trusting an opaque vendor layer.

**Delivery without a registry.** The image tarballs live in the host
`/nix/store`, which every MicroVM mounts read-only over 9p. A systemd
oneshot (`nix/image-preload-module.nix`) imports them into containerd's
`k8s.io` namespace *before* kubelet starts; the StatefulSets reference
them by exact `name:tag` with `imagePullPolicy: Never`. Fully offline and
deterministic.

### Image sizes (Nix-built vs upstream)

Measured on this repo (`du -h` of the gzipped image tar / `nix path-info
-Sh` closure). Upstream figures are the nearest Alpine/Debian vendor tag
(**approximate**, compressed):

| Bus | Nix image (this repo) | Upstream (approx.) | Base |
|-----|----------------------:|-------------------:|------|
| NATS      | **21 MB** | ~16 MB (`nats:*-alpine`) | busybox + glibc |
| MQTT      | **68 MB** | ~9 MB (`eclipse-mosquitto:2`) | busybox + glibc |
| ValKey    | **68 MB** | ~15 MB (`valkey:*-alpine`) | busybox + glibc |
| RabbitMQ  | **487 MB** | ~250 MB (`rabbitmq:4-management`) | glibc + Erlang |

**On size — a deliberate tradeoff.** Against **Alpine/musl** vendor images
the Nix images are *not* smaller: each bundles a full **glibc** closure and
does not share a common vendor base layer. That larger size is by design —
glibc is the complete, standard C library with the more advanced/complete
feature set (NSS, full locale and threading support, wider syscall and
compatibility coverage), whereas Alpine ships **musl**, a deliberately
minimal libc that trades some of those features for size. We prefer the
full-featured, standard runtime over the smallest possible image. The other
wins stand regardless: reproducibility, customisation, and supply-chain
transparency. (Static/musl builds could shrink these — easy for the
Go-based `nats-server`, harder for Mosquitto/ValKey/Erlang — but that is
explicitly not the goal here.)

Reproduce the numbers yourself:

```bash
nix build .#nats-image && du -h ./result && nix path-info -Sh ./result
# upstream, for comparison:
skopeo inspect --raw docker://docker.io/library/eclipse-mosquitto:2 | jq '[.layers[].size]|add'
```

---

## Quick start

```bash
nix develop                                # dev shell (kubectl, helm, go, bus CLIs, …)
nix run .#k8s-check-host                    # verify host prereqs (tun, vhost-net, bridge)
sudo nix run .#k8s-network-setup            # bridge + 4 TAPs + NAT + haproxy LB
nix run .#k8s-gen-secrets                   # generate RabbitMQ/ValKey/SSH secrets → ./secrets/
nix run .#k8s-start-all                     # build + start all 4 VMs (bootstrap auto-runs on cp0)

# watch the first-boot bootstrap (Cilium → base → ArgoCD → Secrets → Applications)
nix run .#k8s-vm-ssh -- --node=cp0 journalctl -u k8s-gitops-bootstrap -f

# verify the buses are up
nix run .#k8s-vm-ssh -- --node=cp0 kubectl get pods -A
```

### Wipe and rebuild

```bash
nix run .#k8s-cluster-rebuild               # wipe data volumes + restart all VMs
```

---

## Using the buses (pub/sub from the host)

Each bus is reachable on any node IP at its NodePort; the Go CLIs default
to `127.0.0.1:<nodePort>` — adjust `-addr` to a node IP (e.g.
`10.33.33.10:30422`) when running off-box. Run a subscriber in one shell
and publish from another:

```bash
# NATS (no auth)
nix run .#nats-sub
nix run .#nats-pub -- -msg "hello nats"

# MQTT / Mosquitto (no auth)
nix run .#mqtt-sub -- -subject demo/topic
nix run .#mqtt-pub -- -subject demo/topic -msg "hello mqtt"

# ValKey (password from the valkey-credentials Secret)
export VALKEY_PASS=$(nix run .#k8s-vm-ssh -- --node=cp0 \
  kubectl -n valkey get secret valkey-credentials -o jsonpath='{.data.password}' | base64 -d)
nix run .#valkey-sub -- -pass "$VALKEY_PASS"
nix run .#valkey-pub -- -pass "$VALKEY_PASS" -msg "hello valkey"

# RabbitMQ (admin password from the rabbitmq-credentials Secret)
export RABBITMQ_PASS=$(nix run .#k8s-vm-ssh -- --node=cp0 \
  kubectl -n rabbitmq get secret rabbitmq-credentials -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' | base64 -d)
nix run .#rabbitmq-sub -- -pass "$RABBITMQ_PASS"
nix run .#rabbitmq-pub -- -pass "$RABBITMQ_PASS" -msg "hello rabbitmq"
```

Each binary takes `pub`/`sub` as its first argument; the `nix run
.#<bus>-<pub|sub>` apps are thin wrappers that prepend it. Sources live in
`clients/<bus>/<bus>cli/` (one small `main.go` per bus).

### Scripting flags

Beyond `-addr`/`-subject`/`-msg`/`-user`/`-pass`, every client shares a set
of flags (in `clients/internal/cli`) that make the CLIs scriptable — useful
for load, demos, and tests:

| Flag | Applies to | Meaning |
|------|-----------|---------|
| `-count n` | pub / sub | pub: send `n` messages (default 1); sub: **exit after** `n` messages (0 = run until Ctrl-C) |
| `-rate N/s` | pub | throttle to `N` messages/second (fractional ok, e.g. `0.5/s`); default is unthrottled |
| `-timeout d` | sub | exit after duration `d` (e.g. `10s`); 0 = run until Ctrl-C |
| `-json` | sub | emit one `{"ts","subject","data"}` object per line instead of a human line |

When a publisher sends more than one message, a 1-based sequence number is
appended to `-msg` (`hello 1`, `hello 2`, …) so each payload is distinct.
`-count`/`-timeout` make a subscriber self-terminating, so a whole pub/sub
exchange fits in a script:

```bash
# publish 100 messages at 10/s; subscriber collects 100 (or gives up after 30s)
nix run .#nats-sub -- -subject bench -count 100 -timeout 30s -json > got.jsonl &
nix run .#nats-pub -- -subject bench -count 100 -rate 10/s
wait; wc -l got.jsonl
```

### NATS concept examples

Beyond the pub/sub CLIs, a set of small **self-contained NATS demos** each
mirror one page of the [NATS core concepts](https://docs.nats.io/concepts)
docs. Each runs all its parties in one process, prints a labeled trace,
asserts the expected outcome, and exits — so `nix run .#<demo>` is a single,
reproducible illustration of one concept. Every demo has its own `README.md`
(topology diagram + explanation) in its source folder.

| Demo | Concept | Source |
|------|---------|--------|
| `nix run .#nats-subjects` | [Subjects & hierarchies](https://docs.nats.io/concepts/subjects) — exact / `*` / `>` wildcard matching | [`clients/nats/subjects`](clients/nats/subjects) |
| `nix run .#nats-request-reply` | [Request-Reply](https://docs.nats.io/concepts/request-reply) — synchronous RPC over `_INBOX` reply subjects | [`clients/nats/request-reply`](clients/nats/request-reply) |
| `nix run .#nats-queue-groups` | [Queue Groups](https://docs.nats.io/concepts/queue-groups) — one message per group member (load balancing) | [`clients/nats/queue-groups`](clients/nats/queue-groups) |
| `nix run .#nats-leaf` | [Topologies → Leaf Nodes](https://docs.nats.io/concepts/topologies) — subject interest bridged hub ⇄ leaf | [`clients/nats/leaf`](clients/nats/leaf) |

```bash
# each defaults to 127.0.0.1:30422; pass a node IP when off-box
nix run .#nats-subjects      -- -addr 10.33.33.10:30422
nix run .#nats-request-reply -- -addr 10.33.33.10:30422 -count 5
nix run .#nats-queue-groups  -- -addr 10.33.33.10:30422 -count 12 -workers 4
# the leaf demo attaches to both the hub (:30422) and the leaf (:30423)
nix run .#nats-leaf          -- -hub-addr 10.33.33.10:30422 -leaf-addr 10.33.33.13:30423
```

Unlike the `pub`/`sub` CLIs these demos take no subcommand — the `nix run`
app is the binary directly.

### Durability & HA flags (opt-in)

By default the CLIs are *liveness* demos — they move a message and exit. Three
opt-in, per-bus flags turn them into *correctness* demos that exercise each
bus's durability / failover guarantee. All are additive: the default
subcommands, `-addr`/`-subject`/`-msg`, and env-var creds are unchanged (the
chaos harness keeps working), and no new Go dependencies are pulled in.

| Flag | Bus | What it does |
|------|-----|--------------|
| `-jetstream` | NATS | Uses **JetStream** instead of core NATS: `pub` idempotently creates a persistent, 3-replica (`FileStorage`, `Replicas: 3`) stream named `MBEX_<SUBJECT>` and publishes with an ack; `sub` binds a **durable consumer** and acks each message. Messages persist and replay across restarts / failover. |
| `-durable` | RabbitMQ | Switches from the default ephemeral fanout to a durable **quorum queue** `<subject>.quorum` (`x-queue-type: quorum`, replicated across all 3 nodes) with persistent messages. This is work-queue (shared, competing-consumer) semantics that survives a broker/node loss. |
| `-sentinels h:p,…` | ValKey | Connects via a go-redis **FailoverClient**: it asks the listed Sentinels for the current primary and always connects there, **following automatic failover**. Every `PUBLISH` reaches the primary (and so fans out to all replicas' subscribers). |

RabbitMQ **publisher confirms are always on** — every `pub` waits for the
broker's ack and reports a nack/timeout as an error, on both the default and
`-durable` paths (one extra round-trip; the one-shot chaos publish still works).

```bash
# NATS: publish 3 durable messages, then replay them from the stream later
nix run .#nats-pub -- -addr 10.33.33.10:30422 -jetstream -subject orders -count 3
nix run .#nats-sub -- -addr 10.33.33.10:30422 -jetstream -subject orders -count 3   # replays the 3

# RabbitMQ: durable quorum queue that survives a broker kill
nix run .#rabbitmq-pub -- -addr 10.33.33.10:30567 -durable -subject jobs -count 3 -pass "$RABBITMQ_PASS"
nix run .#rabbitmq-sub -- -addr 10.33.33.10:30567 -durable -subject jobs -count 3 -pass "$RABBITMQ_PASS"

# ValKey: follow the primary through a failover via Sentinel
nix run .#valkey-pub -- -sentinels 10.33.33.10:30650,10.33.33.10:30651,10.33.33.10:30652 \
  -subject demo -msg "to the primary" -pass "$VALKEY_PASS"
```

#### ValKey Sentinel is host-reachable (per-pod NodePorts + node-IP announces)

For a **host** FailoverClient to reach the primary Sentinel names, each pod must
advertise a node-reachable address rather than an in-cluster
`*.valkey-headless…svc.cluster.local` FQDN. So each `valkey-N` pod gets its own
NodePort Service and announces the node IP + that port:

| Pod | Client NodePort | Sentinel NodePort |
|-----|-----------------|-------------------|
| `valkey-0` | `30640` | `30650` |
| `valkey-1` | `30641` | `30651` |
| `valkey-2` | `30642` | `30652` |

Each pod exports `POD_HOST_IP` (downward API `status.hostIP`) and sets
`replica-announce-ip/-port` (valkey) and `sentinel announce-ip/-port` (sentinel)
to `<node-IP>:<its NodePort>`; `podAntiAffinity` keeps one valkey pod per node,
so the host IP uniquely identifies a pod. Sentinel is **seeded** at a stable
node IP + `valkey-0`'s client NodePort (`10.33.33.10:30640`) and learns the live
primary/replica set from those announcements. The trade-off is that in-cluster
replication + Sentinel health checks now traverse NodePort (the standard
Sentinel-behind-NAT pattern) — acceptable for this lab. The round-robin `valkey`
NodePort (`30637`, `sessionAffinity: ClientIP`) still serves the default
non-Sentinel `-addr` path and the chaos harness.

> **Security note (accepted lab tradeoff):** exposing Sentinel on a NodePort
> makes it reachable from the host/bridge network, and Sentinel here has **no
> `requirepass`** — so anyone who can route to a node IP can query topology or
> issue control commands (e.g. `SENTINEL FAILOVER`). This is accepted for this
> isolated lab (private `10.33.33.0/24` bridge, VM firewall already disabled,
> and the ValKey **data** port still password-protected via `requirepass`).
> Hardening for a real deployment: set `requirepass` on the Sentinels (plus
> `SentinelPassword` on the client and a matching sentinel-to-sentinel auth
> setup), or keep Sentinel off the host and reach the primary another way.

### Host pub/sub delivery (`sessionAffinity`)

A host client reaches a bus through **one** NodePort that round-robins across
the pods. That is fine for **NATS** and **RabbitMQ** — they are true clusters,
so a message published to any node reaches subscribers on every node. It is
**not** fine for **ValKey** or **MQTT**, where delivery is *node-local*:

- **ValKey** — a `PUBLISH` only fans out to subscribers on the same node, plus
  (from the primary) down the replication stream. If the round-robin NodePort
  lands your publisher and subscriber on different replicas, the message is
  silently dropped.
- **MQTT** — the three Mosquitto brokers are independent and meshed with
  bridges; a message does cross brokers, but subscription state propagates
  across a bridge with a small lag that can miss the very first message.

So the **ValKey and MQTT client Services set `sessionAffinity: ClientIP`**:
every connection from one host sticks to the same pod, so a host's `sub` and
`pub` co-locate and deliver deterministically. (The ValKey replication stream
and the MQTT bridge mesh still carry messages between *genuinely distributed*
clients on different pods — affinity only makes the single-host demo reliable.)
NATS and RabbitMQ need no affinity.

> **MQTT bridges run MQTT 3.1.1, not v5.** Mosquitto 2.x v5 bridges negotiate
> topic aliases and then tear themselves down with `PUBLISH invalid topic
> alias` protocol errors, so the mesh sets `bridge_protocol_version mqttv311`
> (3.1.1 has no topic aliases and bridges reliably). This is only the
> broker↔broker link — your client↔broker connection can still speak v5.

> **ValKey pub/sub caveat:** Valkey pub/sub is fire-and-forget and not
> persisted; during a Sentinel failover, in-flight messages can drop, and a
> write (or a `PUBLISH` meant to fan out to every replica) must reach the
> current primary. This is why ValKey uses replication + Sentinel (a single
> logical primary), **not** sharded Cluster mode.

---

## Testing

Two complementary layers: fast **unit tests** for the shared client logic that
run in the Nix sandbox, and a live **chaos / failover** harness that kills real
nodes and measures how quickly each bus recovers.

### Unit tests (`nix flake check`)

The one piece of non-trivial host-side logic is the shared
`clients/internal/cli` package — argument parsing and the pub/sub loop that
every client's `main.go` reuses. `clients/internal/cli/cli_test.go` covers it
with **table-driven** tests (one row per case, each with a `description` and an
`expected`, spanning positive / negative / boundary / corner inputs):

| Unit under test | What the rows assert |
|-----------------|----------------------|
| **`parseRate`** | `""` / `"0"` / whitespace → no throttle; `10/s`, `0.5/s`, `10`, `5/s` → the right per-message interval; `0/s` (deliberately asymmetric with bare `0`), negative, and non-numeric values → errors. |
| **`parseArgs`** | default flag values; every flag parsed, including `-jetstream` / `-durable` / `-sentinels` and `-rate`→interval; missing / unknown subcommand, a flag before the subcommand, a bad `-rate`, an unknown flag, and a non-numeric `-count` → errors; `-h` → `flag.ErrHelp`. |
| **`Limiter`** (`-count`) | reaching the limit closes `Done`; staying below it leaves `Done` open; extra `Hit`s past the limit are idempotent; `count ≤ 0` means unlimited. |
| **`PubLoop`** (`-count`/`-rate`) | the default sends exactly one message (no sequence suffix); `count N` sends `N` sequence-numbered bodies (`msg 1`, `msg 2`, …); an error from `send` stops the loop and propagates. |

To keep parsing testable without touching `os.Args`/`os.Exit`, `Parse` is a thin
wrapper over a pure `parseArgs(bin, defAddr, defPass, args) (*Flags, error)`.

```bash
# in the dev shell
nix develop -c bash -c 'cd clients && go test ./... -v'

# or as part of the flake's checks (builds checks.<system>.cli-tests)
nix flake check
```

`nix flake check` runs these in the same `buildGoModule` sandbox (vendored deps,
no network), so a broken assertion fails the whole flake — the tests are a gate,
not just a convenience. (Flakes only see git-tracked files, so `cli_test.go`
must be committed for the check to exercise it.)

### Chaos / failover test

```bash
nix run .#k8s-chaos-failover -- --rounds=6 --buses=nats,mqtt,valkey,rabbitmq
```

Where the unit tests check the client in isolation, this harness proves the
**clustered buses keep delivering while a node dies**. Each round:

1. **Subscribe** — start a host-side subscriber per bus (the Go CLIs) on subject
   `demo/chaos`, and prime each connection with one warm-up publish. Every
   client connects to a **stable node's NodePort (cp0, which is never killed)**,
   so the measurement isolates *in-cluster* failover from host↔node
   reachability.
2. **Kill** — stop one MicroVM (`k8s-vm-stop-one`), rotating through
   `cp1,cp2,w3`, and record the kill timestamp.
3. **Probe until recovered** — publish a uniquely-tagged probe once per second
   until the subscriber receives it; the **recovery time** is the seconds from
   kill to the first post-kill message delivered (or `TIMEOUT`).
4. **Heal** — restart the node (`k8s-vm-start-one`), wait for it to rejoin, and
   move to the next round.

Results land in `chaos-logs/summary.tsv` (`round`, `node`, `bus`,
`recovery_sec`). **What it demonstrates:** every bus survives the loss of any
single node and resumes delivery on its own — NATS via JetStream Raft
re-election + client reconnect, RabbitMQ via `pause_minority` + quorum-queue
leader re-election, MQTT because a surviving bridged broker still serves, and
ValKey via Sentinel promoting a replica to primary. It also proves the
**harness contract** the clients are built around: the harness drives only the
**default, no-flag** client paths (core NATS, fanout RabbitMQ, round-robin
ValKey `-addr`, plain MQTT), so those must never change.

> **Liveness vs. correctness.** The chaos harness measures *liveness* — does a
> message flow again after a node dies. The opt-in
> [`-jetstream` / `-durable` / `-sentinels` flags](#durability--ha-flags-opt-in)
> are the *correctness* counterpart: they show messages **survive** the failure
> (durable JetStream replay, persistent quorum queues) and that a writer
> **follows the new primary** (ValKey Sentinel FailoverClient) rather than just
> reconnecting somewhere.

See [`docs/resilience-testing.md`](docs/resilience-testing.md) for the per-bus
failover mechanics and expected behaviour.

### Soak test

Where the chaos harness runs one bus interaction at a time and measures
recovery, the **soak test** runs *everything at once for hours* and measures how
each client *holds up*:

```bash
# 4h run, one node killed every 15 min (cp0 spared); launch detached
nohup nix run .#k8s-soak-test -- --duration 4h > soak.out 2>&1 &

nix run .#k8s-soak-test -- --duration 10m --fault-interval 3m   # short trial
nix run .#k8s-soak-test -- --no-faults                          # steady load only
```

It launches **all four buses in their HA modes** — NATS `-jetstream`, RabbitMQ
`-durable` quorum, ValKey `-sentinels`, MQTT (native bridged) — as long-lived,
auto-respawning publisher/subscriber pairs, **loops the [NATS concept
demos](#nats-concept-examples)** on an interval (recording pass/fail), and injects
**rolling single-node failures** (`cp1,cp2,w3`; never two at once, so JetStream's
R3 quorum holds). Every client exports **OpenTelemetry metrics** (published,
received, publish errors, reconnects, sequence gaps = lost messages, and
publish/confirm latency) which the in-cluster **Prometheus** scrapes over the host
bridge and **Grafana** charts on the `soak` dashboard.

On exit the harness queries Prometheus and writes `soak-logs/report.md` +
`summary.tsv` — a per-client table of published/received/loss/reconnects — plus
`events.tsv` (fault timeline) and `demos.tsv` (demo pass/fail). Watch it live at
Grafana `http://10.33.33.10:30300` (dashboard uid `soak`) or Prometheus
`http://10.33.33.10:30900`.

> **Report, don't assert.** Like the chaos harness, the soak *measures* rather
> than pass/fails: expected loss (ValKey pub/sub and any in-flight message during
> a kill) is recorded as data, while JetStream and RabbitMQ quorum should show
> recovery with little or no loss. The stack is deployed via the same GitOps
> flow as the buses; the images are Nix-built and preloaded like the brokers.

---

## Cluster access & SSH auth (read this before SSHing to a node)

**Always reach the nodes through the Nix targets — never raw `ssh`.**

```bash
# Run a command on a node (cp0 | cp1 | cp2 | w3):
nix run .#k8s-vm-ssh -- --node=cp0 <command>

# kubectl on a node needs KUBECONFIG — non-interactive SSH does NOT source the
# profile, so a bare `kubectl` hits localhost:8080 and fails:
nix run .#k8s-vm-ssh -- --node=cp0 \
  env KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig kubectl get pods -A

# Load Nix-built images into a *running* cluster's containerd (all nodes, or one):
nix run .#k8s-image-import                                 # every image, every node
nix run .#k8s-image-import -- --node=w3 --image=grafana    # one image, one node
```

**Why the wrapper, specifically — it configures auth so it is deterministic
and can never spawn an interactive/GUI (askpass) password popup:**

1. It tries the repo key `secrets/ssh-ed25519` **only** — `IdentitiesOnly=yes`
   + `IdentityAgent=none` deliberately ignore your local `ssh-agent`. (Your
   agent's unrelated keys would otherwise be offered first and the servers
   reject them with *"Too many authentication failures"* once `MaxAuthTries`
   is hit.) `BatchMode=yes` + publickey-only mean a rejected key just errors
   out instead of falling back to an interactive prompt.
2. If the key is not accepted, it falls back to the **cluster password**
   (`ssh.password` in `nix/constants.nix`) via `sshpass` with
   `PubkeyAuthentication=no` — fed non-interactively, so again no GUI prompt.

In practice **cp0 accepts the repo key; the worker nodes (cp1/cp2/w3) accept
only the cluster password** — the wrapper handles both transparently, so you
never notice. `k8s-image-import` uses the same wrapper, so it works everywhere.

**Do NOT run raw `ssh root@<ip>` from automation.** It uses your ambient
`ssh-agent`, which (a) offers unrelated keys → *"Too many authentication
failures"*, and (b) on failure falls back to keyboard-interactive/password →
an **X11 askpass GUI popup**. The scripts that drive the cluster
(`k8s-soak-test`, `k8s-chaos-failover`) go through the wrapper via a `kexec`
helper that also sets `KUBECONFIG` — mirror that pattern, don't hand-roll ssh.

## Nix targets reference

**Network / VM lifecycle**
`k8s-check-host`, `k8s-network-setup`, `k8s-network-teardown`,
`k8s-start-all`, `k8s-vm-check`, `k8s-vm-ssh`, `k8s-vm-stop`,
`k8s-vm-stop-one`, `k8s-vm-start-one`, `k8s-vm-wipe`, `k8s-cluster-rebuild`,
`k8s-image-import` (import Nix images into a running cluster's containerd).

**Secrets / certs / manifests**
`k8s-gen-secrets`, `k8s-gen-certs`, `k8s-render-manifests`
(`-- --check` verifies `rendered/` is current).

**Bus images** (`nix build .#…`)
`nats-image`, `rabbitmq-image`, `mosquitto-image`, `valkey-image`,
`prometheus-image`, `prometheus-nats-exporter-image`, `grafana-image`,
`prometheus-redis-exporter-image`, `message-bus-clients`.

**Pub/sub clients** (`nix run .#…`)
`nats-pub`/`nats-sub`, `rabbitmq-pub`/`rabbitmq-sub`,
`mqtt-pub`/`mqtt-sub`, `valkey-pub`/`valkey-sub`.

**Testing**
`k8s-chaos-failover`, `k8s-soak-test`, `k8s-lifecycle-test-all`,
`k8s-cluster-test`.

---

## Rendered-manifests GitOps pattern

Each component is a Nix module under `nix/gitops/env/*.nix` that emits
Kubernetes YAML plus an ArgoCD `Application`. `nix run
.#k8s-render-manifests` writes them to `rendered/`, which is committed to
git; ArgoCD then auto-syncs each `Application` from that path. On first
boot, cp0's bootstrap oneshot applies Cilium → base → ArgoCD → Secrets →
Applications directly from the Nix store (no git fetch needed before the
CNI is up), after which ArgoCD is the source of truth.

Deployed components: `base` (namespaces, RBAC, CoreDNS), `cilium`,
`argocd`, `storage` (local-path provisioner backing the bus PVCs), and the
four buses `nats` / `rabbitmq` / `mqtt` / `valkey`.

---

## Certificate architecture (PKI)

Three CA hierarchies (`k8s-cluster-ca`, `etcd-ca`, `front-proxy-ca`) plus a
service-account keypair are generated at `nix build` time
(`nix/certs.nix`) and baked into each VM's PKI directory. Leaf certs are
per-node (etcd server/peer, kubelet) and per-component (apiserver,
controller-manager, scheduler, …). `nix run .#k8s-gen-certs` copies the
build-time certs to `./certs/` for inspection.

---

## File structure

```
flake.nix                       # orchestrator
nix/constants.nix               # IPs, MACs, ports, bus config (messageBus.*)
nix/nodes.nix                   # node definitions (cp0, cp1, cp2, w3)
nix/microvm.nix                 # mkK8sNode parametric VM generator
nix/k8s-module.nix              # etcd, apiserver, kubelet, containerd
nix/image-preload-module.nix    # import Nix bus images into containerd at boot
nix/gitops-bootstrap-module.nix # first-boot GitOps oneshot (cp0)
nix/network-setup.nix           # bridge + TAP + NAT + haproxy
nix/certs.nix                   # build-time PKI
nix/secrets-gen.nix / secrets.nix  # offline secret gen → K8s Secret manifests
nix/chaos-scripts.nix           # message-bus failover test
nix/clients.nix                 # Go pub/sub CLIs → flake apps
nix/images/                     # Nix-built OCI images (nats, rabbitmq, mosquitto, valkey)
nix/gitops/env/                 # base, argocd, cilium, storage, nats, rabbitmq, mqtt, valkey
clients/                        # Go module: cmd/{natscli,rabbitmqcli,mqttcli,valkeycli}
rendered/                       # committed rendered manifests (ArgoCD source)
docs/                           # secrets.md, resilience-testing.md
```
