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
| **NATS** | 3-node JetStream cluster | Raft re-election + client reconnect | `30422` (client 4222) |
| **RabbitMQ** | 3-node cluster (k8s peer discovery, quorum queues) | `pause_minority` + queue leader re-election | `30567` (AMQP), `30672` (mgmt UI) |
| **MQTT** | 3× Mosquitto, full-mesh bridged (MQTT 3.1.1) | surviving brokers keep serving; client reconnect | `30883` (MQTT 1883) |
| **ValKey** | 1 primary + 2 replicas + 3 Sentinels | Sentinel auto-failover | `30637` (client 6379) |

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
`clients/cmd/*` (one small `main.go` per bus).

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

## Chaos / failover test

```bash
nix run .#k8s-chaos-failover -- --rounds=6 --buses=nats,mqtt,valkey,rabbitmq
```

Kills one MicroVM at a time and measures, per bus, how long a host-side
subscriber takes to receive fresh messages again. Output →
`chaos-logs/summary.tsv`. See [`docs/resilience-testing.md`](docs/resilience-testing.md).

---

## Nix targets reference

**Network / VM lifecycle**
`k8s-check-host`, `k8s-network-setup`, `k8s-network-teardown`,
`k8s-start-all`, `k8s-vm-check`, `k8s-vm-ssh`, `k8s-vm-stop`,
`k8s-vm-stop-one`, `k8s-vm-start-one`, `k8s-vm-wipe`, `k8s-cluster-rebuild`.

**Secrets / certs / manifests**
`k8s-gen-secrets`, `k8s-gen-certs`, `k8s-render-manifests`
(`-- --check` verifies `rendered/` is current).

**Bus images** (`nix build .#…`)
`nats-image`, `rabbitmq-image`, `mosquitto-image`, `valkey-image`,
`message-bus-clients`.

**Pub/sub clients** (`nix run .#…`)
`nats-pub`/`nats-sub`, `rabbitmq-pub`/`rabbitmq-sub`,
`mqtt-pub`/`mqtt-sub`, `valkey-pub`/`valkey-sub`.

**Testing**
`k8s-chaos-failover`, `k8s-lifecycle-test-all`, `k8s-cluster-test`.

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
