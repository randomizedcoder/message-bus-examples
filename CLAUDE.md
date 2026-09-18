# Project: message-bus-examples (clustered buses on a K8s MicroVM cluster)

Four clustered message buses — NATS, RabbitMQ, MQTT (Mosquitto, bridged),
ValKey (replication + Sentinel) — run as hand-written StatefulSets from
**Nix-built OCI images** (`nix/images/*.nix`) preloaded into containerd
(`nix/image-preload-module.nix`) — no upstream images, no registry. Go
pub/sub CLIs (`clients/`, exposed as `nix run .#<bus>-pub/-sub`) exercise
them from the host over NodePorts.

## VM Access

You have full root SSH access to all 4 MicroVMs. Use the pre-generated SSH key:

```bash
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
  -i secrets/ssh-ed25519 root@<IP> '<command>'
```

| Node | IP |
|------|----|
| cp0 | 10.33.33.10 |
| cp1 | 10.33.33.11 |
| cp2 | 10.33.33.12 |
| w3  | 10.33.33.13 |

For kubectl commands via SSH, always set KUBECONFIG:

```bash
ssh -i secrets/ssh-ed25519 root@10.33.33.10 \
  'KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig kubectl get pods -A'
```

For complex kubectl arguments (jsonpath, etc.), use `bash -s <<'REMOTE_EOF'` heredoc
to avoid SSH argument escaping issues.

## Rendered Manifests Workflow

After changing any Nix gitops source (`nix/gitops/env/*.nix`):

```bash
nix run .#k8s-render-manifests   # regenerate rendered/ from Nix
# commit both nix/ and rendered/ changes together
```

ArgoCD watches the `rendered/` directory in git, not the Nix source.

## Secrets

Secrets live in `./secrets/` (git-staged, not committed). Generate with:

```bash
nix run .#k8s-gen-secrets          # first time
nix run .#k8s-gen-secrets -- --force  # rotate
```

See `docs/secrets.md` for the full design.

## Bus images & preload

- Images: `nix/images/*.nix` (`dockerTools.buildLayeredImage`), one per bus,
  named `messagebus.local/<bus>:<tag>` (dotted host prefix so containerd
  does not normalise it to docker.io). Build/measure: `nix build .#nats-image`.
- Preload: `nix/image-preload-module.nix` imports each image tarball (from
  the 9p-shared `/nix/store`) into containerd's `k8s.io` namespace before
  kubelet. StatefulSets use `imagePullPolicy: Never`.
- The image list flows `nix/images` → `busImages` in `flake.nix` →
  `mkK8sNode` → `nix/microvm.nix` → the preload module.

## Key Patterns

- **Bootstrap module**: `nix/gitops-bootstrap-module.nix` — runs on cp0 first boot,
  applies manifests in order (Cilium -> base/CoreDNS -> ArgoCD -> Secrets -> Apps)
- **Constants**: `nix/constants.nix` — IPs, ports, and `messageBus.*` (image
  name/tag, replicas, NodePorts) per bus
- **Bus modules**: `nix/gitops/env/{nats,rabbitmq,mqtt,valkey}.nix` — each a
  clustered StatefulSet + headless Service + client NodePort + ArgoCD Application
- **Failover test**: `nix/chaos-scripts.nix` — SSH to cp0 for credentials,
  drives the Go clients while killing nodes
