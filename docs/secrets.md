# Secrets

The cluster needs a small set of secrets. They are generated **offline**
on the host into `./secrets/` and consumed at `nix build` time by
`nix/secrets.nix`, which assembles them into Kubernetes `Secret`
manifests. Those Secrets are applied by the first-boot bootstrap oneshot
on cp0 *before* the message-bus workloads roll out (see
`nix/gitops-bootstrap-module.nix`).

## Generate

```bash
nix run .#k8s-gen-secrets          # refuses if ./secrets/ already exists
nix run .#k8s-gen-secrets -- --force
```

This writes:

| File | Used for |
|------|----------|
| `rabbitmq-password` | RabbitMQ default `admin` user password |
| `rabbitmq-erlang-cookie` | Shared Erlang cookie every RabbitMQ node presents to peer |
| `valkey-password` | ValKey `requirepass` / `masterauth` (primary, replicas, Sentinel) |
| `ssh-ed25519`, `ssh-ed25519.pub` | Passwordless SSH into the MicroVMs (public key baked into images) |

## Notes

- `./secrets/` is **not** committed. `k8s-gen-secrets` stages the files so
  Nix (flakes only read git-tracked files) can `readFile` them during
  evaluation — run `git reset secrets/` before committing anything else.
- NATS and MQTT (Mosquitto) run without authentication in this lab setup;
  they have no secrets.
- Retrieve a live password from the cluster with, e.g.:

  ```bash
  nix run .#k8s-vm-ssh -- --node=cp0 \
    kubectl -n rabbitmq get secret rabbitmq-credentials \
    -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' | base64 -d
  ```
