# nix/gitops/env/monitoring/default.nix
#
# Observability stack — in-cluster Prometheus + Grafana.
#
# Prometheus scrapes (all via static_configs — the targets are known, so no
# kubernetes_sd/RBAC is needed):
#   • node_exporter on every VM (already running on :9100)
#   • the prometheus-nats-exporter (NATS has no native Prometheus endpoint)
#   • RabbitMQ's rabbitmq_prometheus plugin (per-pod)
#   • Cilium agent/operator/Hubble metrics
#   • the host-side soak clients' OTel /metrics (over the k8sbr0 bridge)
#   • the proto-bench region agents + host drivers (mbbench_* metrics)
#
# Prometheus and Grafana are pinned to cp0 — the node the soak's fault
# rotation never kills — so monitoring survives rolling failures. Grafana
# runs with anonymous Admin access (secure LAN) and a provisioned Prometheus
# datasource + soak dashboard.
#
# Images: Nix-built messagebus.local/{prometheus,grafana}:<tag>, preloaded into
# containerd (imagePullPolicy: Never). NATS metrics come from a per-pod
# prometheus-nats-exporter sidecar defined in nats.nix (see targets.nix `nats`
# and the `nats` scrape job for why per-pod isolation matters under faults).
#
# This directory is the split of the old single-file monitoring.nix: targets +
# prometheus config/server + grafana provisioning/server + the dashboards
# (soak + community, with a proto-bench dashboard slotting in next) + the
# ArgoCD Application. Each file stays well under the ~150-line budget.
{ pkgs, lib }:
let
  constants = import ../../../constants.nix;
  mon     = constants.monitoring;
  ns      = mon.namespace;
  cp0Host = constants.getHostname "cp0";

  # Provisioned Prometheus datasource uid — referenced by the soak dashboard
  # and substituted into the community dashboards' ${DS_PROMETHEUS} input.
  dsUid = "Prometheus";

  targets    = import ./targets.nix { inherit lib; };
  dashboards = import ./dashboards { inherit pkgs lib ns dsUid; };
in
{
  manifests = [
    (import ./prometheus-config.nix { inherit mon ns targets; })
    (import ./prometheus.nix { inherit mon ns cp0Host; })
    (import ./grafana-provisioning.nix { inherit mon ns dsUid; })
    (import ./grafana.nix {
      inherit pkgs mon ns cp0Host;
      inherit (dashboards) volumeMounts volumes;
    })
    (import ./application.nix { inherit constants ns; })
  ] ++ dashboards.manifests;
}
