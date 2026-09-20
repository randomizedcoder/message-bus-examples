# nix/gitops/env/monitoring/prometheus-config.nix
#
# The prometheus-config ConfigMap (prometheus.yml scrape jobs). All jobs use
# static_configs; the target lists come from ./targets.nix. Split out of the old
# monolithic monitoring.nix with no behaviour change.
{ mon, ns, targets }:
{
  name = "monitoring/prometheus-configmap.yaml";
  content = ''
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: prometheus-config
      namespace: ${ns}
    data:
      prometheus.yml: |
        global:
          scrape_interval: 15s
          evaluation_interval: 15s
        scrape_configs:
          - job_name: prometheus
            static_configs:
              - targets: ['localhost:${toString mon.prometheus.port}']
          - job_name: node
            static_configs:
              - targets: ${targets.node}
          - job_name: nats
            static_configs:
              - targets: ${targets.nats}
            # Each per-pod exporter polls its own server over localhost, so
            # prometheus-nats-exporter labels every gnatsd_* / jetstream_*
            # series with an identical server_id="http://localhost:8222".
            # Rewrite server_id to the pod name (from the unique instance
            # label, e.g. nats-0.nats-headless...:7777 -> nats-0) so the NATS
            # Server dashboard's per-server variable and panels (which key on
            # server_id) distinguish the servers again. This also matches the
            # server_name jsz already reports, keeping the two labels aligned.
            metric_relabel_configs:
              - source_labels: [instance]
                regex: '([^.]+)\..*'
                target_label: server_id
                replacement: '$1'
          - job_name: rabbitmq
            static_configs:
              - targets: ${targets.rabbitmq}
          - job_name: redis
            static_configs:
              - targets: ${targets.redis}
          - job_name: mqtt
            static_configs:
              - targets: ${targets.mqtt}
          - job_name: cilium-agent
            static_configs:
              - targets: ${targets.ciliumAgent}
          - job_name: cilium-operator
            static_configs:
              - targets: ${targets.ciliumOperator}
          - job_name: hubble
            static_configs:
              - targets: ${targets.hubble}
          - job_name: grafana
            static_configs:
              - targets: ['grafana:${toString mon.grafana.port}']
          - job_name: soak-clients
            static_configs:
              - targets: ${targets.clients}
          # proto-bench region agents (mbbench_* role=server + Go/process
          # collectors). Fast scrape so a 30s cell has enough samples.
          - job_name: workloads-agents
            scrape_interval: 5s
            static_configs:
              - targets: ${targets.workloads}
            # Label each series with its region (from the pod DNS name,
            # e.g. region-agent-us-east-1.region-agents...:9464).
            metric_relabel_configs:
              - source_labels: [instance]
                regex: 'region-agent-([^.]+)\..*'
                target_label: agent_region
                replacement: '$1'
          # proto-bench host driver(s): benchcli OTel /metrics over the
          # k8sbr0 bridge (mbbench_* role=client).
          - job_name: proto-bench-driver
            scrape_interval: 5s
            static_configs:
              - targets: ${targets.driver}
  '';
}
