# nix/gitops/env/monitoring/dashboards/grpcbus.nix
#
# The gRPC-native message bus dashboard (uid `grpcbus`): fan-out throughput
# (published / delivered / dropped rates), live subscriber count, and the derived
# fan-out and drop ratios — all from the grpcbus_* series the in-cluster broker
# (grpcbrokerd) exposes on its metrics port (scrape job `grpcbus`, see
# ../prometheus-config.nix / ../targets.nix).
#
# Unlike the rpc/protobench dashboards, the grpcbus_* metrics carry an
# intentionally EMPTY label set (topic/publisher/subscriber would be unbounded
# cardinality — clients/internal/grpcbus/metrics.go), so every expr is a plain
# sum(rate(...)) with no `by (...)` grouping.
#
# Its OWN ConfigMap in its own mount subdir, like ./rpc.nix / ./protobench.nix
# (the community dashboards CM is already ~926 KB near the 1 MB etcd cap — see
# grafana-community-cm-size-limit). Returns { manifest; cmName; volName; mountPath }
# so ./default.nix can append this dashboard's volumeMount/volume to the community
# strings grafana.nix mounts (the file provider scans /etc/grafana/dashboards
# recursively).
{ ns }:
{
  cmName    = "grafana-dashboard-grpcbus";
  volName   = "dash-grpcbus";
  mountPath = "/etc/grafana/dashboards/grpcbus";
  manifest = {
    name = "monitoring/grafana-dashboard-grpcbus.yaml";
    content = ''
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: grafana-dashboard-grpcbus
        namespace: ${ns}
      data:
        grpcbus.json: |
          {
            "uid": "grpcbus",
            "title": "gRPC bus",
            "schemaVersion": 39,
            "version": 1,
            "editable": true,
            "time": { "from": "now-1h", "to": "now" },
            "refresh": "15s",
            "templating": { "list": [] },
            "annotations": { "list": [] },
            "panels": [
              {
                "type": "row", "title": "Fan-out throughput",
                "gridPos": { "h": 1, "w": 24, "x": 0, "y": 0 }
              },
              {
                "type": "timeseries",
                "title": "Published rate (msg/s)",
                "gridPos": { "h": 8, "w": 8, "x": 0, "y": 1 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "reqps" }, "overrides": [] },
                "targets": [
                  { "expr": "sum(rate(grpcbus_messages_published_total{job=\"grpcbus\"}[$__rate_interval]))", "legendFormat": "published", "refId": "A" }
                ]
              },
              {
                "type": "timeseries",
                "title": "Delivered rate (msg/s) — one publish → N subscribers",
                "gridPos": { "h": 8, "w": 8, "x": 8, "y": 1 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "reqps" }, "overrides": [] },
                "targets": [
                  { "expr": "sum(rate(grpcbus_messages_delivered_total{job=\"grpcbus\"}[$__rate_interval]))", "legendFormat": "delivered", "refId": "A" }
                ]
              },
              {
                "type": "timeseries",
                "title": "Dropped rate (msg/s) — full subscriber buffers",
                "gridPos": { "h": 8, "w": 8, "x": 16, "y": 1 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "reqps" }, "overrides": [] },
                "targets": [
                  { "expr": "sum(rate(grpcbus_messages_dropped_total{job=\"grpcbus\"}[$__rate_interval]))", "legendFormat": "dropped", "refId": "A" }
                ]
              },
              {
                "type": "row", "title": "Subscribers & ratios",
                "gridPos": { "h": 1, "w": 24, "x": 0, "y": 9 }
              },
              {
                "type": "stat",
                "title": "Active subscribers",
                "gridPos": { "h": 8, "w": 8, "x": 0, "y": 10 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "none" }, "overrides": [] },
                "targets": [
                  { "expr": "sum(grpcbus_active_subscribers{job=\"grpcbus\"})", "legendFormat": "subscribers", "refId": "A" }
                ]
              },
              {
                "type": "timeseries",
                "title": "Fan-out ratio (delivered / published)",
                "gridPos": { "h": 8, "w": 8, "x": 8, "y": 10 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "none" }, "overrides": [] },
                "targets": [
                  { "expr": "sum(rate(grpcbus_messages_delivered_total{job=\"grpcbus\"}[$__rate_interval])) / clamp_min(sum(rate(grpcbus_messages_published_total{job=\"grpcbus\"}[$__rate_interval])), 1)", "legendFormat": "deliveries per publish", "refId": "A" }
                ]
              },
              {
                "type": "timeseries",
                "title": "Drop ratio (dropped / (delivered + dropped))",
                "gridPos": { "h": 8, "w": 8, "x": 16, "y": 10 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "percentunit", "min": 0, "max": 1 }, "overrides": [] },
                "targets": [
                  { "expr": "sum(rate(grpcbus_messages_dropped_total{job=\"grpcbus\"}[$__rate_interval])) / clamp_min(sum(rate(grpcbus_messages_delivered_total{job=\"grpcbus\"}[$__rate_interval])) + sum(rate(grpcbus_messages_dropped_total{job=\"grpcbus\"}[$__rate_interval])), 1)", "legendFormat": "drop fraction", "refId": "A" }
                ]
              }
            ]
          }
    '';
  };
}
