# nix/gitops/env/monitoring/dashboards/rpc.nix
#
# The RPC-lab Grafana dashboard (§22 metrics, uid `rpc`): request/response rate
# by transport and outcome, round-trip latency, errors/timeouts, §29 idempotent
# replays, and §26 payload sizes — all from the rpc_* series the host
# rpc-benchmark exposes on :9310 (scrape job `rpc-benchmark`, see
# ../prometheus-config.nix / ../targets.nix). The bounded label set is
# {transport, codec, service, method} (+ result on responses/duration); request_id
# / trace_id / client_id are deliberately NOT labels (§36 cardinality), so trace
# correlation is by the §23 spans, not label joins here.
#
# Its OWN ConfigMap in its own mount subdir, like ./protobench.nix (the community
# dashboards CM is already ~926 KB near the 1 MB etcd cap — see
# grafana-community-cm-size-limit). Returns { manifest; cmName; volName; mountPath }
# so ./default.nix can append this dashboard's volumeMount/volume to the community
# strings grafana.nix mounts (the file provider scans /etc/grafana/dashboards
# recursively).
{ ns }:
{
  cmName    = "grafana-dashboard-rpc";
  volName   = "dash-rpc";
  mountPath = "/etc/grafana/dashboards/rpc";
  manifest = {
    name = "monitoring/grafana-dashboard-rpc.yaml";
    content = ''
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: grafana-dashboard-rpc
        namespace: ${ns}
      data:
        rpc.json: |
          {
            "uid": "rpc",
            "title": "RPC lab",
            "schemaVersion": 39,
            "version": 1,
            "editable": true,
            "time": { "from": "now-1h", "to": "now" },
            "refresh": "15s",
            "templating": { "list": [] },
            "annotations": { "list": [
              {
                "name": "rpc events",
                "datasource": { "type": "grafana", "uid": "-- Grafana --" },
                "enable": true, "iconColor": "red", "type": "tags",
                "tags": ["rpc"]
              }
            ] },
            "panels": [
              {
                "type": "row", "title": "Round-trip latency",
                "gridPos": { "h": 1, "w": 24, "x": 0, "y": 0 }
              },
              {
                "type": "timeseries",
                "title": "OK latency p50/p99 (by transport/codec)",
                "gridPos": { "h": 8, "w": 12, "x": 0, "y": 1 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
                "targets": [
                  { "expr": "histogram_quantile(0.5, sum by (le,transport,codec) (rate(rpc_request_duration_seconds_bucket{job=\"rpc-benchmark\",result=\"ok\"}[$__rate_interval])))", "legendFormat": "p50 {{transport}}/{{codec}}", "refId": "A" },
                  { "expr": "histogram_quantile(0.99, sum by (le,transport,codec) (rate(rpc_request_duration_seconds_bucket{job=\"rpc-benchmark\",result=\"ok\"}[$__rate_interval])))", "legendFormat": "p99 {{transport}}/{{codec}}", "refId": "B" }
                ]
              },
              {
                "type": "timeseries",
                "title": "OK latency p99 (by service.method)",
                "gridPos": { "h": 8, "w": 12, "x": 12, "y": 1 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
                "targets": [
                  { "expr": "histogram_quantile(0.99, sum by (le,service,method) (rate(rpc_request_duration_seconds_bucket{job=\"rpc-benchmark\",result=\"ok\"}[$__rate_interval])))", "legendFormat": "{{service}}.{{method}}", "refId": "A" }
                ]
              },
              {
                "type": "row", "title": "Throughput & outcomes",
                "gridPos": { "h": 1, "w": 24, "x": 0, "y": 9 }
              },
              {
                "type": "timeseries",
                "title": "Request rate (req/s, by transport)",
                "gridPos": { "h": 8, "w": 12, "x": 0, "y": 10 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "reqps" }, "overrides": [] },
                "targets": [
                  { "expr": "sum by (transport) (rate(rpc_requests_total{job=\"rpc-benchmark\"}[$__rate_interval]))", "legendFormat": "{{transport}}", "refId": "A" }
                ]
              },
              {
                "type": "timeseries",
                "title": "Response rate by result (resp/s) — ok/timeout/error/non_ok",
                "gridPos": { "h": 8, "w": 12, "x": 12, "y": 10 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "reqps" }, "overrides": [] },
                "targets": [
                  { "expr": "sum by (result) (rate(rpc_responses_total{job=\"rpc-benchmark\"}[$__rate_interval]))", "legendFormat": "{{result}}", "refId": "A" }
                ]
              },
              {
                "type": "row", "title": "Errors, timeouts & idempotency (§28 / §29)",
                "gridPos": { "h": 1, "w": 24, "x": 0, "y": 18 }
              },
              {
                "type": "timeseries",
                "title": "Errors & timeouts rate (per s, by transport)",
                "gridPos": { "h": 8, "w": 12, "x": 0, "y": 19 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "cps" }, "overrides": [] },
                "targets": [
                  { "expr": "sum by (transport) (rate(rpc_errors_total{job=\"rpc-benchmark\"}[$__rate_interval]))", "legendFormat": "errors {{transport}}", "refId": "A" },
                  { "expr": "sum by (transport) (rate(rpc_timeouts_total{job=\"rpc-benchmark\"}[$__rate_interval]))", "legendFormat": "timeouts {{transport}}", "refId": "B" }
                ]
              },
              {
                "type": "timeseries",
                "title": "Idempotent replays rate (per s, by service.method) — §29",
                "gridPos": { "h": 8, "w": 12, "x": 12, "y": 19 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "cps" }, "overrides": [] },
                "targets": [
                  { "expr": "sum by (service,method) (rate(rpc_idempotent_replays_total{job=\"rpc-benchmark\"}[$__rate_interval]))", "legendFormat": "{{service}}.{{method}}", "refId": "A" }
                ]
              },
              {
                "type": "row", "title": "Payload size (§26)",
                "gridPos": { "h": 1, "w": 24, "x": 0, "y": 27 }
              },
              {
                "type": "timeseries",
                "title": "Request wire size p50/p99 (by transport/codec)",
                "gridPos": { "h": 8, "w": 12, "x": 0, "y": 28 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "decbytes" }, "overrides": [] },
                "targets": [
                  { "expr": "histogram_quantile(0.5, sum by (le,transport,codec) (rate(rpc_request_bytes_bucket{job=\"rpc-benchmark\"}[$__rate_interval])))", "legendFormat": "p50 {{transport}}/{{codec}}", "refId": "A" },
                  { "expr": "histogram_quantile(0.99, sum by (le,transport,codec) (rate(rpc_request_bytes_bucket{job=\"rpc-benchmark\"}[$__rate_interval])))", "legendFormat": "p99 {{transport}}/{{codec}}", "refId": "B" }
                ]
              },
              {
                "type": "timeseries",
                "title": "Response wire size p50/p99 (by transport/codec)",
                "gridPos": { "h": 8, "w": 12, "x": 12, "y": 28 },
                "datasource": { "type": "prometheus", "uid": "Prometheus" },
                "fieldConfig": { "defaults": { "unit": "decbytes" }, "overrides": [] },
                "targets": [
                  { "expr": "histogram_quantile(0.5, sum by (le,transport,codec) (rate(rpc_response_bytes_bucket{job=\"rpc-benchmark\"}[$__rate_interval])))", "legendFormat": "p50 {{transport}}/{{codec}}", "refId": "A" },
                  { "expr": "histogram_quantile(0.99, sum by (le,transport,codec) (rate(rpc_response_bytes_bucket{job=\"rpc-benchmark\"}[$__rate_interval])))", "legendFormat": "p99 {{transport}}/{{codec}}", "refId": "B" }
                ]
              }
            ]
          }
    '';
  };
}
