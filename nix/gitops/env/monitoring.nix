# nix/gitops/env/monitoring.nix
#
# Observability stack — in-cluster Prometheus + Grafana + a NATS exporter.
#
# Prometheus scrapes (all via static_configs — the targets are known, so no
# kubernetes_sd/RBAC is needed):
#   • node_exporter on every VM (already running on :9100)
#   • the prometheus-nats-exporter (NATS has no native Prometheus endpoint)
#   • RabbitMQ's rabbitmq_prometheus plugin (per-pod)
#   • Cilium agent/operator/Hubble metrics
#   • the host-side soak clients' OTel /metrics (over the k8sbr0 bridge)
#
# Prometheus and Grafana are pinned to cp0 — the node the soak's fault
# rotation never kills — so monitoring survives rolling failures. Grafana
# runs with anonymous Admin access (secure LAN) and a provisioned Prometheus
# datasource + soak dashboard.
#
# Images: Nix-built messagebus.local/{prometheus,prometheus-nats-exporter,
# grafana}:<tag>, preloaded into containerd (imagePullPolicy: Never).
#
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  mon   = constants.monitoring;
  ns    = mon.namespace;
  domain = "svc.${constants.k8s.clusterDomain}";
  natsC = constants.messageBus.nats;
  rmqC  = constants.messageBus.rabbitmq;
  cp0Host = constants.getHostname "cp0";

  # Inline YAML array of quoted targets: ['a:1', 'b:2'].
  mkTargets = items: "[" + builtins.concatStringsSep ", " (map (t: "'${t}'") items) + "]";

  nodeTargets = mkTargets (map (ip: "${ip}:${toString constants.nodeExporter.port}") constants.allNodeIps4);
  ciliumAgentTargets    = mkTargets (map (ip: "${ip}:${toString constants.hubble.agentMetricsPort}") constants.allNodeIps4);
  ciliumOperatorTargets = mkTargets (map (ip: "${ip}:${toString constants.hubble.operatorMetricsPort}") constants.allNodeIps4);
  hubbleTargets         = mkTargets (map (ip: "${ip}:${toString constants.hubble.hubbleMetricsPort}") constants.allNodeIps4);
  rmqTargets = mkTargets (map
    (i: "rabbitmq-${toString i}.rabbitmq-headless.${rmqC.namespace}.${domain}:${toString rmqC.prometheusPort}")
    (lib.range 0 (rmqC.replicas - 1)));
  # Host-side soak clients: hostBridgeIP:(base+0 .. base+count-1). Absent ones
  # just show DOWN — the soak harness fills the range as it launches clients.
  clientTargets = mkTargets (map
    (i: "${mon.hostBridgeIP}:${toString (mon.clientMetricsBasePort + i)}")
    (lib.range 0 (mon.clientMetricsCount - 1)));

  # NATS monitoring URLs the exporter polls (3 hub servers + the leaf); the
  # exporter labels each series with server_name so per-node metrics are kept.
  natsUrlList =
    (map (i: "http://nats-${toString i}.nats-headless.${natsC.namespace}.${domain}:${toString natsC.monitorPort}")
      (lib.range 0 (natsC.replicas - 1)))
    ++ [ "http://nats-leaf-0.nats-leaf-headless.${natsC.namespace}.${domain}:${toString natsC.monitorPort}" ];
  natsUrlArgs = builtins.concatStringsSep "\n        " (map (u: "- \"${u}\"") natsUrlList);
in
{
  manifests = [
    # ─── NATS exporter (NATS has no native Prometheus endpoint) ────────
    {
      name = "monitoring/nats-exporter.yaml";
      content = ''
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: nats-exporter
          namespace: ${ns}
          labels:
            app: nats-exporter
        spec:
          replicas: 1
          selector:
            matchLabels:
              app: nats-exporter
          template:
            metadata:
              labels:
                app: nats-exporter
            spec:
              containers:
              - name: nats-exporter
                image: ${mon.natsExporter.image}:${mon.natsExporter.tag}
                imagePullPolicy: Never
                command: ["/bin/prometheus-nats-exporter"]
                args:
                - "-varz"
                - "-jsz=all"
                - "-connz"
                - "-leafz"
                - "-routez"
                - "-port"
                - "${toString mon.natsExporter.port}"
                ${natsUrlArgs}
                ports:
                - containerPort: ${toString mon.natsExporter.port}
                  name: metrics
                readinessProbe:
                  httpGet:
                    path: /metrics
                    port: ${toString mon.natsExporter.port}
                  initialDelaySeconds: 5
                  periodSeconds: 15
                resources:
                  requests:
                    cpu: 25m
                    memory: 32Mi
                  limits:
                    cpu: 200m
                    memory: 128Mi
        ---
        apiVersion: v1
        kind: Service
        metadata:
          name: nats-exporter
          namespace: ${ns}
          labels:
            app: nats-exporter
        spec:
          selector:
            app: nats-exporter
          ports:
          - name: metrics
            port: ${toString mon.natsExporter.port}
            targetPort: ${toString mon.natsExporter.port}
      '';
    }

    # ─── Prometheus config ─────────────────────────────────────────────
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
                  - targets: ${nodeTargets}
              - job_name: nats
                static_configs:
                  - targets: ['nats-exporter:${toString mon.natsExporter.port}']
              - job_name: rabbitmq
                static_configs:
                  - targets: ${rmqTargets}
              - job_name: cilium-agent
                static_configs:
                  - targets: ${ciliumAgentTargets}
              - job_name: cilium-operator
                static_configs:
                  - targets: ${ciliumOperatorTargets}
              - job_name: hubble
                static_configs:
                  - targets: ${hubbleTargets}
              - job_name: grafana
                static_configs:
                  - targets: ['grafana:${toString mon.grafana.port}']
              - job_name: soak-clients
                static_configs:
                  - targets: ${clientTargets}
      '';
    }

    # ─── Prometheus (StatefulSet, pinned to cp0, local-path PVC) ───────
    {
      name = "monitoring/prometheus.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: prometheus
          namespace: ${ns}
          labels:
            app: prometheus
        spec:
          type: NodePort
          selector:
            app: prometheus
          ports:
          - name: web
            port: ${toString mon.prometheus.port}
            targetPort: ${toString mon.prometheus.port}
            nodePort: ${toString mon.prometheus.nodePort}
        ---
        apiVersion: apps/v1
        kind: StatefulSet
        metadata:
          name: prometheus
          namespace: ${ns}
        spec:
          serviceName: prometheus
          replicas: 1
          selector:
            matchLabels:
              app: prometheus
          template:
            metadata:
              labels:
                app: prometheus
            spec:
              # Pin to cp0 — the node the soak never kills — so the TSDB
              # (and its local-path PV) stay put across rolling failures.
              affinity:
                nodeAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                    nodeSelectorTerms:
                    - matchExpressions:
                      - key: kubernetes.io/hostname
                        operator: In
                        values: [ ${cp0Host} ]
              securityContext:
                fsGroup: 0
              containers:
              - name: prometheus
                image: ${mon.prometheus.image}:${mon.prometheus.tag}
                imagePullPolicy: Never
                command: ["/bin/prometheus"]
                args:
                - "--config.file=/etc/prometheus/prometheus.yml"
                - "--storage.tsdb.path=/data"
                - "--storage.tsdb.retention.time=${mon.prometheus.retention}"
                - "--web.listen-address=0.0.0.0:${toString mon.prometheus.port}"
                - "--web.enable-lifecycle"
                ports:
                - containerPort: ${toString mon.prometheus.port}
                  name: web
                readinessProbe:
                  httpGet:
                    path: /-/ready
                    port: ${toString mon.prometheus.port}
                  initialDelaySeconds: 10
                  periodSeconds: 15
                livenessProbe:
                  httpGet:
                    path: /-/healthy
                    port: ${toString mon.prometheus.port}
                  initialDelaySeconds: 20
                  periodSeconds: 30
                volumeMounts:
                - name: config
                  mountPath: /etc/prometheus
                - name: data
                  mountPath: /data
                resources:
                  requests:
                    cpu: 100m
                    memory: 256Mi
                  limits:
                    cpu: '1'
                    memory: 1Gi
              volumes:
              - name: config
                configMap:
                  name: prometheus-config
          volumeClaimTemplates:
          - metadata:
              name: data
            spec:
              accessModes: ["ReadWriteOnce"]
              storageClassName: local-path
              resources:
                requests:
                  storage: ${mon.prometheus.storage}
      '';
    }

    # ─── Grafana provisioning (datasource + dashboard provider) ────────
    {
      name = "monitoring/grafana-provisioning.yaml";
      content = ''
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: grafana-datasources
          namespace: ${ns}
        data:
          datasource.yaml: |
            apiVersion: 1
            datasources:
              - name: Prometheus
                type: prometheus
                access: proxy
                url: http://prometheus:${toString mon.prometheus.port}
                isDefault: true
        ---
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: grafana-dashboard-provider
          namespace: ${ns}
        data:
          provider.yaml: |
            apiVersion: 1
            providers:
              - name: soak
                orgId: 1
                type: file
                disableDeletion: false
                editable: true
                options:
                  path: /etc/grafana/dashboards
      '';
    }

    # ─── Grafana dashboard (infra panels; Phase C adds client panels) ──
    {
      name = "monitoring/grafana-dashboard.yaml";
      content = ''
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: grafana-dashboards
          namespace: ${ns}
        data:
          soak.json: |
            {
              "uid": "soak",
              "title": "Message-bus Soak",
              "schemaVersion": 39,
              "version": 1,
              "editable": true,
              "time": { "from": "now-1h", "to": "now" },
              "refresh": "15s",
              "templating": { "list": [] },
              "annotations": { "list": [] },
              "panels": [
                {
                  "type": "timeseries",
                  "title": "Scrape targets up (by job)",
                  "gridPos": { "h": 8, "w": 24, "x": 0, "y": 0 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "targets": [
                    { "expr": "sum by (job) (up)", "legendFormat": "{{job}}", "refId": "A" }
                  ]
                },
                {
                  "type": "timeseries",
                  "title": "Node CPU busy % (by instance)",
                  "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "fieldConfig": { "defaults": { "unit": "percent" }, "overrides": [] },
                  "targets": [
                    { "expr": "100 - (avg by (instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[1m])) * 100)", "legendFormat": "{{instance}}", "refId": "A" }
                  ]
                },
                {
                  "type": "timeseries",
                  "title": "Node memory available % (by instance)",
                  "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "fieldConfig": { "defaults": { "unit": "percent" }, "overrides": [] },
                  "targets": [
                    { "expr": "node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes * 100", "legendFormat": "{{instance}}", "refId": "A" }
                  ]
                },
                {
                  "type": "timeseries",
                  "title": "RabbitMQ messages ready (by node)",
                  "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "targets": [
                    { "expr": "sum by (instance) (rabbitmq_queue_messages_ready)", "legendFormat": "{{instance}}", "refId": "A" }
                  ]
                },
                {
                  "type": "timeseries",
                  "title": "NATS connections (by server)",
                  "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "targets": [
                    { "expr": "gnatsd_varz_connections", "legendFormat": "{{server_id}}", "refId": "A" }
                  ]
                },
                {
                  "type": "row",
                  "title": "Soak clients",
                  "gridPos": { "h": 1, "w": 24, "x": 0, "y": 24 }
                },
                {
                  "type": "timeseries",
                  "title": "Publish vs receive rate (msg/s, by bus)",
                  "gridPos": { "h": 8, "w": 12, "x": 0, "y": 25 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "fieldConfig": { "defaults": { "unit": "cps" }, "overrides": [] },
                  "targets": [
                    { "expr": "sum by (bus) (rate(mbclient_published_total[1m]))", "legendFormat": "{{bus}} pub", "refId": "A" },
                    { "expr": "sum by (bus) (rate(mbclient_received_total[1m]))", "legendFormat": "{{bus}} recv", "refId": "B" }
                  ]
                },
                {
                  "type": "timeseries",
                  "title": "Cumulative lost messages (sequence gaps, by bus)",
                  "gridPos": { "h": 8, "w": 12, "x": 12, "y": 25 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "targets": [
                    { "expr": "sum by (bus) (mbclient_receive_gaps_total)", "legendFormat": "{{bus}}", "refId": "A" }
                  ]
                },
                {
                  "type": "timeseries",
                  "title": "Client reconnects (cumulative, by bus)",
                  "gridPos": { "h": 8, "w": 12, "x": 0, "y": 33 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "targets": [
                    { "expr": "sum by (bus) (mbclient_reconnects_total)", "legendFormat": "{{bus}}", "refId": "A" }
                  ]
                },
                {
                  "type": "timeseries",
                  "title": "Publish/confirm latency p99 (s, by bus)",
                  "gridPos": { "h": 8, "w": 12, "x": 12, "y": 33 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
                  "targets": [
                    { "expr": "histogram_quantile(0.99, sum by (le, bus) (rate(mbclient_request_latency_seconds_bucket[5m])))", "legendFormat": "{{bus}}", "refId": "A" }
                  ]
                },
                {
                  "type": "timeseries",
                  "title": "Publish errors rate (msg/s, by bus)",
                  "gridPos": { "h": 8, "w": 12, "x": 0, "y": 41 },
                  "datasource": { "type": "prometheus", "uid": "Prometheus" },
                  "targets": [
                    { "expr": "sum by (bus) (rate(mbclient_publish_errors_total[1m]))", "legendFormat": "{{bus}}", "refId": "A" }
                  ]
                }
              ]
            }
      '';
    }

    # ─── Grafana (Deployment, pinned to cp0, anonymous Admin) ──────────
    {
      name = "monitoring/grafana.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: grafana
          namespace: ${ns}
          labels:
            app: grafana
        spec:
          type: NodePort
          selector:
            app: grafana
          ports:
          - name: web
            port: ${toString mon.grafana.port}
            targetPort: ${toString mon.grafana.port}
            nodePort: ${toString mon.grafana.nodePort}
        ---
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: grafana
          namespace: ${ns}
        spec:
          replicas: 1
          strategy:
            type: Recreate
          selector:
            matchLabels:
              app: grafana
          template:
            metadata:
              labels:
                app: grafana
            spec:
              affinity:
                nodeAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                    nodeSelectorTerms:
                    - matchExpressions:
                      - key: kubernetes.io/hostname
                        operator: In
                        values: [ ${cp0Host} ]
              containers:
              - name: grafana
                image: ${mon.grafana.image}:${mon.grafana.tag}
                imagePullPolicy: Never
                command: ["/bin/grafana", "server", "--homepath=/share/grafana"]
                env:
                - name: HOME
                  value: /var/lib/grafana
                - name: GF_PATHS_DATA
                  value: /var/lib/grafana
                - name: GF_PATHS_LOGS
                  value: /var/log/grafana
                - name: GF_PATHS_PLUGINS
                  value: /var/lib/grafana/plugins
                - name: GF_PATHS_PROVISIONING
                  value: /etc/grafana/provisioning
                - name: GF_AUTH_ANONYMOUS_ENABLED
                  value: "true"
                - name: GF_AUTH_ANONYMOUS_ORG_ROLE
                  value: "Admin"
                - name: GF_AUTH_DISABLE_LOGIN_FORM
                  value: "true"
                - name: GF_ANALYTICS_REPORTING_ENABLED
                  value: "false"
                - name: GF_ANALYTICS_CHECK_FOR_UPDATES
                  value: "false"
                ports:
                - containerPort: ${toString mon.grafana.port}
                  name: web
                readinessProbe:
                  httpGet:
                    path: /api/health
                    port: ${toString mon.grafana.port}
                  initialDelaySeconds: 10
                  periodSeconds: 15
                livenessProbe:
                  httpGet:
                    path: /api/health
                    port: ${toString mon.grafana.port}
                  initialDelaySeconds: 30
                  periodSeconds: 30
                volumeMounts:
                - name: datasources
                  mountPath: /etc/grafana/provisioning/datasources
                - name: dashboard-provider
                  mountPath: /etc/grafana/provisioning/dashboards
                - name: dashboards
                  mountPath: /etc/grafana/dashboards
                - name: data
                  mountPath: /var/lib/grafana
                - name: logs
                  mountPath: /var/log/grafana
                resources:
                  requests:
                    cpu: 50m
                    memory: 128Mi
                  limits:
                    cpu: 500m
                    memory: 512Mi
              volumes:
              - name: datasources
                configMap:
                  name: grafana-datasources
              - name: dashboard-provider
                configMap:
                  name: grafana-dashboard-provider
              - name: dashboards
                configMap:
                  name: grafana-dashboards
              - name: data
                emptyDir: {}
              - name: logs
                emptyDir: {}
      '';
    }

    # ─── ArgoCD Application ─────────────────────────────────────────────
    {
      name = "monitoring/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: monitoring
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/monitoring
            directory:
              exclude: 'application.yaml'
          destination:
            server: https://kubernetes.default.svc
            namespace: ${ns}
          syncPolicy:
            automated:
              prune: true
              selfHeal: true
      '';
    }
  ];
}
