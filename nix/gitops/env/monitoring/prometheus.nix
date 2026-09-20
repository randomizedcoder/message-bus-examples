# nix/gitops/env/monitoring/prometheus.nix
#
# Prometheus Service (NodePort) + StatefulSet, pinned to cp0 with a local-path
# PVC so the TSDB survives rolling node failures. Split out of the old
# monolithic monitoring.nix with no behaviour change.
{ mon, ns, cp0Host }:
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
            # 1Gi crash-looped: after ~26h the TSDB head block (now also
            # carrying the workloads-agents series) crossed 1Gi and every
            # restart then OOMed replaying that head within ~8s. 2Gi gives
            # replay + the extra cardinality ~2x headroom on cp0's ~7.7Gi.
            resources:
              requests:
                cpu: 100m
                memory: 512Mi
              limits:
                cpu: '1'
                memory: 2Gi
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
