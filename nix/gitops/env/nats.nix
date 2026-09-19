# nix/gitops/env/nats.nix
#
# NATS — 3-node JetStream cluster (rendered-manifests pattern).
#
# A StatefulSet of 3 nats-server pods, each running with JetStream
# enabled and full-mesh cluster routes to the other two via the
# headless Service DNS. server_name is the pod name (unique per pod,
# required by JetStream's Raft meta-group). Client access from the host
# is via a NodePort Service on the client port (4222).
#
# Image: Nix-built messagebus.local/nats:<tag>, preloaded into containerd
# (imagePullPolicy: Never).
#
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  c = constants.messageBus.nats;
  mon = constants.monitoring;
  ns = c.namespace;
  domain = "svc.${constants.k8s.clusterDomain}";

  routes = builtins.concatStringsSep "\n        " (builtins.map
    (i: "nats://nats-${toString i}.nats-headless.${ns}.${domain}:${toString c.clusterPort}")
    (lib.range 0 (c.replicas - 1)));

  # Leaf-node topology. The hub (the 3-node cluster) runs on the control-plane
  # nodes and accepts leaf connections on leafPort; a single leaf pod runs on
  # the worker node and dials the hub's leafnode listener via the headless
  # Service DNS. server_name / hostnames come from constants so they track the
  # live node labels (kubernetes.io/hostname = k8s-<node>).
  hubLeafURL   = "nats://nats-headless.${ns}.${domain}:${toString c.leafPort}";
  cpHostnames  = builtins.map constants.getHostname [ "cp0" "cp1" "cp2" ];
  leafHostname = constants.getHostname "w3";
in
{
  manifests = [
    # ─── Server config ────────────────────────────────────────────────
    {
      name = "nats/configmap.yaml";
      content = ''
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: nats-config
          namespace: ${ns}
        data:
          nats.conf: |
            # $POD_NAME is expanded from the environment at startup.
            server_name: $POD_NAME
            listen: 0.0.0.0:${toString c.clientPort}
            http: 0.0.0.0:${toString c.monitorPort}

            jetstream {
              store_dir: /data/jetstream
              max_memory_store: 256MB
              max_file_store: 2GB
            }

            # Accept leaf-node connections (the leaf pod on the worker dials
            # this listener and bridges subject interest into the cluster).
            leafnodes {
              listen: 0.0.0.0:${toString c.leafPort}
            }

            cluster {
              name: nats-cluster
              listen: 0.0.0.0:${toString c.clusterPort}
              routes: [
                ${routes}
              ]
            }
      '';
    }

    # ─── Headless Service (peer discovery + stable pod DNS) ────────────
    {
      name = "nats/service-headless.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: nats-headless
          namespace: ${ns}
        spec:
          clusterIP: None
          publishNotReadyAddresses: true
          selector:
            app: nats
          ports:
          - name: client
            port: ${toString c.clientPort}
          - name: cluster
            port: ${toString c.clusterPort}
          - name: monitor
            port: ${toString c.monitorPort}
          - name: leaf
            port: ${toString c.leafPort}
      '';
    }

    # ─── Client Service (in-cluster ClusterIP + host NodePort) ─────────
    {
      name = "nats/service.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: nats
          namespace: ${ns}
        spec:
          type: NodePort
          selector:
            app: nats
          ports:
          - name: client
            port: ${toString c.clientPort}
            targetPort: ${toString c.clientPort}
            nodePort: ${toString c.nodePort}
      '';
    }

    # ─── StatefulSet ───────────────────────────────────────────────────
    {
      name = "nats/statefulset.yaml";
      content = ''
        apiVersion: apps/v1
        kind: StatefulSet
        metadata:
          name: nats
          namespace: ${ns}
        spec:
          serviceName: nats-headless
          replicas: ${toString c.replicas}
          podManagementPolicy: Parallel
          selector:
            matchLabels:
              app: nats
          template:
            metadata:
              labels:
                app: nats
            spec:
              affinity:
                # One nats pod per node ...
                podAntiAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                  - labelSelector:
                      matchLabels:
                        app: nats
                    topologyKey: kubernetes.io/hostname
                # ... and keep the cluster (hub) on the control-plane nodes,
                # leaving the worker node free for the single leaf pod.
                nodeAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                    nodeSelectorTerms:
                    - matchExpressions:
                      - key: kubernetes.io/hostname
                        operator: In
                        values: [ ${builtins.concatStringsSep ", " cpHostnames} ]
              containers:
              - name: nats
                image: ${c.image}:${c.tag}
                imagePullPolicy: Never
                command: ["/bin/nats-server", "-c", "/etc/nats/nats.conf"]
                env:
                - name: POD_NAME
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.name
                ports:
                - containerPort: ${toString c.clientPort}
                  name: client
                - containerPort: ${toString c.clusterPort}
                  name: cluster
                - containerPort: ${toString c.monitorPort}
                  name: monitor
                - containerPort: ${toString c.leafPort}
                  name: leaf
                livenessProbe:
                  httpGet:
                    path: /healthz
                    port: ${toString c.monitorPort}
                  initialDelaySeconds: 10
                  periodSeconds: 15
                readinessProbe:
                  httpGet:
                    path: /healthz
                    port: ${toString c.monitorPort}
                  initialDelaySeconds: 5
                  periodSeconds: 10
                volumeMounts:
                - name: config
                  mountPath: /etc/nats
                - name: data
                  mountPath: /data
                resources:
                  requests:
                    cpu: 100m
                    memory: 128Mi
                  limits:
                    cpu: 500m
                    memory: 512Mi
              # prometheus-nats-exporter sidecar — scrapes THIS pod's nats-server
              # over localhost and exposes Prometheus metrics on the exporter
              # port, scraped per-pod over the headless Service. One exporter per
              # server (co-located) mirrors the redis_exporter/Valkey sidecar and
              # follows the exporter's own guidance: a single aggregating exporter
              # returns HTTP 500 for the whole scrape when any one monitored
              # server is briefly unreachable, so under rolling node faults it
              # gapped every NATS/JetStream panel. Per pod, a node fault gaps only
              # that one server's target. No readinessProbe: the exporter must
              # never gate the nats-server's own client readiness.
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
                - "http://localhost:${toString c.monitorPort}"
                ports:
                - containerPort: ${toString mon.natsExporter.port}
                  name: metrics
                resources:
                  requests:
                    cpu: 25m
                    memory: 32Mi
                  limits:
                    cpu: 200m
                    memory: 128Mi
              volumes:
              - name: config
                configMap:
                  name: nats-config
          volumeClaimTemplates:
          - metadata:
              name: data
            spec:
              accessModes: ["ReadWriteOnce"]
              storageClassName: local-path
              resources:
                requests:
                  storage: 2Gi
      '';
    }

    # ─── Leaf node: config ─────────────────────────────────────────────
    # A standalone nats-server (no cluster, no JetStream) that dials the hub's
    # leafnode listener and bridges subject interest. Clients attached here see
    # a normal NATS server; messages cross the leaf link to/from the cluster.
    {
      name = "nats/leaf-configmap.yaml";
      content = ''
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: nats-leaf-config
          namespace: ${ns}
        data:
          nats.conf: |
            # $POD_NAME is expanded from the environment at startup.
            server_name: $POD_NAME
            listen: 0.0.0.0:${toString c.clientPort}
            http: 0.0.0.0:${toString c.monitorPort}

            leafnodes {
              remotes: [
                { url: "${hubLeafURL}" }
              ]
            }
      '';
    }

    # ─── Leaf node: headless Service (stable pod DNS) ──────────────────
    {
      name = "nats/leaf-service-headless.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: nats-leaf-headless
          namespace: ${ns}
        spec:
          clusterIP: None
          publishNotReadyAddresses: true
          selector:
            app: nats-leaf
          ports:
          - name: client
            port: ${toString c.clientPort}
          - name: monitor
            port: ${toString c.monitorPort}
      '';
    }

    # ─── Leaf node: client Service (host NodePort) ─────────────────────
    {
      name = "nats/leaf-service.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: nats-leaf
          namespace: ${ns}
        spec:
          type: NodePort
          selector:
            app: nats-leaf
          ports:
          - name: client
            port: ${toString c.clientPort}
            targetPort: ${toString c.clientPort}
            nodePort: ${toString c.nodePortLeaf}
      '';
    }

    # ─── Leaf node: StatefulSet (single pod, pinned to the worker) ─────
    {
      name = "nats/leaf-statefulset.yaml";
      content = ''
        apiVersion: apps/v1
        kind: StatefulSet
        metadata:
          name: nats-leaf
          namespace: ${ns}
        spec:
          serviceName: nats-leaf-headless
          replicas: 1
          selector:
            matchLabels:
              app: nats-leaf
          template:
            metadata:
              labels:
                app: nats-leaf
            spec:
              nodeSelector:
                kubernetes.io/hostname: ${leafHostname}
              containers:
              - name: nats
                image: ${c.image}:${c.tag}
                imagePullPolicy: Never
                command: ["/bin/nats-server", "-c", "/etc/nats/nats.conf"]
                env:
                - name: POD_NAME
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.name
                ports:
                - containerPort: ${toString c.clientPort}
                  name: client
                - containerPort: ${toString c.monitorPort}
                  name: monitor
                livenessProbe:
                  httpGet:
                    path: /healthz
                    port: ${toString c.monitorPort}
                  initialDelaySeconds: 10
                  periodSeconds: 15
                readinessProbe:
                  httpGet:
                    path: /healthz
                    port: ${toString c.monitorPort}
                  initialDelaySeconds: 5
                  periodSeconds: 10
                volumeMounts:
                - name: config
                  mountPath: /etc/nats
                resources:
                  requests:
                    cpu: 100m
                    memory: 128Mi
                  limits:
                    cpu: 500m
                    memory: 512Mi
              # prometheus-nats-exporter sidecar — see the hub StatefulSet above.
              # One exporter per server, co-located, scraped per-pod so a worker
              # fault gaps only the leaf's target, not the hub/JetStream metrics.
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
                - "http://localhost:${toString c.monitorPort}"
                ports:
                - containerPort: ${toString mon.natsExporter.port}
                  name: metrics
                resources:
                  requests:
                    cpu: 25m
                    memory: 32Mi
                  limits:
                    cpu: 200m
                    memory: 128Mi
              volumes:
              - name: config
                configMap:
                  name: nats-leaf-config
      '';
    }

    # ─── ArgoCD Application ─────────────────────────────────────────────
    {
      name = "nats/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: nats
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/nats
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
