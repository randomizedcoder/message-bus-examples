# nix/gitops/env/valkey.nix
#
# ValKey — 3-node replication + Sentinel (rendered-manifests pattern).
#
# HA shape: one logical primary with two replicas, guarded by 3 Sentinels
# that perform automatic failover. Sentinel — NOT Redis/Valkey Cluster
# (sharded) — is the right topology for pub/sub HA: Valkey pub/sub is not
# persisted and does not fan out across a sharded cluster, whereas a
# Sentinel-managed primary keeps a single logical PUBLISH/SUBSCRIBE target
# that survives a node loss.
#
# Each of the 3 StatefulSet pods runs two containers: `valkey` (data) and
# `sentinel` (monitor). On startup each data node asks a Sentinel for the
# current primary and REPLICAOF's it (falling back to valkey-0 on first
# boot). Client access from the host is a NodePort on 6379.
#
# Image: Nix-built messagebus.local/valkey:<tag>, imagePullPolicy: Never.
#
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  c = constants.messageBus.valkey;
  mon = constants.monitoring;
  ns = c.namespace;
  fqdn = "valkey-headless.${ns}.svc.${constants.k8s.clusterDomain}";
  master0 = "valkey-0.${fqdn}";

  # Per-pod NodePort Services: pod valkey-N is individually reachable from the
  # host at <node>:${nodePortClientBase+N} (client) and
  # <node>:${nodePortSentinelBase+N} (sentinel). The pods announce exactly
  # these addresses (start-valkey.sh / start-sentinel.sh), so a host
  # FailoverClient asks a Sentinel for the primary and connects straight to it,
  # following failover. Selector uses the StatefulSet-injected pod-name label.
  perPodServices = map (i: {
    name = "valkey/service-pod-${toString i}.yaml";
    content = ''
      apiVersion: v1
      kind: Service
      metadata:
        name: valkey-${toString i}
        namespace: ${ns}
      spec:
        type: NodePort
        selector:
          statefulset.kubernetes.io/pod-name: valkey-${toString i}
        ports:
        - name: client
          port: ${toString c.clientPort}
          targetPort: ${toString c.clientPort}
          nodePort: ${toString (c.nodePortClientBase + i)}
        - name: sentinel
          port: ${toString c.sentinelPort}
          targetPort: ${toString c.sentinelPort}
          nodePort: ${toString (c.nodePortSentinelBase + i)}
    '';
  }) (lib.range 0 (c.replicas - 1));
in
{
  manifests = [
    # ─── Config + start scripts ────────────────────────────────────────
    {
      name = "valkey/configmap.yaml";
      content = ''
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: valkey-config
          namespace: ${ns}
        data:
          valkey.conf: |
            port ${toString c.clientPort}
            dir /data
            appendonly yes
            protected-mode no
            repl-diskless-sync yes
            repl-diskless-load on-empty-db

          sentinel.conf: |
            port ${toString c.sentinelPort}
            # Seed at a stable node IP + valkey-0's client NodePort. Sentinel
            # then learns the live primary/replica set from node-reachable
            # addresses the pods announce (see start scripts), so a *host*
            # client can follow failover. Quorum 2 of 3.
            sentinel monitor ${c.masterName} ${constants.network.ipv4.cp0} ${toString c.nodePortClientBase} 2
            sentinel down-after-milliseconds ${c.masterName} 5000
            sentinel failover-timeout ${c.masterName} 10000
            sentinel parallel-syncs ${c.masterName} 1

          start-valkey.sh: |
            #!/bin/sh
            set -eu
            ORD="''${HOSTNAME##*-}"
            # Announce a node-reachable address (node IP + this pod's client
            # NodePort) so both host clients and Sentinel reach a replica the
            # same way. podAntiAffinity pins one valkey pod per node, so
            # POD_HOST_IP uniquely identifies this pod.
            ANNOUNCE_PORT=$((${toString c.nodePortClientBase} + ORD))
            CONF=/tmp/valkey.conf
            cat /config/valkey.conf > "$CONF"
            {
              echo "requirepass $VALKEY_PASSWORD"
              echo "masterauth $VALKEY_PASSWORD"
              echo "replica-announce-ip $POD_HOST_IP"
              echo "replica-announce-port $ANNOUNCE_PORT"
            } >> "$CONF"
            # Best-effort bootstrap: ask any Sentinel who the current primary
            # is (returns node-IP + client-NodePort, two lines).
            ADDR=$(valkey-cli -h ${fqdn} -p ${toString c.sentinelPort} \
              sentinel get-master-addr-by-name ${c.masterName} 2>/dev/null || true)
            MIP=$(printf '%s\n' "$ADDR" | head -n1)
            MPORT=$(printf '%s\n' "$ADDR" | tail -n1)
            if [ -n "$MIP" ] && [ "$MIP" != "$POD_HOST_IP" ]; then
              echo "replicaof $MIP $MPORT" >> "$CONF"
            elif [ "$ORD" != "0" ]; then
              echo "replicaof ${master0} ${toString c.clientPort}" >> "$CONF"
            fi
            exec valkey-server "$CONF"

          start-sentinel.sh: |
            #!/bin/sh
            set -eu
            ORD="''${HOSTNAME##*-}"
            # Announce this Sentinel at node IP + its own sentinel NodePort so
            # host clients and peer Sentinels reach it via the node.
            ANNOUNCE_PORT=$((${toString c.nodePortSentinelBase} + ORD))
            CONF=/tmp/sentinel.conf
            cat /config/sentinel.conf > "$CONF"
            {
              echo "sentinel auth-pass ${c.masterName} $VALKEY_PASSWORD"
              echo "sentinel announce-ip $POD_HOST_IP"
              echo "sentinel announce-port $ANNOUNCE_PORT"
            } >> "$CONF"
            exec valkey-sentinel "$CONF"
      '';
    }

    # ─── Headless Service ──────────────────────────────────────────────
    {
      name = "valkey/service-headless.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: valkey-headless
          namespace: ${ns}
        spec:
          clusterIP: None
          publishNotReadyAddresses: true
          selector:
            app: valkey
          ports:
          - name: client
            port: ${toString c.clientPort}
          - name: sentinel
            port: ${toString c.sentinelPort}
      '';
    }

    # ─── Client Service (host NodePort → current primary or any replica) ─
    # NOTE: this Service load-balances across all pods. Reads work
    # anywhere; writes must go to the primary — clients that write should
    # resolve the primary via Sentinel (port 26379). For simple demos the
    # Go client connects here and writes to whichever pod is primary.
    {
      name = "valkey/service.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: valkey
          namespace: ${ns}
        spec:
          type: NodePort
          # Valkey pub/sub is node-local: a PUBLISH only fans out to
          # subscribers on the same node (plus, from the primary, down the
          # replication stream). Without affinity the round-robin NodePort
          # can land a host's publisher and subscriber on different pods and
          # silently drop the message. ClientIP affinity pins all
          # connections from one host to one pod, so host-run pub/sub
          # co-locate and deliver reliably.
          sessionAffinity: ClientIP
          selector:
            app: valkey
          ports:
          - name: client
            port: ${toString c.clientPort}
            targetPort: ${toString c.clientPort}
            nodePort: ${toString c.nodePort}
      '';
    }

    # ─── StatefulSet (valkey + sentinel sidecar) ───────────────────────
    {
      name = "valkey/statefulset.yaml";
      content = ''
        apiVersion: apps/v1
        kind: StatefulSet
        metadata:
          name: valkey
          namespace: ${ns}
        spec:
          serviceName: valkey-headless
          replicas: ${toString c.replicas}
          selector:
            matchLabels:
              app: valkey
          template:
            metadata:
              labels:
                app: valkey
            spec:
              affinity:
                podAntiAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                  - labelSelector:
                      matchLabels:
                        app: valkey
                    topologyKey: kubernetes.io/hostname
              containers:
              - name: valkey
                image: ${c.image}:${c.tag}
                imagePullPolicy: Never
                command: ["/bin/sh", "/config/start-valkey.sh"]
                env:
                - name: VALKEY_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: valkey-credentials
                      key: password
                # Node IP backing this pod's NodePort — announced so replicas
                # and Sentinel are reachable from the host (see start scripts).
                - name: POD_HOST_IP
                  valueFrom:
                    fieldRef:
                      fieldPath: status.hostIP
                ports:
                - containerPort: ${toString c.clientPort}
                  name: client
                readinessProbe:
                  exec:
                    command: ["/bin/sh", "-c", "valkey-cli -a \"$VALKEY_PASSWORD\" -p ${toString c.clientPort} ping | grep -q PONG"]
                  initialDelaySeconds: 5
                  periodSeconds: 10
                volumeMounts:
                - name: config
                  mountPath: /config
                - name: data
                  mountPath: /data
                # No /tmp in the minimal Nix image; the start script writes
                # (and valkey-server rewrites) its config there.
                - name: tmp
                  mountPath: /tmp
                resources:
                  requests:
                    cpu: 100m
                    memory: 128Mi
                  limits:
                    cpu: 500m
                    memory: 512Mi
              - name: sentinel
                image: ${c.image}:${c.tag}
                imagePullPolicy: Never
                command: ["/bin/sh", "/config/start-sentinel.sh"]
                env:
                - name: VALKEY_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: valkey-credentials
                      key: password
                - name: POD_HOST_IP
                  valueFrom:
                    fieldRef:
                      fieldPath: status.hostIP
                ports:
                - containerPort: ${toString c.sentinelPort}
                  name: sentinel
                volumeMounts:
                - name: config
                  mountPath: /config
                # Sentinel rewrites its config file at runtime (persisted
                # master/replica state); /tmp must exist and be writable.
                - name: tmp
                  mountPath: /tmp
                resources:
                  requests:
                    cpu: 50m
                    memory: 64Mi
                  limits:
                    cpu: 250m
                    memory: 128Mi
              # redis_exporter sidecar: scrapes this pod's Valkey over localhost
              # with the shared password and exposes Prometheus metrics on
              # :${toString mon.redisExporter.port}. Prometheus scrapes each pod
              # via the headless Service (see the `redis` job in monitoring.nix)
              # so the Valkey/Redis Grafana dashboards have per-node data.
              - name: redis-exporter
                image: ${mon.redisExporter.image}:${mon.redisExporter.tag}
                imagePullPolicy: Never
                command: ["/bin/redis_exporter"]
                args:
                - "-redis.addr=redis://localhost:${toString c.clientPort}"
                - "-web.listen-address=:${toString mon.redisExporter.port}"
                env:
                # redis_exporter reads REDIS_PASSWORD from the environment, so
                # the password stays out of the process args / ps output.
                - name: REDIS_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: valkey-credentials
                      key: password
                ports:
                - containerPort: ${toString mon.redisExporter.port}
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
                  name: valkey-config
                  defaultMode: 0755
              - name: tmp
                emptyDir: {}
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

    # ─── ArgoCD Application ─────────────────────────────────────────────
    {
      name = "valkey/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: valkey
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/valkey
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
  ] ++ perPodServices;
}
