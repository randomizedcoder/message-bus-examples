# nix/gitops/env/mqtt.nix
#
# MQTT — Eclipse Mosquitto, 3-node full-mesh bridged (rendered-manifests).
#
# Mosquitto has no native clustering, so we mesh three independent brokers
# with MQTT bridges: each broker forwards its LOCALLY-published messages
# OUT to the other two (`topic # out` + `try_private`). Because bridged
# messages are flagged and never re-forwarded over another bridge, a
# publish on any node reaches subscribers on all nodes exactly once — no
# loops, no duplicate delivery — and the set keeps serving through a node
# loss. Each pod's bridge blocks (to its peers, excluding itself) are
# generated at startup from the pod ordinal.
#
# Image: Nix-built messagebus.local/mosquitto:<tag>, imagePullPolicy: Never.
#
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  c = constants.messageBus.mqtt;
  mon = constants.monitoring;
  ns = c.namespace;
  fqdn = "mqtt-headless.${ns}.svc.${constants.k8s.clusterDomain}";
in
{
  manifests = [
    # ─── Config + start script ─────────────────────────────────────────
    {
      name = "mqtt/configmap.yaml";
      content = ''
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: mqtt-config
          namespace: ${ns}
        data:
          mosquitto.conf: |
            # The minimal Nix image has no 'mosquitto'/'nobody' user, so
            # tell mosquitto to keep running as root instead of trying (and
            # failing) to drop privileges.
            user root
            listener ${toString c.mqttPort} 0.0.0.0
            allow_anonymous true
            persistence true
            persistence_location /data/

          start-mosquitto.sh: |
            #!/bin/sh
            set -eu
            ORD="''${HOSTNAME##*-}"
            CONF=/tmp/mosquitto.conf
            cat /config/mosquitto.conf > "$CONF"
            # Full-mesh bridge: forward local pubs OUT to every other pod.
            # (printf appends, not a heredoc — keeps the config indentation-safe.)
            i=0
            while [ "$i" -lt ${toString c.replicas} ]; do
              if [ "$i" != "$ORD" ]; then
                peer="mqtt-$i.${fqdn}"
                {
                  printf '\nconnection bridge-to-%s\n' "$i"
                  printf 'address %s:%s\n' "$peer" "${toString c.mqttPort}"
                  printf 'topic # out 0 "" ""\n'
                  # Bridge over MQTT 3.1.1, not v5: Mosquitto 2.x v5 bridges
                  # negotiate topic aliases and then tear themselves down with
                  # "PUBLISH invalid topic alias" protocol errors. 3.1.1 has no
                  # topic aliases and bridges reliably. (Client↔broker links
                  # are independent and can still use v5.)
                  printf 'bridge_protocol_version mqttv311\n'
                  printf 'cleansession true\n'
                  printf 'try_private true\n'
                  printf 'notifications false\n'
                  printf 'restart_timeout 5\n'
                } >> "$CONF"
              fi
              i=$((i + 1))
            done
            exec mosquitto -c "$CONF"
      '';
    }

    # ─── Headless Service (peer DNS for bridges) ───────────────────────
    {
      name = "mqtt/service-headless.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: mqtt-headless
          namespace: ${ns}
        spec:
          clusterIP: None
          publishNotReadyAddresses: true
          selector:
            app: mqtt
          ports:
          - name: mqtt
            port: ${toString c.mqttPort}
      '';
    }

    # ─── Client Service (in-cluster ClusterIP + host NodePort) ─────────
    {
      name = "mqtt/service.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: mqtt
          namespace: ${ns}
        spec:
          type: NodePort
          # Pin each host's connections to one broker. The round-robin
          # NodePort can otherwise land a host's publisher and subscriber on
          # different brokers; the bridge mesh does forward between them, but
          # subscription-propagation latency across the bridge can drop the
          # very first message. ClientIP affinity co-locates host-run pub/sub
          # on one broker for deterministic local delivery. (The full-mesh
          # bridges still carry messages between genuinely distributed
          # clients on different brokers.)
          sessionAffinity: ClientIP
          selector:
            app: mqtt
          ports:
          - name: mqtt
            port: ${toString c.mqttPort}
            targetPort: ${toString c.mqttPort}
            nodePort: ${toString c.nodePort}
      '';
    }

    # ─── StatefulSet ───────────────────────────────────────────────────
    {
      name = "mqtt/statefulset.yaml";
      content = ''
        apiVersion: apps/v1
        kind: StatefulSet
        metadata:
          name: mqtt
          namespace: ${ns}
        spec:
          serviceName: mqtt-headless
          replicas: ${toString c.replicas}
          podManagementPolicy: Parallel
          selector:
            matchLabels:
              app: mqtt
          template:
            metadata:
              labels:
                app: mqtt
            spec:
              affinity:
                podAntiAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                  - labelSelector:
                      matchLabels:
                        app: mqtt
                    topologyKey: kubernetes.io/hostname
              containers:
              - name: mosquitto
                image: ${c.image}:${c.tag}
                imagePullPolicy: Never
                command: ["/bin/sh", "/config/start-mosquitto.sh"]
                ports:
                - containerPort: ${toString c.mqttPort}
                  name: mqtt
                readinessProbe:
                  tcpSocket:
                    port: ${toString c.mqttPort}
                  initialDelaySeconds: 5
                  periodSeconds: 10
                livenessProbe:
                  tcpSocket:
                    port: ${toString c.mqttPort}
                  initialDelaySeconds: 10
                  periodSeconds: 15
                volumeMounts:
                - name: config
                  mountPath: /config
                - name: data
                  mountPath: /data
                # The minimal Nix image has no /tmp; the start script writes
                # its generated mosquitto.conf there, so provide a writable one.
                - name: tmp
                  mountPath: /tmp
                resources:
                  requests:
                    cpu: 50m
                    memory: 64Mi
                  limits:
                    cpu: 250m
                    memory: 256Mi
              # $SYS → Prometheus bridge, co-located per pod (design §11.5).
              # Subscribes to this broker's own $SYS tree over localhost —
              # $SYS is broker-local (never bridged), so per-pod scraping gives
              # per-broker stats. No readinessProbe: it must never gate the
              # broker's own client readiness (mirrors the NATS exporter).
              - name: sysexporter
                image: ${mon.mosquittoExporter.image}:${mon.mosquittoExporter.tag}
                imagePullPolicy: Never
                args:
                - "-addr"
                - "localhost:${toString c.mqttPort}"
                - "-metrics-addr"
                - ":${toString mon.mosquittoExporter.port}"
                ports:
                - containerPort: ${toString mon.mosquittoExporter.port}
                  name: metrics
                resources:
                  requests:
                    cpu: 25m
                    memory: 32Mi
                  limits:
                    cpu: 200m
                    memory: 64Mi
              volumes:
              - name: config
                configMap:
                  name: mqtt-config
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
                  storage: 1Gi
      '';
    }

    # ─── ArgoCD Application ─────────────────────────────────────────────
    {
      name = "mqtt/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: mqtt
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/mqtt
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
