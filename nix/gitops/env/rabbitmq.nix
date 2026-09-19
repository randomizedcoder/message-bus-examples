# nix/gitops/env/rabbitmq.nix
#
# RabbitMQ — 3-node cluster via the k8s peer-discovery plugin
# (rendered-manifests pattern, no operator).
#
# The three pods form one cluster using the bundled
# `rabbitmq_peer_discovery_k8s` plugin: each node queries the Kubernetes
# API for the endpoints of the headless Service and peers with the others
# by hostname. Every node presents the same Erlang cookie (from the
# rabbitmq-credentials Secret). Quorum queues (declared by clients) then
# replicate across the three nodes; `pause_minority` fences a partitioned
# minority. Client access from the host is a NodePort on the AMQP port
# (plus one for the management UI).
#
# Image: Nix-built messagebus.local/rabbitmq:<tag>, imagePullPolicy: Never.
#
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  c = constants.messageBus.rabbitmq;
  ns = c.namespace;
  suffix = ".rabbitmq-headless.${ns}.svc.${constants.k8s.clusterDomain}";
in
{
  manifests = [
    # ─── RBAC: let the peer-discovery plugin read endpoints ────────────
    {
      name = "rabbitmq/rbac.yaml";
      content = ''
        apiVersion: v1
        kind: ServiceAccount
        metadata:
          name: rabbitmq
          namespace: ${ns}
        ---
        apiVersion: rbac.authorization.k8s.io/v1
        kind: Role
        metadata:
          name: rabbitmq-peer-discovery
          namespace: ${ns}
        rules:
        - apiGroups: [""]
          resources: ["endpoints"]
          verbs: ["get", "list"]
        ---
        apiVersion: rbac.authorization.k8s.io/v1
        kind: RoleBinding
        metadata:
          name: rabbitmq-peer-discovery
          namespace: ${ns}
        roleRef:
          apiGroup: rbac.authorization.k8s.io
          kind: Role
          name: rabbitmq-peer-discovery
        subjects:
        - kind: ServiceAccount
          name: rabbitmq
          namespace: ${ns}
      '';
    }

    # ─── Config + enabled plugins + entrypoint ─────────────────────────
    {
      name = "rabbitmq/configmap.yaml";
      content = ''
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: rabbitmq-config
          namespace: ${ns}
        data:
          enabled_plugins: |
            [rabbitmq_management,rabbitmq_peer_discovery_k8s,rabbitmq_prometheus].

          rabbitmq.conf: |
            cluster_formation.peer_discovery_backend = rabbit_peer_discovery_k8s
            cluster_formation.k8s.host = kubernetes.default.svc.${constants.k8s.clusterDomain}
            cluster_formation.k8s.address_type = hostname
            cluster_formation.k8s.service_name = rabbitmq-headless
            cluster_formation.k8s.hostname_suffix = ${suffix}
            cluster_partition_handling = pause_minority
            queue_master_locator = min-masters
            loopback_users = none
            management.tcp.port = ${toString c.mgmtPort}

          start-rabbitmq.sh: |
            #!/bin/sh
            set -eu
            export HOME=/var/lib/rabbitmq
            mkdir -p "$HOME"
            # Shared Erlang cookie (mounted from the Secret) → the file
            # every node reads to authenticate to its peers.
            cp /secret/RABBITMQ_ERLANG_COOKIE "$HOME/.erlang.cookie"
            chmod 600 "$HOME/.erlang.cookie"
            # RABBITMQ_NODENAME / RABBITMQ_USE_LONGNAME come from the pod env
            # (see the StatefulSet) so the CLI tools agree with the server.
            export RABBITMQ_CONFIG_FILE=/etc/rabbitmq/rabbitmq
            export RABBITMQ_ENABLED_PLUGINS_FILE=/etc/rabbitmq/enabled_plugins
            exec rabbitmq-server
      '';
    }

    # ─── Headless Service (peer discovery + stable pod DNS) ────────────
    {
      name = "rabbitmq/service-headless.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: rabbitmq-headless
          namespace: ${ns}
        spec:
          clusterIP: None
          publishNotReadyAddresses: true
          selector:
            app: rabbitmq
          ports:
          - name: amqp
            port: ${toString c.amqpPort}
          - name: epmd
            port: ${toString c.epmdPort}
          - name: dist
            port: ${toString c.distPort}
          - name: management
            port: ${toString c.mgmtPort}
          - name: prometheus
            port: ${toString c.prometheusPort}
      '';
    }

    # ─── Client Service (host NodePorts: AMQP + management UI) ──────────
    {
      name = "rabbitmq/service.yaml";
      content = ''
        apiVersion: v1
        kind: Service
        metadata:
          name: rabbitmq
          namespace: ${ns}
        spec:
          type: NodePort
          selector:
            app: rabbitmq
          ports:
          - name: amqp
            port: ${toString c.amqpPort}
            targetPort: ${toString c.amqpPort}
            nodePort: ${toString c.nodePortAmqp}
          - name: management
            port: ${toString c.mgmtPort}
            targetPort: ${toString c.mgmtPort}
            nodePort: ${toString c.nodePortMgmt}
      '';
    }

    # ─── StatefulSet ───────────────────────────────────────────────────
    {
      name = "rabbitmq/statefulset.yaml";
      content = ''
        apiVersion: apps/v1
        kind: StatefulSet
        metadata:
          name: rabbitmq
          namespace: ${ns}
        spec:
          serviceName: rabbitmq-headless
          replicas: ${toString c.replicas}
          podManagementPolicy: OrderedReady
          selector:
            matchLabels:
              app: rabbitmq
          template:
            metadata:
              labels:
                app: rabbitmq
            spec:
              serviceAccountName: rabbitmq
              terminationGracePeriodSeconds: 60
              affinity:
                podAntiAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                  - labelSelector:
                      matchLabels:
                        app: rabbitmq
                    topologyKey: kubernetes.io/hostname
              containers:
              - name: rabbitmq
                image: ${c.image}:${c.tag}
                imagePullPolicy: Never
                command: ["/bin/sh", "/etc/rabbitmq/start-rabbitmq.sh"]
                # Node name + long-name mode live in the POD env (not just
                # the start script) so that rabbitmqctl / rabbitmq-diagnostics
                # — including the readiness/liveness probes — target the same
                # `rabbit@<pod>.<headless-fqdn>` node the server runs as.
                # (The Erlang cookie arrives via envFrom below, so the CLI
                # authenticates too.) ELIXIR_ERL_OPTIONS=+fnu forces UTF-8
                # filename encoding — the minimal Nix image has no locale
                # archive, so Erlang would otherwise default to latin1.
                env:
                - name: RABBITMQ_POD_NAME
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.name
                - name: RABBITMQ_USE_LONGNAME
                  value: "true"
                - name: RABBITMQ_NODENAME
                  value: "rabbit@$(RABBITMQ_POD_NAME)${suffix}"
                - name: ELIXIR_ERL_OPTIONS
                  value: "+fnu"
                # Default user/password + Erlang cookie come from the Secret.
                envFrom:
                - secretRef:
                    name: rabbitmq-credentials
                ports:
                - containerPort: ${toString c.amqpPort}
                  name: amqp
                - containerPort: ${toString c.mgmtPort}
                  name: management
                - containerPort: ${toString c.epmdPort}
                  name: epmd
                - containerPort: ${toString c.distPort}
                  name: dist
                readinessProbe:
                  exec:
                    command: ["/bin/sh", "-c", "rabbitmq-diagnostics -q ping"]
                  initialDelaySeconds: 20
                  periodSeconds: 15
                  timeoutSeconds: 10
                livenessProbe:
                  exec:
                    command: ["/bin/sh", "-c", "rabbitmq-diagnostics -q ping"]
                  initialDelaySeconds: 60
                  periodSeconds: 30
                  timeoutSeconds: 15
                volumeMounts:
                - name: config
                  mountPath: /etc/rabbitmq
                - name: cookie
                  mountPath: /secret
                  readOnly: true
                - name: data
                  mountPath: /var/lib/rabbitmq
                resources:
                  requests:
                    cpu: 200m
                    memory: 384Mi
                  limits:
                    cpu: '1'
                    memory: 1Gi
              volumes:
              - name: config
                configMap:
                  name: rabbitmq-config
                  defaultMode: 0755
              - name: cookie
                secret:
                  secretName: rabbitmq-credentials
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
      name = "rabbitmq/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: rabbitmq
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/rabbitmq
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
