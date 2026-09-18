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
  ns = c.namespace;
  domain = "svc.${constants.k8s.clusterDomain}";

  routes = builtins.concatStringsSep "\n        " (builtins.map
    (i: "nats://nats-${toString i}.nats-headless.${ns}.${domain}:${toString c.clusterPort}")
    (lib.range 0 (c.replicas - 1)));
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
                podAntiAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                  - labelSelector:
                      matchLabels:
                        app: nats
                    topologyKey: kubernetes.io/hostname
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
