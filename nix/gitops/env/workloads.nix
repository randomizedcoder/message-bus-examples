# nix/gitops/env/workloads.nix
#
# proto-bench region agents (design §9.2). Four single-pod Deployments, one per
# "region", each pinned to its MicroVM by nodeSelector, plus:
#   • a per-region NodePort Service with externalTrafficPolicy: Local, so a
#     host connection to 10.33.33.1x:3071x is served by that node's pod and
#     nothing else (the geography of the demo);
#   • a headless Service `region-agents` giving each pod a stable DNS name
#     (region-agent-<region>.region-agents.workloads.svc:9464) for per-pod
#     Prometheus scraping;
#   • a `region-agent-global` NodePort (30700, default policy) as the "global
#     API" endpoint for the WatchWorkload / log demos;
#   • an ArgoCD Application (registered in nix/gitops/default.nix).
#
# GOGC/GOMEMLIMIT are set per GC profile by the k8s-proto-bench harness via
# `kubectl set env` (design §9.2); the baseline here is GOGC=100. Bus flags and
# credential envFrom arrive with the bus responders in P3 — in P2 the agent
# serves gRPC only.
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  mb = constants.messageBus;
  wl = mb.workloads;
  ns = wl.namespace;
  g  = toString wl.grpcPort;
  m  = toString wl.metricsPort;
  domain = "svc.${constants.k8s.clusterDomain}";

  # In-cluster bus endpoints the agent's responders/consumers connect to
  # (design §3.9). NATS + MQTT are unauthenticated; the RabbitMQ user/pass reach
  # the URL via $(VAR) expansion from the rabbitmq-credentials Secret (envFrom),
  # and the Valkey password via VALKEY_PASSWORD (secretKeyRef) — both replicated
  # into the workloads namespace by nix/secrets.nix.
  natsURL = "nats://nats.${mb.nats.namespace}.${domain}:${toString mb.nats.clientPort}";
  amqpURL = "amqp://$(RABBITMQ_DEFAULT_USER):$(RABBITMQ_DEFAULT_PASS)@rabbitmq.${mb.rabbitmq.namespace}.${domain}:${toString mb.rabbitmq.amqpPort}/";
  mqttAddr = "mqtt.${mb.mqtt.namespace}.${domain}:${toString mb.mqtt.mqttPort}";
  valkeySentinels = builtins.concatStringsSep "," (map
    (i: "valkey-${toString i}.valkey-headless.${mb.valkey.namespace}.${domain}:${toString mb.valkey.sentinelPort}")
    (lib.range 0 (mb.valkey.replicas - 1)));

  # One entry per node, in nodeNames order so the NodePort index is stable.
  agents = lib.imap0 (i: node: {
    region   = wl.regions.${node};
    hostname = constants.getHostname node;   # k8s-<node>
    nodePort = wl.nodePortGrpcBase + i;       # 30710 + index
  }) constants.nodeNames;

  globalRegion = wl.regions.cp0;

  mkAgent = a: {
    name = "workloads/region-agent-${a.region}.yaml";
    content = ''
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: region-agent-${a.region}
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: region-agent
          app: region-agent-${a.region}
      spec:
        replicas: 1
        selector:
          matchLabels:
            app: region-agent-${a.region}
        template:
          metadata:
            labels:
              app.kubernetes.io/name: region-agent
              app: region-agent-${a.region}
          spec:
            hostname: region-agent-${a.region}
            subdomain: region-agents
            nodeSelector:
              kubernetes.io/hostname: ${a.hostname}
            containers:
            - name: region-agent
              image: ${wl.image}:${wl.tag}
              imagePullPolicy: Never
              args:
              - -region=${a.region}
              - -grpc-addr=:${g}
              - -metrics-addr=:${m}
              - -codec=proto
              - -pool=all
              - -nats=${natsURL}
              - -amqp=${amqpURL}
              - -valkey-sentinels=${valkeySentinels}
              - -mqtt=${mqttAddr}
              env:
              - name: REGION
                value: ${a.region}
              - name: GOGC
                value: "100"
              - name: VALKEY_PASSWORD
                valueFrom:
                  secretKeyRef:
                    name: valkey-credentials
                    key: password
              envFrom:
              - secretRef:
                  name: rabbitmq-credentials
              ports:
              - containerPort: ${g}
                name: grpc
              - containerPort: ${m}
                name: metrics
              readinessProbe:
                httpGet:
                  path: /healthz
                  port: ${m}
                initialDelaySeconds: 3
                periodSeconds: 10
              livenessProbe:
                httpGet:
                  path: /healthz
                  port: ${m}
                initialDelaySeconds: 10
                periodSeconds: 15
              resources:
                requests:
                  cpu: 200m
                  memory: 128Mi
                limits:
                  cpu: "2"
                  memory: 512Mi
      ---
      apiVersion: v1
      kind: Service
      metadata:
        name: region-agent-${a.region}
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: region-agent
      spec:
        type: NodePort
        externalTrafficPolicy: Local
        selector:
          app: region-agent-${a.region}
        ports:
        - name: grpc
          port: ${g}
          targetPort: ${g}
          nodePort: ${toString a.nodePort}
    '';
  };

  shared = {
    name = "workloads/region-agents-shared.yaml";
    content = ''
      apiVersion: v1
      kind: Service
      metadata:
        name: region-agents
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: region-agent
      spec:
        clusterIP: None
        selector:
          app.kubernetes.io/name: region-agent
        ports:
        - name: metrics
          port: ${m}
          targetPort: ${m}
        - name: grpc
          port: ${g}
          targetPort: ${g}
      ---
      apiVersion: v1
      kind: Service
      metadata:
        name: region-agent-global
        namespace: ${ns}
      spec:
        type: NodePort
        selector:
          app: region-agent-${globalRegion}
        ports:
        - name: grpc
          port: ${g}
          targetPort: ${g}
          nodePort: ${toString wl.nodePortGlobalApi}
    '';
  };

  application = {
    name = "workloads/application.yaml";
    content = ''
      apiVersion: argoproj.io/v1alpha1
      kind: Application
      metadata:
        name: workloads
        namespace: argocd
      spec:
        project: default
        source:
          repoURL: ${constants.gitops.repoURL}
          targetRevision: ${constants.gitops.targetRevision}
          path: ${constants.gitops.renderedPath}/workloads
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
  };
in
{
  manifests = (map mkAgent agents) ++ [ shared application ];
}
