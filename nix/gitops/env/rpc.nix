# nix/gitops/env/rpc.nix
#
# RPC lab in-cluster deployment (§17). Two single-pod Deployments in the `rpc`
# namespace:
#   • rpc-service — the terminal GatewayService backend, reached only inside the
#     cluster via a ClusterIP Service (rpc-service.rpc.svc:9440);
#   • rpc-gateway — gateway-B, a GatewayService ingress that forwards to
#     rpc-service, exposed on a NodePort so the host driver (a matching
#     gateway-A) can reach it: host → gateway-A → gateway-B (NodePort) →
#     rpc-service. Everything speaks GatewayService.Call, so every hop is uniform.
#
# Both images are Nix-built Go binaries preloaded into containerd
# (imagePullPolicy: Never). This phase is gRPC only; the bus transports (later
# phases) reuse this gateway-B next to the brokers. Probes are tcpSocket on the
# gRPC port — the binaries serve gRPC only, with no HTTP health endpoint.
#
# An ArgoCD Application (registered in nix/gitops/default.nix) syncs rendered/rpc.
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  mb = constants.messageBus;
  rpc = mb.rpc;
  ns = rpc.namespace;
  gw = rpc.gateway;
  svc = rpc.service;
  g = toString gw.grpcPort;
  s = toString svc.grpcPort;
  domain = "svc.${constants.k8s.clusterDomain}";
  serviceTarget = "rpc-service.${ns}.${domain}:${s}";

  service = {
    name = "rpc/rpc-service.yaml";
    content = ''
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: rpc-service
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: rpc-service
          app: rpc-service
      spec:
        replicas: 1
        selector:
          matchLabels:
            app: rpc-service
        template:
          metadata:
            labels:
              app.kubernetes.io/name: rpc-service
              app: rpc-service
          spec:
            containers:
            - name: rpc-service
              image: ${svc.image}:${svc.tag}
              imagePullPolicy: Never
              args:
              - -grpc-addr=:${s}
              - -validate=true
              ports:
              - containerPort: ${s}
                name: grpc
              readinessProbe:
                tcpSocket:
                  port: ${s}
                initialDelaySeconds: 3
                periodSeconds: 10
              livenessProbe:
                tcpSocket:
                  port: ${s}
                initialDelaySeconds: 10
                periodSeconds: 15
              resources:
                requests:
                  cpu: 100m
                  memory: 64Mi
                limits:
                  cpu: "1"
                  memory: 256Mi
      ---
      apiVersion: v1
      kind: Service
      metadata:
        name: rpc-service
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: rpc-service
      spec:
        type: ClusterIP
        selector:
          app: rpc-service
        ports:
        - name: grpc
          port: ${s}
          targetPort: ${s}
    '';
  };

  gateway = {
    name = "rpc/rpc-gateway.yaml";
    content = ''
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: rpc-gateway
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: rpc-gateway
          app: rpc-gateway
      spec:
        replicas: 1
        selector:
          matchLabels:
            app: rpc-gateway
        template:
          metadata:
            labels:
              app.kubernetes.io/name: rpc-gateway
              app: rpc-gateway
          spec:
            containers:
            - name: rpc-gateway
              image: ${gw.image}:${gw.tag}
              imagePullPolicy: Never
              args:
              - -grpc-addr=:${g}
              - -backend=${serviceTarget}
              ports:
              - containerPort: ${g}
                name: grpc
              readinessProbe:
                tcpSocket:
                  port: ${g}
                initialDelaySeconds: 3
                periodSeconds: 10
              livenessProbe:
                tcpSocket:
                  port: ${g}
                initialDelaySeconds: 10
                periodSeconds: 15
              resources:
                requests:
                  cpu: 100m
                  memory: 64Mi
                limits:
                  cpu: "1"
                  memory: 256Mi
      ---
      apiVersion: v1
      kind: Service
      metadata:
        name: rpc-gateway
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: rpc-gateway
      spec:
        type: NodePort
        selector:
          app: rpc-gateway
        ports:
        - name: grpc
          port: ${g}
          targetPort: ${g}
          nodePort: ${toString gw.nodePort}
    '';
  };

  application = {
    name = "rpc/application.yaml";
    content = ''
      apiVersion: argoproj.io/v1alpha1
      kind: Application
      metadata:
        name: rpc
        namespace: argocd
      spec:
        project: default
        source:
          repoURL: ${constants.gitops.repoURL}
          targetRevision: ${constants.gitops.targetRevision}
          path: ${constants.gitops.renderedPath}/rpc
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
  manifests = [ service gateway application ];
}
