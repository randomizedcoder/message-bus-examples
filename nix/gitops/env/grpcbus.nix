# nix/gitops/env/grpcbus.nix
#
# gRPC-native message bus in-cluster deployment: a single-pod grpcbrokerd
# Deployment in the `grpcbus` namespace, exposed on a NodePort so the host
# grpcbuscli pub/sub clients reach it (host → NodePort → broker), exactly like
# the other buses' NodePorts. The broker fans each published Message out to
# every live subscriber of its topic over gRPC server-streaming.
#
# The image is a Nix-built Go binary preloaded into containerd
# (imagePullPolicy: Never). Probes are tcpSocket on the gRPC port — the binary
# serves gRPC only, with no HTTP health endpoint. The broker also serves an OTel
# /metrics endpoint (grpcbus_*) on metricsPort for a later scrape PR.
#
# An ArgoCD Application (registered in nix/gitops/default.nix) syncs
# rendered/grpcbus. The namespace itself is declared in base.nix.
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  bus = constants.messageBus.grpcbus;
  ns = bus.namespace;
  g = toString bus.grpcPort;
  m = toString bus.metricsPort;

  broker = {
    name = "grpcbus/grpc-broker.yaml";
    content = ''
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: grpc-broker
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: grpc-broker
          app: grpc-broker
      spec:
        replicas: 1
        selector:
          matchLabels:
            app: grpc-broker
        template:
          metadata:
            labels:
              app.kubernetes.io/name: grpc-broker
              app: grpc-broker
          spec:
            containers:
            - name: grpc-broker
              image: ${bus.image}:${bus.tag}
              imagePullPolicy: Never
              args:
              - -grpc-addr=:${g}
              - -metrics-addr=:${m}
              - -validate=true
              ports:
              - containerPort: ${g}
                name: grpc
              - containerPort: ${m}
                name: metrics
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
        name: grpc-broker
        namespace: ${ns}
        labels:
          app.kubernetes.io/name: grpc-broker
      spec:
        type: NodePort
        selector:
          app: grpc-broker
        ports:
        - name: grpc
          port: ${g}
          targetPort: ${g}
          nodePort: ${toString bus.nodePort}
    '';
  };

  application = {
    name = "grpcbus/application.yaml";
    content = ''
      apiVersion: argoproj.io/v1alpha1
      kind: Application
      metadata:
        name: grpcbus
        namespace: argocd
      spec:
        project: default
        source:
          repoURL: ${constants.gitops.repoURL}
          targetRevision: ${constants.gitops.targetRevision}
          path: ${constants.gitops.renderedPath}/grpcbus
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
  manifests = [ broker application ];
}
