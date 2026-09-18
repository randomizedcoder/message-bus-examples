# nix/gitops/env/argocd.nix
#
# ArgoCD — rendered-manifests pattern.
#
# `helm template` is run against the pinned argo-cd chart tarball at Nix
# build time; the result lands in rendered/argocd/install.yaml. The
# gitops-bootstrap unit on cp0 applies this directly on first boot (so
# ArgoCD can then self-manage via the path-source Application below).
#
{ pkgs, lib, helm }:
let
  constants = import ../../constants.nix;

  valuesYaml = ''
    # ArgoCD Helm values — rendered at Nix build time.
    server:
      service:
        type: NodePort
        nodePortHttps: ${toString constants.argocd.nodePortHttps}
    configs:
      params:
        server.insecure: true
    controller:
      resources:
        requests:
          cpu: 100m
          memory: 256Mi
    repoServer:
      resources:
        requests:
          cpu: 100m
          memory: 256Mi
      startupProbe:
        httpGet:
          path: /healthz
          port: metrics
        failureThreshold: 30
        periodSeconds: 5
      livenessProbe:
        timeoutSeconds: 5
        initialDelaySeconds: 0
        periodSeconds: 30
        failureThreshold: 5
    redis:
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
    # Disable Redis auth init Job — the argocd v3.3.6 image doesn't
    # expose the `argocd` binary needed by the hook Job, and a test
    # cluster doesn't need Redis auth anyway.
    redisSecretInit:
      enabled: false
  '';

  rendered = helm.renderChart {
    name        = "argocd";
    releaseName = "argocd";
    namespace   = "argocd";
    chart       = constants.helmCharts.argocd;
    values      = valuesYaml;
  };
in
{
  manifests = [
    {
      name = "argocd/install.yaml";
      source = "${rendered}/install.yaml";
    }
    {
      name = "argocd/values.yaml";
      content = valuesYaml;
    }
    # Self-managing Application — ArgoCD owns its own install.yaml after
    # the bootstrap unit has done the initial kubectl apply.
    {
      name = "argocd/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: argocd
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/argocd
            directory:
              recurse: false
              exclude: '{application.yaml,values.yaml}'
          destination:
            server: https://kubernetes.default.svc
            namespace: argocd
          syncPolicy:
            automated:
              prune: true
              selfHeal: true
            syncOptions:
              - ServerSideApply=true
              - CreateNamespace=true
      '';
    }
  ];
}
