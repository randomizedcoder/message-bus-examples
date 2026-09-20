# nix/gitops/env/monitoring/application.nix
#
# The ArgoCD Application that syncs rendered/monitoring/. Split out of the old
# monolithic monitoring.nix with no behaviour change.
{ constants, ns }:
{
  name = "monitoring/application.yaml";
  content = ''
    apiVersion: argoproj.io/v1alpha1
    kind: Application
    metadata:
      name: monitoring
      namespace: argocd
    spec:
      project: default
      source:
        repoURL: ${constants.gitops.repoURL}
        targetRevision: ${constants.gitops.targetRevision}
        path: ${constants.gitops.renderedPath}/monitoring
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
