# nix/gitops/env/storage.nix
#
# Rancher local-path-provisioner — the StorageClass that backs every bus's
# PersistentVolumeClaims (NATS JetStream, RabbitMQ, Mosquitto, ValKey).
#
# The upstream installer YAML is pinned by version+hash in constants and
# fetched at Nix build time, then emitted verbatim as a `source` manifest
# (namespace, ServiceAccount, RBAC, Deployment, `local-path` StorageClass,
# ConfigMap). ArgoCD syncs it like any other component; the buses request
# `storageClassName: local-path` with WaitForFirstConsumer binding, so
# their PVCs bind once the provisioner is up.
#
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
  lpp = constants.localPathProvisioner;
  installer = pkgs.fetchurl {
    url  = lpp.url;
    hash = lpp.hash;
  };
in
{
  manifests = [
    {
      name = "storage/install.yaml";
      source = installer;
    }
    {
      name = "storage/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: storage
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/storage
            directory:
              exclude: 'application.yaml'
          destination:
            server: https://kubernetes.default.svc
          syncPolicy:
            automated:
              prune: true
              selfHeal: true
            syncOptions:
              - CreateNamespace=true
      '';
    }
  ];
}
