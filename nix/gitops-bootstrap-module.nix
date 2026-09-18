# nix/gitops-bootstrap-module.nix
#
# NixOS module: first-boot GitOps bootstrap on cp0.
#
# On the first boot where /var/lib/k8s-bootstrap/done does not exist, a
# systemd oneshot applies, in order:
#   1.  rendered/cilium/install.yaml  (CNI must be up before anything else)
#   1b. rendered/base/ (namespaces, RBAC, CoreDNS)
#   2.  rendered/argocd/install.yaml  (ArgoCD controller + server + CRDs)
#   2b. Pre-generated Secrets (if secretsPath is set) — bus credentials
#   3.  rendered/*/application*.yaml  (all Application CRs — ArgoCD takes over)
#
# The rendered manifests are injected as a Nix-store path via
# `services.k8s-gitops-bootstrap.manifestsPath`, so no git fetch is needed
# from inside the VM (CNI isn't up yet — pod egress wouldn't work either).
# After day 1, ArgoCD is source of truth via the git repoURL in each
# Application CR.
#
{ config, pkgs, lib, ... }:
with lib;
let
  constants = import ./constants.nix;
  cfg = config.services.k8s-gitops-bootstrap;
  kubeconfig = "${constants.k8s.pkiDir}/admin-kubeconfig";
  markerDir = "/var/lib/k8s-bootstrap";
  markerFile = "${markerDir}/done";
in
{
  options.services.k8s-gitops-bootstrap = {
    enable = mkEnableOption "K8s first-boot GitOps bootstrap (cp0 only)";

    manifestsPath = mkOption {
      type = types.path;
      description = ''
        Path to the rendered manifests directory (Nix-store path). Must contain
        cilium/install.yaml, argocd/install.yaml, and */application*.yaml.
      '';
    };

    secretsPath = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = ''
        Path to pre-generated K8s Secret manifests (JSON files from
        nix/secrets.nix). Applied between ArgoCD install and Application
        CRs so Secrets exist before workloads reference them. Set to null
        to skip (Secrets will use __BOOTSTRAPPED_OUT_OF_BAND__ placeholders).
      '';
    };
  };

  config = mkIf cfg.enable {
    systemd.services.k8s-gitops-bootstrap = {
      description = "First-boot GitOps bootstrap: Cilium -> ArgoCD -> Secrets -> Applications";

      # Needs a functioning apiserver before it can kubectl apply. On cp0
      # the apiserver is local, so after kubelet.service is up enough to
      # start the static-pod apiserver, we can hit localhost:6443.
      wants = [ "kubelet.service" "network-online.target" ];
      after = [ "kubelet.service" "network-online.target" ];
      wantedBy = [ "multi-user.target" ];

      # Idempotent — once done, the marker file short-circuits re-runs.
      unitConfig = {
        ConditionPathExists = "!${markerFile}";
      };

      path = with pkgs; [ kubectl curl coreutils findutils gnugrep gnused jq ];

      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        # Be forgiving — if the apiserver is still warming up when we start,
        # the inner retries will catch it, but if something is truly broken
        # we want the unit to keep retrying on reboot rather than blocking it.
        TimeoutStartSec = "15min";
        # Restart on failure so a transient apiserver hiccup recovers.
        Restart = "on-failure";
        RestartSec = "15s";
      };

      script = ''
        set -eu
        export KUBECONFIG=${kubeconfig}

        log() { echo "[bootstrap] $*"; }

        wait_for_apiserver() {
          log "waiting for apiserver (https://localhost:6443/livez)"
          for i in $(seq 1 120); do
            if curl -sk --max-time 3 https://localhost:6443/livez >/dev/null 2>&1; then
              log "  apiserver is live"
              return 0
            fi
            sleep 2
          done
          log "ERROR: apiserver not live after 240s"
          return 1
        }

        apply_file() {
          local f="$1"
          log "kubectl apply --server-side -f $f"
          # --force-conflicts: we own all these fields on first install.
          kubectl apply --server-side --force-conflicts -f "$f"
        }

        mkdir -p ${markerDir}

        wait_for_apiserver

        # ── 1. Cilium ──────────────────────────────────────────────────
        if [ -f ${cfg.manifestsPath}/cilium/install.yaml ]; then
          apply_file ${cfg.manifestsPath}/cilium/install.yaml
          log "waiting for cilium DaemonSet"
          kubectl -n kube-system rollout status ds/cilium --timeout=300s || {
            log "WARN: cilium rollout not complete; continuing"
          }
        fi

        # ── 1b. Base manifests (namespaces, RBAC, CoreDNS) ─────────────
        if [ -f ${cfg.manifestsPath}/base/namespaces.yaml ]; then
          apply_file ${cfg.manifestsPath}/base/namespaces.yaml
        fi
        if [ -f ${cfg.manifestsPath}/base/rbac.yaml ]; then
          apply_file ${cfg.manifestsPath}/base/rbac.yaml
        fi
        if [ -f ${cfg.manifestsPath}/base/coredns.yaml ]; then
          apply_file ${cfg.manifestsPath}/base/coredns.yaml
          log "waiting for CoreDNS"
          kubectl -n kube-system rollout status deploy/coredns --timeout=120s || {
            log "WARN: CoreDNS rollout not complete; continuing"
          }
        fi

        # ── 2. ArgoCD ──────────────────────────────────────────────────
        if [ -f ${cfg.manifestsPath}/argocd/install.yaml ]; then

          # The argo-cd chart expects an argocd-redis Secret for Redis auth.
          # The chart's redis-secret-init Job is disabled (broken in v3.3.6
          # image), so we create the Secret ourselves before applying the
          # chart manifests.
          if ! kubectl -n argocd get secret argocd-redis >/dev/null 2>&1; then
            REDIS_PASS=$(head -c 32 /dev/urandom | base64 | tr -d '/+=' | head -c 32)
            log "creating argocd-redis Secret"
            kubectl -n argocd create secret generic argocd-redis \
              --from-literal=auth="$REDIS_PASS"
          fi

          apply_file ${cfg.manifestsPath}/argocd/install.yaml
          log "waiting for Application CRD"
          kubectl wait --for=condition=Established \
            crd/applications.argoproj.io --timeout=180s || true
          log "waiting for argocd-server"
          kubectl -n argocd rollout status deploy/argocd-server --timeout=300s || {
            log "WARN: argocd-server rollout not complete; continuing"
          }
        fi

        # ── 2b. Pre-generated Secrets ──────────────────────────────────
        # If nix/secrets.nix produced a secretsPath, apply all Secret
        # manifests now — before Application CRs, so workloads find
        # their Secrets already populated on first rollout.
        ${optionalString (cfg.secretsPath != null) ''
          if [ -d "${cfg.secretsPath}" ]; then
            log "applying pre-generated secrets from ${cfg.secretsPath}"
            find "${cfg.secretsPath}" -name '*.json' -print0 |
              while IFS= read -r -d "" f; do
                apply_file "$f"
              done
          fi
        ''}

        # ── 3. Application CRs (ArgoCD takes over from here) ──────────
        log "applying Application CRs"
        find ${cfg.manifestsPath} -maxdepth 2 -name 'application*.yaml' -print0 |
          while IFS= read -r -d "" f; do
            apply_file "$f"
          done

        touch ${markerFile}
        log "bootstrap complete — marker: ${markerFile}"
      '';
    };
  };
}
