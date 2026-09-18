# nix/image-preload-module.nix
#
# NixOS module: preload Nix-built OCI images into containerd at boot.
#
# The message-bus images are built by nix/images/*.nix and live in the
# host /nix/store, which every MicroVM mounts read-only over 9p. A
# systemd oneshot imports each image tarball into containerd's `k8s.io`
# namespace (the namespace the CRI / kubelet uses) before kubelet starts
# scheduling pods. The StatefulSets then reference the images by their
# exact name:tag with imagePullPolicy: Never — no registry, no network.
#
{ config, pkgs, lib, ... }:
with lib;
let
  cfg = config.services.k8s-image-preload;
in
{
  options.services.k8s-image-preload = {
    enable = mkEnableOption "preload Nix-built OCI images into containerd";

    images = mkOption {
      type = types.listOf (types.attrsOf types.anything);
      default = [ ];
      description = ''
        Images to import into containerd's k8s.io namespace. Each entry is
        an attrset { name; tag; archive; } where `archive` is a
        docker-archive tarball derivation (from dockerTools.buildLayeredImage).
      '';
    };
  };

  config = mkIf (cfg.enable && cfg.images != [ ]) {
    systemd.services.k8s-image-preload = {
      description = "Preload Nix-built OCI images into containerd (k8s.io namespace)";

      # containerd must be running to import; kubelet should wait for us
      # so pods never race ahead of their (Never-pull) images. A failure
      # here does not hard-block kubelet — pods just ImagePullBackOff and
      # recover once the retrying oneshot succeeds.
      after = [ "containerd.service" ];
      requires = [ "containerd.service" ];
      before = [ "kubelet.service" ];
      wantedBy = [ "multi-user.target" ];

      path = [ pkgs.containerd ];

      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        TimeoutStartSec = "10min";
        Restart = "on-failure";
        RestartSec = "10s";
      };

      script = ''
        set -eu
        log() { echo "[image-preload] $*"; }

        # Wait for the containerd socket (containerd.service may report
        # active slightly before the socket is accepting connections).
        for i in $(seq 1 60); do
          if ctr version >/dev/null 2>&1; then break; fi
          sleep 1
        done

        ${concatMapStringsSep "\n" (img: ''
          log "importing ${img.name}:${img.tag}"
          ctr -n k8s.io images import "${img.archive}"
        '') cfg.images}

        log "preload complete (${toString (length cfg.images)} images)"
      '';
    };
  };
}
