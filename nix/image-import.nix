# nix/image-import.nix
#
# k8s-image-import — import the Nix-built OCI images into a *running* cluster's
# containerd (the `k8s.io` namespace), on every node (or one).
#
# The boot-time preload module (nix/image-preload-module.nix) only imports
# images when a VM boots. When an image is added or changed on an already-
# running cluster — e.g. a new exporter sidecar image — the running nodes don't
# have it yet (imagePullPolicy: Never, no registry), so pods ImagePullBackOff.
# This app closes that gap: it SSHes to each node (via the hardened k8s-vm-ssh
# wrapper — publickey-only, no agent, no password/askpass) and runs
# `ctr -n k8s.io images import <archive>` for each image. The archives live on
# the 9p-shared /nix/store, so no copy is needed. Importing an image that is
# already present is a cheap no-op, so the whole thing is idempotent.
#
#   nix run .#k8s-image-import                              # all images, all nodes
#   nix run .#k8s-image-import -- --node=cp1                # all images, one node
#   nix run .#k8s-image-import -- --image=prometheus-redis-exporter
#   nix run .#k8s-image-import -- --node=w3 --image=grafana
#
{ pkgs }:
let
  lib = pkgs.lib;
  constants = import ./constants.nix;
  nodes = import ./nodes.nix { inherit constants; };
  vmScripts = import ./microvm-scripts.nix { inherit pkgs; };
  busImagesMod = import ./images { inherit pkgs lib; };
  imageList = busImagesMod.imageList;

  nodeNames = builtins.attrNames nodes.definitions;   # cp0 cp1 cp2 w3
  shortName = name: lib.last (lib.splitString "/" name);
in
{
  imageImport = pkgs.writeShellApplication {
    name = "k8s-image-import";
    runtimeInputs = [ vmScripts.ssh pkgs.coreutils ];
    text = ''
      # Not -e: a failed import on one node/image should warn, not abort the run.
      set -uo pipefail

      NODES=(${builtins.concatStringsSep " " nodeNames})
      ONLY_NODE=""
      ONLY_IMAGE=""

      usage() {
        cat <<EOF
Usage: k8s-image-import [--node=NODE] [--image=NAME]

Imports the Nix-built OCI images into the running cluster's containerd
(k8s.io namespace) via SSH. Idempotent (re-importing is a no-op).

Options:
  --node=NODE    Only this node (default: all — ${builtins.concatStringsSep " " nodeNames})
  --image=NAME   Only this image, by short name (e.g. prometheus-redis-exporter,
                 grafana, nats, valkey) or full messagebus.local/<name>
  -h, --help     Show this help
EOF
      }

      while [[ $# -gt 0 ]]; do
        case "$1" in
          --node=*)  ONLY_NODE="''${1#*=}" ;;
          --node)    ONLY_NODE="''${2-}"; shift ;;
          --image=*) ONLY_IMAGE="''${1#*=}" ;;
          --image)   ONLY_IMAGE="''${2-}"; shift ;;
          -h|--help) usage; exit 0 ;;
          *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
        esac
        shift
      done

      [ -n "$ONLY_NODE" ] && NODES=("$ONLY_NODE")

      rc=0
      for node in "''${NODES[@]}"; do
        echo "=== $node ==="
        ${lib.concatMapStringsSep "\n" (img: ''
          if [ -z "$ONLY_IMAGE" ] || [ "$ONLY_IMAGE" = "${img.name}" ] || [ "$ONLY_IMAGE" = "${shortName img.name}" ]; then
            echo "  import ${img.name}:${img.tag}"
            if ! k8s-vm-ssh --node="$node" ctr -n k8s.io images import "${img.archive}"; then
              echo "  WARN: import failed: ${img.name} on $node" >&2
              rc=1
            fi
          fi
        '') imageList}
      done
      exit "$rc"
    '';
  };
}
