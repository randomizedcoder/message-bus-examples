# nix/microvm-scripts.nix
#
# Helper scripts for managing K8s MicroVMs.
#
{ pkgs }:
let
  constants = import ./constants.nix;

  # Pattern to identify our K8s MicroVMs by process name.
  # QEMU's -name flag sets the process name, so use pgrep -x (exact match)
  # to avoid false positives from pgrep matching its own cmdline.
  vmPattern = "k8s-(cp0|cp1|cp2|w3)";
in
{
  check = pkgs.writeShellApplication {
    name = "k8s-vm-check";
    runtimeInputs = with pkgs; [ procps ];
    text = ''
      echo "=== K8s MicroVM Processes ==="
      echo

      if pgrep -ax '${vmPattern}'; then
        echo
        echo "=== Count ==="
        pgrep -cx '${vmPattern}'
      else
        echo "(none running)"
        echo
        echo "=== Count ==="
        echo "0"
      fi
    '';
  };

  stopOne = pkgs.writeShellApplication {
    name = "k8s-vm-stop-one";
    runtimeInputs = with pkgs; [ procps ];
    text = ''
      NODE=""
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --node=*) NODE="''${1#--node=}"; shift ;;
          *) echo "Unknown arg: $1" >&2; exit 2 ;;
        esac
      done

      case "$NODE" in
        cp0|cp1|cp2|w3) ;;
        *) echo "Usage: k8s-vm-stop-one --node=cp0|cp1|cp2|w3" >&2; exit 2 ;;
      esac

      PROC="k8s-$NODE"

      if ! pgrep -x "$PROC" > /dev/null; then
        echo "  $PROC not running, nothing to do"
        exit 0
      fi

      echo "  Sending SIGTERM to $PROC..."
      pkill -x "$PROC" || true
      sleep 2

      if pgrep -x "$PROC" > /dev/null; then
        echo "  Still alive, sending SIGKILL..."
        pkill -9 -x "$PROC" || true
        sleep 1
      fi

      if pgrep -x "$PROC" > /dev/null; then
        echo "  FAILED: $PROC still running" >&2
        exit 1
      fi

      echo "  $PROC stopped."
    '';
  };

  startOne = pkgs.writeShellApplication {
    name = "k8s-vm-start-one";
    runtimeInputs = with pkgs; [ procps nix coreutils ];
    text = ''
      NODE=""
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --node=*) NODE="''${1#--node=}"; shift ;;
          *) echo "Unknown arg: $1" >&2; exit 2 ;;
        esac
      done

      case "$NODE" in
        cp0|cp1|cp2|w3) ;;
        *) echo "Usage: k8s-vm-start-one --node=cp0|cp1|cp2|w3" >&2; exit 2 ;;
      esac

      PROC="k8s-$NODE"
      RESULT_LINK="result-k8s-$NODE-oneshot"

      if pgrep -x "$PROC" > /dev/null; then
        echo "  $PROC already running, nothing to do"
        exit 0
      fi

      rm -f "$RESULT_LINK"
      nix build ".#k8s-microvm-$NODE" -o "$RESULT_LINK"
      "$RESULT_LINK/bin/microvm-run" </dev/null >/dev/null 2>&1 &
      disown || true
      echo "  $PROC started (PID $!)"
    '';
  };

  stop = pkgs.writeShellApplication {
    name = "k8s-vm-stop";
    runtimeInputs = with pkgs; [ procps ];
    text = ''
      echo "=== Stopping K8s MicroVMs ==="

      if ! pgrep -x '${vmPattern}' > /dev/null; then
        echo "No K8s MicroVMs running."
        exit 0
      fi

      echo "Found processes:"
      pgrep -ax '${vmPattern}'

      echo
      echo "Sending SIGTERM..."
      pkill -x '${vmPattern}' || true

      sleep 2

      if pgrep -x '${vmPattern}' > /dev/null; then
        echo "Processes still running, sending SIGKILL..."
        pkill -9 -x '${vmPattern}' || true
      fi

      echo "Done."
    '';
  };

  ssh = pkgs.writeShellApplication {
    name = "k8s-vm-ssh";
    runtimeInputs = with pkgs; [ openssh sshpass ];
    text = ''
      unset SSH_AUTH_SOCK

      NODE="cp0"
      PASSTHROUGH_ARGS=()

      while [[ $# -gt 0 ]]; do
        case "$1" in
          --node=*)
            NODE="''${1#--node=}"
            shift
            ;;
          *)
            PASSTHROUGH_ARGS+=("$1")
            shift
            ;;
        esac
      done

      case "$NODE" in
        cp0) HOST="${constants.network.ipv4.cp0}" ;;
        cp1) HOST="${constants.network.ipv4.cp1}" ;;
        cp2) HOST="${constants.network.ipv4.cp2}" ;;
        w3)  HOST="${constants.network.ipv4.w3}" ;;
        *)
          echo "Unknown node: $NODE"
          echo "Valid nodes: cp0, cp1, cp2, w3"
          exit 1
          ;;
      esac

      SSH_KEY="./secrets/ssh-ed25519"
      if [ -f "$SSH_KEY" ]; then
        exec ssh \
          -o StrictHostKeyChecking=no \
          -o UserKnownHostsFile=/dev/null \
          -o IdentityFile="$SSH_KEY" \
          -o IdentitiesOnly=yes \
          -o LogLevel=ERROR \
          "root@$HOST" "''${PASSTHROUGH_ARGS[@]}"
      else
        exec sshpass -p ${constants.ssh.password} ssh \
          -o StrictHostKeyChecking=no \
          -o UserKnownHostsFile=/dev/null \
          -o LogLevel=ERROR \
          "root@$HOST" "''${PASSTHROUGH_ARGS[@]}"
      fi
    '';
  };

  wipe = pkgs.writeShellApplication {
    name = "k8s-vm-wipe";
    runtimeInputs = with pkgs; [ procps coreutils ];
    text = ''
      echo "=== Stopping K8s MicroVMs ==="
      if pgrep -x '${vmPattern}' > /dev/null; then
        pkill -x '${vmPattern}' || true
        sleep 2
        if pgrep -x '${vmPattern}' > /dev/null; then
          pkill -9 -x '${vmPattern}' || true
          sleep 1
        fi
      fi

      echo ""
      echo "=== Removing per-VM data images (etcd / containerd / kubelet state) ==="
      REMOVED=0
      for node in ${builtins.concatStringsSep " " constants.nodeNames}; do
        img="k8s-$node-data.img"
        if [ -f "$img" ]; then
          rm -f "$img"
          echo "  removed $img"
          REMOVED=$((REMOVED + 1))
        fi
      done

      echo ""
      if [ "$REMOVED" -eq 0 ]; then
        echo "No data images found. Nothing to wipe."
      else
        echo "Wiped $REMOVED data image(s)."
      fi
    '';
  };

  clusterRebuild = pkgs.writeShellApplication {
    name = "k8s-cluster-rebuild";
    runtimeInputs = with pkgs; [ nix coreutils ];
    text = ''
      echo "=== Wipe ==="
      nix run .#k8s-vm-wipe

      echo ""
      echo "=== Start ==="
      nix run .#k8s-start-all

      echo ""
      echo "Bootstrap runs on cp0 in the background."
      echo "Watch progress with:"
      echo "  nix run .#k8s-vm-ssh -- --node=cp0 journalctl -fu k8s-gitops-bootstrap"
    '';
  };

  startAll = pkgs.writeShellApplication {
    name = "k8s-start-all";
    runtimeInputs = with pkgs; [ procps nix coreutils ];
    text = ''
      echo "=== Starting K8s Cluster (3 CP + 1 Worker) ==="

      # Start control planes first, then worker
      ${builtins.concatStringsSep "\n" (builtins.map (n: let
        hostname = constants.getHostname n;
      in ''
      echo ""
      echo "--- Starting ${n} ---"
      RESULT_LINK="result-k8s-${n}"
      rm -f "$RESULT_LINK"

      if pgrep -f "process=${hostname}" > /dev/null 2>&1; then
        echo "  ${n} already running, skipping"
      else
        nix build ".#k8s-microvm-${n}" -o "$RESULT_LINK"
        "$RESULT_LINK/bin/microvm-run" &
        echo "  ${n} started (PID: $!)"
        sleep 2
      fi
      '') constants.nodeNames)}

      echo ""
      echo "=== All nodes started ==="
      echo "Verify: nix run .#k8s-vm-ssh -- --node=cp0 kubectl get nodes"
    '';
  };
}
