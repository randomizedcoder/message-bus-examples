# nix/chaos-scripts.nix
#
# Chaos / failover verification tool for the message buses.
#
# Each round kills one MicroVM, then measures — per bus — how long a
# host-side subscriber takes to start receiving freshly-published
# messages again (i.e. how long the clustered bus takes to reroute around
# the lost node). The node is brought back and the loop repeats.
#
# Clients connect to a STABLE node's NodePort (default cp0, which is never
# killed) so the measurement isolates in-cluster failover, not host↔node
# reachability. NodePort traffic is rerouted by kube-proxy/Cilium to a
# surviving pod.
#
# See README "Chaos / Failover Test" for usage.
#
{ pkgs }:
let
  constants = import ./constants.nix;
  vmScripts = import ./microvm-scripts.nix { inherit pkgs; };
  clients   = import ./clients.nix { inherit pkgs; };
  mb = constants.messageBus;
in
{
  chaosFailover = pkgs.writeShellApplication {
    name = "k8s-chaos-failover";
    runtimeInputs = with pkgs; [
      coreutils gawk gnused procps
      vmScripts.stopOne vmScripts.startOne vmScripts.ssh
      clients.package
    ];
    text = ''
      set -uo pipefail

      # ─── Defaults (from constants.nix) ───────────────────────────────
      ROUNDS=${toString constants.chaos.defaultRounds}
      INTERVAL=${toString constants.chaos.defaultIntervalSec}
      POST_ROUND_WAIT=${toString constants.chaos.defaultPostRoundWait}
      LOG_DIR="${constants.chaos.defaultLogDir}"
      NODES="cp1,cp2,w3"          # nodes eligible to be killed
      CLIENT_NODE_IP="${constants.network.ipv4.cp0}"   # stable client endpoint
      BUSES="nats,mqtt,valkey,rabbitmq"
      RECOVERY_TIMEOUT=120

      NATS_PORT="${toString mb.nats.nodePort}"
      MQTT_PORT="${toString mb.mqtt.nodePort}"
      VALKEY_PORT="${toString mb.valkey.nodePort}"
      RABBITMQ_PORT="${toString mb.rabbitmq.nodePortAmqp}"

      usage() {
        cat <<EOF
Usage: k8s-chaos-failover [OPTIONS]

Kills one MicroVM at a time and measures per-bus message-flow recovery.

Options:
  --rounds=N         Number of rounds (default: $ROUNDS)
  --interval=SEC     Seconds the node stays down (default: $INTERVAL)
  --post-round-wait=SEC  Seconds after node rejoins (default: $POST_ROUND_WAIT)
  --nodes=LIST       Comma-separated kill rotation (default: $NODES)
  --buses=LIST       Comma-separated buses (default: $BUSES)
  --log-dir=DIR      Output directory (default: $LOG_DIR)
  -h, --help         Show this help

Requires: cluster running; SSH to cp0 for reading bus credentials.
EOF
      }

      while [[ $# -gt 0 ]]; do
        case "$1" in
          --rounds=*)          ROUNDS="''${1#*=}" ;;
          --interval=*)        INTERVAL="''${1#*=}" ;;
          --post-round-wait=*) POST_ROUND_WAIT="''${1#*=}" ;;
          --nodes=*)           NODES="''${1#*=}" ;;
          --buses=*)           BUSES="''${1#*=}" ;;
          --log-dir=*)         LOG_DIR="''${1#*=}" ;;
          -h|--help)           usage; exit 0 ;;
          *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
        esac
        shift
      done

      mkdir -p "$LOG_DIR"
      SUMMARY="$LOG_DIR/summary.tsv"
      echo -e "round\tnode\tbus\trecovery_sec" > "$SUMMARY"

      # ─── Fetch bus credentials from the cluster (via cp0) ────────────
      log() { echo "[chaos] $*"; }
      # Run a command on cp0 with KUBECONFIG set. Non-interactive SSH does
      # not source the profile, so kubectl would otherwise default to
      # localhost:8080 and fail — silently corrupting credential fetches.
      kexec() { k8s-vm-ssh --node=cp0 env KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig "$@"; }

      log "reading bus credentials from cluster"
      RABBITMQ_PASS="$(kexec kubectl -n ${mb.rabbitmq.namespace} get secret rabbitmq-credentials \
        -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' 2>/dev/null | base64 -d || true)"
      VALKEY_PASS="$(kexec kubectl -n ${mb.valkey.namespace} get secret valkey-credentials \
        -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
      export RABBITMQ_PASS VALKEY_PASS

      # bus → "binary port [extra-flags]"
      bus_addr() {
        case "$1" in
          nats)     echo "$CLIENT_NODE_IP:$NATS_PORT" ;;
          mqtt)     echo "$CLIENT_NODE_IP:$MQTT_PORT" ;;
          valkey)   echo "$CLIENT_NODE_IP:$VALKEY_PORT" ;;
          rabbitmq) echo "$CLIENT_NODE_IP:$RABBITMQ_PORT" ;;
        esac
      }
      bus_bin() {
        case "$1" in
          nats) echo natscli ;; mqtt) echo mqttcli ;;
          valkey) echo valkeycli ;; rabbitmq) echo rabbitmqcli ;;
        esac
      }
      # subscriber for a bus, writing received lines to $2
      sub_start() {
        local bus="$1" out="$2" addr; addr="$(bus_addr "$bus")"
        : > "$out"
        "$(bus_bin "$bus")" sub -addr "$addr" -subject demo/chaos >> "$out" 2>&1 &
        echo $!
      }
      pub_once() {
        local bus="$1" msg="$2" addr; addr="$(bus_addr "$bus")"
        "$(bus_bin "$bus")" pub -addr "$addr" -subject demo/chaos -msg "$msg" >/dev/null 2>&1 || true
      }

      IFS=',' read -ra NODE_ARR <<< "$NODES"
      IFS=',' read -ra BUS_ARR  <<< "$BUSES"

      log "starting: rounds=$ROUNDS interval=$INTERVAL nodes=$NODES buses=$BUSES"

      for ((r = 1; r <= ROUNDS; r++)); do
        node="''${NODE_ARR[$(( (r - 1) % ''${#NODE_ARR[@]} ))]}"
        log "── round $r/$ROUNDS — victim: $node ──"

        declare -A PIDS
        for bus in "''${BUS_ARR[@]}"; do
          PIDS[$bus]="$(sub_start "$bus" "$LOG_DIR/$bus.sub")"
          pub_once "$bus" "warmup-r$r"     # prime connection
        done
        sleep 3

        log "killing $node"
        k8s-vm-stop-one --node="$node" || log "WARN: stop-one failed"
        kill_ts=$(date +%s)

        # For each bus, publish a unique probe until the subscriber sees it.
        for bus in "''${BUS_ARR[@]}"; do
          probe="probe-r$r-$bus-$kill_ts"
          recovered=""
          for ((t = 0; t < RECOVERY_TIMEOUT; t++)); do
            pub_once "$bus" "$probe"
            if grep -q "$probe" "$LOG_DIR/$bus.sub" 2>/dev/null; then
              recovered=$(( $(date +%s) - kill_ts ))
              break
            fi
            sleep 1
          done
          recovered="''${recovered:-TIMEOUT}"
          log "  $bus recovery: ''${recovered}s"
          echo -e "$r\t$node\t$bus\t$recovered" >> "$SUMMARY"
        done

        sleep "$INTERVAL"

        log "restarting $node"
        k8s-vm-start-one --node="$node" || log "WARN: start-one failed"

        for bus in "''${BUS_ARR[@]}"; do
          kill "''${PIDS[$bus]}" 2>/dev/null || true
        done
        unset PIDS

        log "post-round settle ($POST_ROUND_WAIT s)"
        sleep "$POST_ROUND_WAIT"
      done

      log "done — summary:"
      cat "$SUMMARY"
    '';
  };
}
