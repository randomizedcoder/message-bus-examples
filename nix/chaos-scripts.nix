# nix/chaos-scripts.nix
#
# Chaos / failover verification tool for the message buses.
#
# Each round kills one MicroVM, then measures — per bus — how the host-side
# clients fare while a node is down. Unlike a single-endpoint probe, this runs
# CLIENTS producer+consumer pairs per bus, each pinned to a different node's
# NodePort (round-robin over --client-nodes, default cp0,cp1,cp2,w3). Because
# the clients are SPREAD across the nodes, every round some of them are attached
# directly to the victim node: when it dies their NodePort ingress vanishes, so
# they drop; the pairs on surviving nodes ride through via in-cluster failover.
# This demonstrates the clients being distributed across the server nodes and
# the mixed fate of "on the dead node" vs "on a live node".
#
# Two things are measured per client:
#   * survivors  — time from the KILL until the client next receives a message
#                  (in-cluster reroute time); ~seconds if it rides through.
#   * direct-hit — time from the node RESTART until the client resumes
#                  (clients whose transport auto-reconnects come back; the
#                  RabbitMQ CLI and the gRPC-bus subscriber do not, and show
#                  TIMEOUT — an honest result, not a harness failure).
#
# cp0 is never killed: it hosts the kubectl/credentials path (kexec) and is the
# survivor baseline. A client pinned to cp0 always rides through.
#
# grpcbus is included but is a SINGLE-POD broker (no HA): if the broker pod's
# node is killed, every grpcbus client is affected until the pod reschedules,
# not just the one pinned to the victim — that is the no-HA story, by design.
#
# See README "Chaos / Failover Test" for usage.
#
{ pkgs }:
let
  constants = import ./constants.nix;
  vmScripts = import ./microvm-scripts.nix { inherit pkgs; };
  clients   = import ./clients.nix { inherit pkgs; };
  mb = constants.messageBus;
  ip = constants.network.ipv4;
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
      WARMUP=${toString constants.chaos.defaultWarmupSec}
      LOG_DIR="${constants.chaos.defaultLogDir}"
      NODES="cp1,cp2,w3"          # nodes eligible to be killed (cp0 spared)
      CLIENT_NODES="${constants.chaos.defaultClientNodes}"  # nodes clients pin to
      CLIENTS=${toString constants.chaos.defaultClients}    # producer+consumer pairs per bus
      BUSES="nats,mqtt,valkey,rabbitmq,grpcbus"
      REROUTE_TIMEOUT=60          # survivor ride-through budget (from kill)
      RECOVERY_TIMEOUT=120        # direct-hit recovery budget (from restart)

      NATS_PORT="${toString mb.nats.nodePort}"
      MQTT_PORT="${toString mb.mqtt.nodePort}"
      VALKEY_PORT="${toString mb.valkey.nodePort}"
      RABBITMQ_PORT="${toString mb.rabbitmq.nodePortAmqp}"
      GRPCBUS_PORT="${toString mb.grpcbus.nodePort}"

      usage() {
        cat <<EOF
Usage: k8s-chaos-failover [OPTIONS]

Kills one MicroVM at a time and measures per-bus, per-client failover. Runs
CLIENTS producer+consumer pairs per bus, spread across the client nodes, so
each round some clients sit directly on the victim node.

Options:
  --rounds=N            Number of rounds (default: $ROUNDS)
  --interval=SEC        Seconds the node stays down (default: $INTERVAL)
  --post-round-wait=SEC Seconds after node rejoins (default: $POST_ROUND_WAIT)
  --warmup=SEC          Warmup before the kill (default: $WARMUP)
  --clients=N           Producer+consumer pairs per bus (default: $CLIENTS)
  --client-nodes=LIST   Nodes clients pin to, round-robin (default: $CLIENT_NODES)
  --nodes=LIST          Comma-separated kill rotation (default: $NODES)
  --buses=LIST          Comma-separated buses (default: $BUSES)
  --log-dir=DIR         Output directory (default: $LOG_DIR)
  -h, --help            Show this help

Requires: cluster running; SSH to cp0 for reading bus credentials.
EOF
      }

      while [[ $# -gt 0 ]]; do
        case "$1" in
          --rounds=*)          ROUNDS="''${1#*=}" ;;
          --interval=*)        INTERVAL="''${1#*=}" ;;
          --post-round-wait=*) POST_ROUND_WAIT="''${1#*=}" ;;
          --warmup=*)          WARMUP="''${1#*=}" ;;
          --clients=*)         CLIENTS="''${1#*=}" ;;
          --client-nodes=*)    CLIENT_NODES="''${1#*=}" ;;
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
      echo -e "round\tvictim\tbus\tclient_node\trole\trecovered_from\trecovery_sec\tmsgs" > "$SUMMARY"

      log() { echo "[chaos] $*"; }
      # Run a command on cp0 with KUBECONFIG set. Non-interactive SSH does not
      # source the profile, so kubectl would otherwise default to localhost:8080.
      kexec() { k8s-vm-ssh --node=cp0 env KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig "$@"; }

      log "reading bus credentials from cluster"
      RABBITMQ_PASS="$(kexec kubectl -n ${mb.rabbitmq.namespace} get secret rabbitmq-credentials \
        -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' 2>/dev/null | base64 -d || true)"
      VALKEY_PASS="$(kexec kubectl -n ${mb.valkey.namespace} get secret valkey-credentials \
        -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
      export RABBITMQ_PASS VALKEY_PASS

      node_ip() {
        case "$1" in
          cp0) echo "${ip.cp0}" ;;
          cp1) echo "${ip.cp1}" ;;
          cp2) echo "${ip.cp2}" ;;
          w3)  echo "${ip.w3}" ;;
          *)   echo "" ;;
        esac
      }
      bus_port() {
        case "$1" in
          nats)     echo "$NATS_PORT" ;;
          mqtt)     echo "$MQTT_PORT" ;;
          valkey)   echo "$VALKEY_PORT" ;;
          rabbitmq) echo "$RABBITMQ_PORT" ;;
          grpcbus)  echo "$GRPCBUS_PORT" ;;
        esac
      }
      bus_bin() {
        case "$1" in
          nats) echo natscli ;; mqtt) echo mqttcli ;;
          valkey) echo valkeycli ;; rabbitmq) echo rabbitmqcli ;;
          grpcbus) echo grpcbuscli ;;
        esac
      }
      # (bus, node) -> host:port
      bus_addr() { echo "$(node_ip "$2"):$(bus_port "$1")"; }
      # lines currently in a subscriber's output file, as an integer
      cnt() { local n; n=$(wc -l < "$1" 2>/dev/null || echo 0); echo "$(( n ))"; }

      # Wait until each of the given keys' subscriber files grow past their
      # PRECNT baseline, recording RECOV[key] = seconds since $base_ts. Only
      # keys whose ROLE matches $want_role are watched. Stops early once all
      # matching keys have advanced, else after $timeout seconds.
      measure() {
        local want_role="$1" base_ts="$2" timeout="$3" t key cur
        local -a pending=()
        for key in "''${!ROLE[@]}"; do
          [[ "''${ROLE[$key]}" == "$want_role" ]] && pending+=("$key")
        done
        for ((t = 0; t < timeout; t++)); do
          local -a still=()
          for key in "''${pending[@]}"; do
            cur=$(cnt "''${SUBOUT[$key]}")
            if (( cur > ''${PRECNT[$key]} )); then
              RECOV[$key]=$(( $(date +%s) - base_ts ))
            else
              still+=("$key")
            fi
          done
          pending=("''${still[@]}")
          (( ''${#pending[@]} == 0 )) && break
          sleep 1
        done
      }

      IFS=',' read -ra NODE_ARR <<< "$NODES"
      IFS=',' read -ra BUS_ARR  <<< "$BUSES"
      IFS=',' read -ra CN_ARR   <<< "$CLIENT_NODES"

      log "starting: rounds=$ROUNDS interval=$INTERVAL clients=$CLIENTS buses=$BUSES"
      log "client nodes=$CLIENT_NODES  kill rotation=$NODES"

      for ((r = 1; r <= ROUNDS; r++)); do
        victim="''${NODE_ARR[$(( (r - 1) % ''${#NODE_ARR[@]} ))]}"
        log "── round $r/$ROUNDS — victim: $victim ──"

        declare -A SUBPID PUBPID SUBOUT ROLE BUSOF CNOF PRECNT RECOV

        # Launch CLIENTS producer+consumer pairs per bus, spread over CN_ARR.
        for bus in "''${BUS_ARR[@]}"; do
          bin="$(bus_bin "$bus")"
          for ((i = 0; i < CLIENTS; i++)); do
            cn="''${CN_ARR[$(( i % ''${#CN_ARR[@]} ))]}"
            key="$bus.c$i.$cn"
            out="$LOG_DIR/r$r.$key.sub"
            : > "$out"
            addr="$(bus_addr "$bus" "$cn")"
            "$bin" sub -addr "$addr" -subject demo/chaos -json >> "$out" 2>&1 &
            SUBPID[$key]=$!
            # Persistent producer at 1/s for the whole round (killed at cleanup).
            "$bin" pub -addr "$addr" -subject demo/chaos -msg "m-$key-r$r" \
              -count 1000000 -rate 1/s >/dev/null 2>&1 &
            PUBPID[$key]=$!
            SUBOUT[$key]="$out"
            BUSOF[$key]="$bus"
            CNOF[$key]="$cn"
            if [[ "$cn" == "$victim" ]]; then ROLE[$key]="direct-hit"; else ROLE[$key]="survivor"; fi
          done
        done

        log "warmup $WARMUP s ($(( ''${#SUBOUT[@]} )) pairs across $BUSES)"
        sleep "$WARMUP"

        # Baseline each subscriber's received count just before the kill.
        for key in "''${!SUBOUT[@]}"; do PRECNT[$key]="$(cnt "''${SUBOUT[$key]}")"; done

        log "killing $victim"
        k8s-vm-stop-one --node="$victim" || log "WARN: stop-one failed"
        kill_ts=$(date +%s)

        # Phase A: survivors should keep receiving (measure reroute time).
        log "measuring survivor ride-through (up to $REROUTE_TIMEOUT s)"
        measure survivor "$kill_ts" "$REROUTE_TIMEOUT"

        log "node stays down ($INTERVAL s)"
        sleep "$INTERVAL"

        log "restarting $victim"
        k8s-vm-start-one --node="$victim" || log "WARN: start-one failed"
        restart_ts=$(date +%s)

        # Re-baseline the direct-hit subscribers at restart, then measure how
        # long (if ever) they resume after their node returns.
        for key in "''${!ROLE[@]}"; do
          [[ "''${ROLE[$key]}" == "direct-hit" ]] && PRECNT[$key]="$(cnt "''${SUBOUT[$key]}")"
        done
        log "measuring direct-hit recovery after restart (up to $RECOVERY_TIMEOUT s)"
        measure direct-hit "$restart_ts" "$RECOVERY_TIMEOUT"

        # Write one summary row per client.
        for key in "''${!SUBOUT[@]}"; do
          role="''${ROLE[$key]}"
          if [[ "$role" == "survivor" ]]; then from="kill"; else from="restart"; fi
          rec="''${RECOV[$key]:-TIMEOUT}"
          msgs="$(cnt "''${SUBOUT[$key]}")"
          echo -e "$r\t$victim\t''${BUSOF[$key]}\t''${CNOF[$key]}\t$role\t$from\t$rec\t$msgs" >> "$SUMMARY"
        done

        # Cleanup this round's clients.
        for key in "''${!SUBPID[@]}"; do kill "''${SUBPID[$key]}" 2>/dev/null || true; done
        for key in "''${!PUBPID[@]}"; do kill "''${PUBPID[$key]}" 2>/dev/null || true; done
        pkill -x natscli    2>/dev/null || true
        pkill -x mqttcli    2>/dev/null || true
        pkill -x valkeycli  2>/dev/null || true
        pkill -x rabbitmqcli 2>/dev/null || true
        pkill -x grpcbuscli 2>/dev/null || true
        unset SUBPID PUBPID SUBOUT ROLE BUSOF CNOF PRECNT RECOV

        log "post-round settle ($POST_ROUND_WAIT s)"
        sleep "$POST_ROUND_WAIT"
      done

      log "done — summary:"
      cat "$SUMMARY"
    '';
  };
}
