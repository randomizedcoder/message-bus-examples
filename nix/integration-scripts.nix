# nix/integration-scripts.nix
#
# All-clients integration smoke test.
#
# Brings up pub+sub for all four message-bus CLIs at once, in their
# HA/durable modes (NATS JetStream, RabbitMQ quorum queue, ValKey via Sentinel,
# MQTT bridged), publishes N known messages per bus, and ASSERTS that every one
# is received. Unlike the chaos/soak/bench harnesses (which report, never fail),
# this exits non-zero on any loss — a real smoke test, CI-ready later.
#
#   nix run .#k8s-integration-test
#   nix run .#k8s-integration-test -- --buses nats,rabbitmq,valkey --count 50
#   nix run .#k8s-integration-test -- --timeout 30s --log-dir ./integration-logs
#
# Requires a running cluster with the four buses deployed; SSH to cp0 for the
# RabbitMQ/ValKey credentials (handled by the k8s-vm-ssh wrapper).
#
{ pkgs }:
let
  constants = import ./constants.nix;
  vmScripts = import ./microvm-scripts.nix { inherit pkgs; };
  clients = import ./clients.nix { inherit pkgs; };
  mb = constants.messageBus;
  net = constants.network.ipv4;
in
{
  integrationTest = pkgs.writeShellApplication {
    name = "k8s-integration-test";
    runtimeInputs = with pkgs; [
      coreutils
      gawk
      gnused
      gnugrep
      procps
      jq
      vmScripts.ssh
      clients.package
    ];
    text = ''
      # Not -e: one bus failing must be recorded and reported, not abort the
      # whole run. The process exit code is computed from the per-bus results.
      set -uo pipefail

      # ─── Defaults (from constants.nix) ───────────────────────────────
      BUSES="${constants.integration.defaultBuses}"
      COUNT=${toString constants.integration.defaultCount}
      TIMEOUT="${constants.integration.defaultTimeout}"
      SETTLE="${constants.integration.defaultSettle}"
      LOG_DIR="${constants.integration.defaultLogDir}"
      NODE="cp0"

      NATS_PORT=${toString mb.nats.nodePort}
      RABBITMQ_PORT=${toString mb.rabbitmq.nodePortAmqp}
      MQTT_PORT=${toString mb.mqtt.nodePort}
      VALKEY_SENT0=${toString (mb.valkey.nodePortSentinelBase + 0)}
      VALKEY_SENT1=${toString (mb.valkey.nodePortSentinelBase + 1)}
      VALKEY_SENT2=${toString (mb.valkey.nodePortSentinelBase + 2)}

      usage() {
        cat <<EOF
Usage: k8s-integration-test [OPTIONS]

Brings up pub+sub for all selected buses at once (HA/durable modes), publishes
--count known messages per bus, and asserts every one is received. Exits
non-zero on any loss. Writes a PASS/FAIL table to --log-dir/integration.md.

Options:
  --buses=LIST      Comma-separated subset of nats,rabbitmq,valkey,mqtt
                    (default: $BUSES)
  --count=N, -n N   Messages published per bus (default: $COUNT)
  --timeout=DUR     Subscriber give-up timeout, e.g. 20s, 1m (default: $TIMEOUT)
  --settle=DUR      Delay before publishing, lets the sub connect (default: $SETTLE)
  --node=NAME       Client endpoint node cp0|cp1|cp2|w3 (default: $NODE)
  --log-dir=DIR     Output directory (default: $LOG_DIR)
  -h, --help        Show this help

Requires: cluster running with the four buses deployed; SSH to cp0 for the
RabbitMQ/ValKey credentials.
EOF
      }

      # Accept both --flag=value and --flag value.
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --buses=*)   BUSES="''${1#*=}" ;;
          --buses)     BUSES="''${2-}"; shift ;;
          --count=*)   COUNT="''${1#*=}" ;;
          --count|-n)  COUNT="''${2-}"; shift ;;
          --timeout=*) TIMEOUT="''${1#*=}" ;;
          --timeout)   TIMEOUT="''${2-}"; shift ;;
          --settle=*)  SETTLE="''${1#*=}" ;;
          --settle)    SETTLE="''${2-}"; shift ;;
          --node=*)    NODE="''${1#*=}" ;;
          --node)      NODE="''${2-}"; shift ;;
          --log-dir=*) LOG_DIR="''${1#*=}" ;;
          --log-dir)   LOG_DIR="''${2-}"; shift ;;
          -h|--help)   usage; exit 0 ;;
          *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
        esac
        shift
      done

      if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [ "$COUNT" -lt 1 ]; then
        echo "invalid --count: $COUNT (want a positive integer)" >&2; exit 2
      fi

      case "$NODE" in
        cp0) CLIENT_NODE_IP="${net.cp0}" ;;
        cp1) CLIENT_NODE_IP="${net.cp1}" ;;
        cp2) CLIENT_NODE_IP="${net.cp2}" ;;
        w3)  CLIENT_NODE_IP="${net.w3}" ;;
        *)   echo "unknown --node: $NODE (want cp0|cp1|cp2|w3)" >&2; exit 2 ;;
      esac
      SENTINELS="$CLIENT_NODE_IP:$VALKEY_SENT0,$CLIENT_NODE_IP:$VALKEY_SENT1,$CLIENT_NODE_IP:$VALKEY_SENT2"

      log() { echo "[integ] $(date +%H:%M:%S) $*"; }
      # Run a command on cp0 with KUBECONFIG set. Non-interactive SSH does not
      # source the profile, so a bare kubectl would hit localhost:8080.
      kexec() { k8s-vm-ssh --node=cp0 env KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig "$@"; }

      # Clean up any stray clients this test started, on any exit.
      # shellcheck disable=SC2329  # invoked indirectly via `trap cleanup EXIT`
      cleanup() {
        for b in natscli rabbitmqcli valkeycli mqttcli; do
          pkill -x "$b" >/dev/null 2>&1 || true
        done
      }
      trap cleanup EXIT

      mkdir -p "$LOG_DIR"
      rm -f "$LOG_DIR"/*.result "$LOG_DIR"/*-sub.json "$LOG_DIR"/*-sub.err \
            "$LOG_DIR"/*-pub.log "$LOG_DIR"/*.recv-set 2>/dev/null || true

      log "reading bus credentials from cluster"
      RABBITMQ_PASS="$(kexec kubectl -n ${mb.rabbitmq.namespace} get secret rabbitmq-credentials \
        -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' 2>/dev/null | base64 -d || true)"
      VALKEY_PASS="$(kexec kubectl -n ${mb.valkey.namespace} get secret valkey-credentials \
        -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"

      # bus_ready BUS — bounded wait for the bus's pod-0 to be Ready.
      bus_ready() {
        case "$1" in
          nats)     kexec kubectl -n ${mb.nats.namespace} wait --for=condition=Ready pod/nats-0 --timeout=60s >/dev/null 2>&1 ;;
          rabbitmq) kexec kubectl -n ${mb.rabbitmq.namespace} wait --for=condition=Ready pod/rabbitmq-0 --timeout=60s >/dev/null 2>&1 ;;
          valkey)   kexec kubectl -n ${mb.valkey.namespace} wait --for=condition=Ready pod/valkey-0 --timeout=90s >/dev/null 2>&1 ;;
          mqtt)     kexec kubectl -n ${mb.mqtt.namespace} wait --for=condition=Ready pod/mqtt-0 --timeout=60s >/dev/null 2>&1 ;;
          *) return 1 ;;
        esac
      }

      # run_bus BUS — start a subscriber, publish COUNT distinct messages, verify
      # every one arrives, and write "bus<TAB>subject<TAB>expected<TAB>received<TAB>result"
      # to $LOG_DIR/$BUS.result. Runs concurrently, one per bus.
      run_bus() {
        local bus="$1" subject msgbase
        local -a sub pub
        case "$bus" in
          nats)
            subject="integ.nats"; msgbase="integ-nats"
            sub=(natscli sub -jetstream -addr "$CLIENT_NODE_IP:$NATS_PORT" -subject "$subject")
            pub=(natscli pub -jetstream -addr "$CLIENT_NODE_IP:$NATS_PORT" -subject "$subject") ;;
          rabbitmq)
            subject="integ.rmq"; msgbase="integ-rabbitmq"
            sub=(rabbitmqcli sub -durable -addr "$CLIENT_NODE_IP:$RABBITMQ_PORT" -subject "$subject" -pass "$RABBITMQ_PASS")
            pub=(rabbitmqcli pub -durable -addr "$CLIENT_NODE_IP:$RABBITMQ_PORT" -subject "$subject" -pass "$RABBITMQ_PASS") ;;
          valkey)
            subject="integ.vk"; msgbase="integ-valkey"
            sub=(valkeycli sub -sentinels "$SENTINELS" -subject "$subject" -pass "$VALKEY_PASS")
            pub=(valkeycli pub -sentinels "$SENTINELS" -subject "$subject" -pass "$VALKEY_PASS") ;;
          mqtt)
            subject="integ/mqtt"; msgbase="integ-mqtt"
            sub=(mqttcli sub -addr "$CLIENT_NODE_IP:$MQTT_PORT" -subject "$subject")
            pub=(mqttcli pub -addr "$CLIENT_NODE_IP:$MQTT_PORT" -subject "$subject") ;;
          *) echo "unknown bus: $bus" >&2; return ;;
        esac

        local result="$LOG_DIR/$bus.result"
        local subout="$LOG_DIR/$bus-sub.json"
        local puberr="$LOG_DIR/$bus-pub.log"

        if ! bus_ready "$bus"; then
          log "$bus: pod not Ready — skipping"
          printf '%s\t%s\t%d\t%d\t%s\n' "$bus" "$subject" "$COUNT" 0 "FAIL(not-ready)" > "$result"
          return
        fi

        # Subscriber collects for the whole window (-timeout, no -count): the
        # buses are at-least-once (JetStream/quorum/QoS-1 can redeliver), so a
        # -count limiter would stop on total received — duplicates included —
        # and could exit before every distinct message arrives. Give it time to
        # establish, then publish.
        "''${sub[@]}" -timeout "$TIMEOUT" -json > "$subout" 2> "$LOG_DIR/$bus-sub.err" &
        local subpid=$!
        sleep "$SETTLE"
        local pubrc=0
        "''${pub[@]}" -count "$COUNT" -msg "$msgbase" > "$puberr" 2>&1 || pubrc=$?
        wait "$subpid" 2>/dev/null || true

        # Verify every expected payload is present in the DISTINCT received set.
        # PubLoop appends a 1-based sequence to -msg when count>1, so payloads
        # are "<msgbase> 1".."<msgbase> N" (or just "<msgbase>" for count==1).
        local recvset="$LOG_DIR/$bus.recv-set"
        jq -r '.data' "$subout" 2>/dev/null | sort -u > "$recvset" || : > "$recvset"
        local missing=0 i exp
        for ((i = 1; i <= COUNT; i++)); do
          if [ "$COUNT" -eq 1 ]; then exp="$msgbase"; else exp="$msgbase $i"; fi
          grep -Fxq -- "$exp" "$recvset" || missing=$((missing + 1))
        done
        local recv verdict
        recv=$(grep -c . "$recvset" 2>/dev/null || echo 0)   # distinct payloads received
        if [ "$pubrc" -eq 0 ] && [ "$missing" -eq 0 ]; then verdict="PASS"; else verdict="FAIL"; fi
        printf '%s\t%s\t%d\t%d\t%s\n' "$bus" "$subject" "$COUNT" "$recv" "$verdict" > "$result"
        log "$bus: $verdict (expected $COUNT, distinct received $recv, missing $missing, pub rc=$pubrc)"
      }

      # ─── Launch every selected bus at once, then join ────────────────
      IFS=',' read -r -a BUS_ARR <<< "$BUSES"
      log "running buses: $BUSES (count=$COUNT timeout=$TIMEOUT node=$NODE)"
      declare -a RUN_PIDS=()
      for b in "''${BUS_ARR[@]}"; do
        [ -n "$b" ] || continue
        run_bus "$b" &
        RUN_PIDS+=("$!")
      done
      for p in "''${RUN_PIDS[@]}"; do wait "$p" 2>/dev/null || true; done

      # ─── Aggregate + report ──────────────────────────────────────────
      TSV="$LOG_DIR/integration.tsv"
      REPORT="$LOG_DIR/integration.md"
      printf 'bus\tsubject\texpected\treceived\tresult\n' > "$TSV"
      {
        echo "# Integration test"
        echo
        echo "Node \`$NODE\` · $COUNT messages/bus · timeout $TIMEOUT"
        echo
        echo "| Bus | Subject | Expected | Received | Result |"
        echo "|-----|---------|---------:|---------:|--------|"
      } > "$REPORT"

      EXIT=0
      for b in "''${BUS_ARR[@]}"; do
        [ -n "$b" ] || continue
        if [ -f "$LOG_DIR/$b.result" ]; then
          cat "$LOG_DIR/$b.result" >> "$TSV"
          IFS=$'\t' read -r r_bus r_subject r_expected r_received r_result < "$LOG_DIR/$b.result"
          echo "| $r_bus | $r_subject | $r_expected | $r_received | $r_result |" >> "$REPORT"
          [ "$r_result" = "PASS" ] || EXIT=1
        else
          printf '%s\t-\t%d\t%d\t%s\n' "$b" "$COUNT" 0 "FAIL(no-result)" >> "$TSV"
          echo "| $b | - | $COUNT | 0 | FAIL(no-result) |" >> "$REPORT"
          EXIT=1
        fi
      done

      {
        echo
        if [ "$EXIT" -eq 0 ]; then
          echo "**RESULT: PASS** — every selected bus delivered all messages."
        else
          echo "**RESULT: FAIL** — at least one bus lost messages (see table)."
        fi
      } >> "$REPORT"

      cat "$REPORT"
      exit "$EXIT"
    '';
  };
}
