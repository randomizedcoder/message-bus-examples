# nix/bench-scripts.nix
#
# Per-bus throughput / latency benchmark for the four message-bus clients.
#
# For each requested bus it starts a subscriber, then publishes --count
# messages as fast as possible (no -rate throttle) and times the publisher to
# completion. Publish throughput (msgs/s) is computed from that wall-clock
# time; consume throughput and request/confirm latency percentiles are read
# from the client OTel metrics via the in-cluster Prometheus (queried from the
# cp0 host over its NodePort, per the observability convention). Results are
# written as bench.tsv + bench.md (one row per bus).
#
#   nix run .#k8s-client-bench                       # all four buses, 200k msgs each
#   nix run .#k8s-client-bench -- --count 500000 --buses nats,rabbitmq
#   nix run .#k8s-client-bench -- --msg-size 256
#
# This is the live-cluster complement to the hermetic Go micro-benchmarks
# (`nix run .#clients-bench`). It needs the cluster up with the monitoring
# stack deployed, and SSH to cp0 for bus credentials.
#
{ pkgs }:
let
  constants = import ./constants.nix;
  vmScripts = import ./microvm-scripts.nix { inherit pkgs; };
  clients   = import ./clients.nix { inherit pkgs; };
  mb  = constants.messageBus;
  mon = constants.monitoring;
in
{
  clientBench = pkgs.writeShellApplication {
    name = "k8s-client-bench";
    runtimeInputs = with pkgs; [
      coreutils gawk gnused curl jq procps
      vmScripts.ssh
      clients.package
    ];
    text = ''
      # Not -e: a single bus failing should not abort the other buses' runs.
      set -uo pipefail

      # ─── Defaults (from constants.nix) ───────────────────────────────
      COUNT=${toString constants.bench.defaultCount}
      MSG_SIZE=${toString constants.bench.defaultMsgSize}
      BUSES="${constants.bench.defaultBuses}"
      LOG_DIR="${constants.bench.defaultLogDir}"
      WARMUP=${toString constants.bench.warmupSec}
      SETTLE=${toString constants.bench.settleSec}

      CLIENT_NODE_IP="${constants.network.ipv4.cp0}"   # stable endpoint
      METRICS_IP="${mon.hostBridgeIP}"                  # host bridge — Prometheus scrapes here
      METRICS_BASE=${toString mon.clientMetricsBasePort}
      PROM_URL="http://${constants.network.ipv4.cp0}:${toString mon.prometheus.nodePort}"

      NATS_PORT=${toString mb.nats.nodePort}
      RABBITMQ_PORT=${toString mb.rabbitmq.nodePortAmqp}
      MQTT_PORT=${toString mb.mqtt.nodePort}
      VALKEY_SENT0=${toString (mb.valkey.nodePortSentinelBase + 0)}
      VALKEY_SENT1=${toString (mb.valkey.nodePortSentinelBase + 1)}
      VALKEY_SENT2=${toString (mb.valkey.nodePortSentinelBase + 2)}

      usage() {
        cat <<EOF
Usage: k8s-client-bench [OPTIONS]

For each bus: start a subscriber, publish --count messages unthrottled, time
the publisher, and report publish/consume throughput and latency percentiles
(from Prometheus). Writes bench.tsv + bench.md to --log-dir.

Options:
  --count=N          Messages per publisher, unthrottled (default: $COUNT)
  --msg-size=BYTES   Pad each message body to ~BYTES (default: $MSG_SIZE = "bench")
  --buses=LIST       Comma-separated subset to run (default: $BUSES)
  --log-dir=DIR      Output directory (default: $LOG_DIR)
  --warmup=SEC       Subscriber connect grace before publishing (default: $WARMUP)
  -h, --help         Show this help

Requires: cluster running with the monitoring stack deployed; SSH to cp0 for
bus credentials. NATS is benchmarked in core (fire-and-forget) mode — the pure
publish path.
EOF
      }

      while [[ $# -gt 0 ]]; do
        case "$1" in
          --count=*)    COUNT="''${1#*=}" ;;
          --count)      COUNT="''${2-}"; shift ;;
          --msg-size=*) MSG_SIZE="''${1#*=}" ;;
          --msg-size)   MSG_SIZE="''${2-}"; shift ;;
          --buses=*)    BUSES="''${1#*=}" ;;
          --buses)      BUSES="''${2-}"; shift ;;
          --log-dir=*)  LOG_DIR="''${1#*=}" ;;
          --log-dir)    LOG_DIR="''${2-}"; shift ;;
          --warmup=*)   WARMUP="''${1#*=}" ;;
          --warmup)     WARMUP="''${2-}"; shift ;;
          -h|--help)    usage; exit 0 ;;
          *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
        esac
        shift
      done

      log() { echo "[bench] $(date +%H:%M:%S) $*"; }
      kexec() { k8s-vm-ssh --node=cp0 env KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig "$@"; }
      mport() { echo "$METRICS_IP:$(( METRICS_BASE + $1 ))"; }

      # Message body: the default "bench", or a MSG_SIZE-byte pad of 'x' (the
      # per-message sequence suffix adds a few bytes on top).
      MSG="bench"
      if [[ "$MSG_SIZE" =~ ^[0-9]+$ ]] && [ "$MSG_SIZE" -gt 0 ]; then
        MSG="$(printf 'x%.0s' $(seq 1 "$MSG_SIZE"))"
      fi

      mkdir -p "$LOG_DIR"
      TSV="$LOG_DIR/bench.tsv"
      REPORT="$LOG_DIR/bench.md"
      echo -e "bus\tmode\tcount\tpublish_s\tpublish_rate\treceived\tlatency_p50_ms\tlatency_p99_ms" > "$TSV"

      log "reading bus credentials from cluster"
      RABBITMQ_PASS="$(kexec kubectl -n ${mb.rabbitmq.namespace} get secret rabbitmq-credentials \
        -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' 2>/dev/null | base64 -d || true)"
      VALKEY_PASS="$(kexec kubectl -n ${mb.valkey.namespace} get secret valkey-credentials \
        -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"

      PROM_OK=0
      if curl -s --max-time 5 "$PROM_URL/-/ready" >/dev/null 2>&1; then PROM_OK=1; fi
      [ "$PROM_OK" = 1 ] || log "WARN: Prometheus unreachable at $PROM_URL — consume rate/latency will be blank"

      # prom_scalar QUERY → the single scalar value (or "" on miss/error).
      prom_scalar() {
        [ "$PROM_OK" = 1 ] || { echo ""; return; }
        curl -s --max-time 5 "$PROM_URL/api/v1/query" \
          --data-urlencode "query=$1" 2>/dev/null \
          | jq -r '.data.result[0].value[1] // ""' 2>/dev/null
      }

      # run_one BUS MODE ROLEBASE runs a sub + timed pub for one bus.
      #   $1 bus label, $2 mode label, $3 metrics-port offset (sub=$3, pub=$3+1)
      # The remaining bus-specific pub/sub argv are provided via the *_SUB /
      # *_PUB arrays set by the caller.
      run_bus() {
        local bus="$1" mode="$2" off="$3"; shift 3
        local sub_mport pub_mport
        sub_mport="$(mport "$off")"; pub_mport="$(mport $(( off + 1 )))"
        local sublog="$LOG_DIR/$bus-sub.log" publog="$LOG_DIR/$bus-pub.log"
        : > "$sublog"; : > "$publog"

        log "$bus: starting subscriber"
        "''${SUB_CMD[@]}" -metrics-addr "$sub_mport" >> "$sublog" 2>&1 &
        local sub_pid=$!
        sleep "$WARMUP"

        log "$bus: publishing $COUNT messages (unthrottled)"
        local t0 t1 elapsed rate
        t0=$(date +%s.%N)
        "''${PUB_CMD[@]}" -count "$COUNT" -msg "$MSG" -metrics-addr "$pub_mport" >> "$publog" 2>&1 || \
          log "WARN: $bus publisher exited non-zero (see $publog)"
        t1=$(date +%s.%N)

        # Let trailing subscriber deliveries + the final scrape land.
        sleep "$SETTLE"
        kill "$sub_pid" 2>/dev/null || true
        wait "$sub_pid" 2>/dev/null || true

        elapsed=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", b-a}')
        rate=$(awk -v c="$COUNT" -v e="$elapsed" 'BEGIN{ if(e>0) printf "%.0f", c/e; else print "0" }')

        local received p50 p99
        received=$(prom_scalar "sum(mbclient_received_total{bus=\"$bus\",role=\"sub\"})")
        p50=$(prom_scalar "1000 * histogram_quantile(0.50, sum by (le) (rate(mbclient_request_latency_seconds_bucket{bus=\"$bus\"}[5m])))")
        p99=$(prom_scalar "1000 * histogram_quantile(0.99, sum by (le) (rate(mbclient_request_latency_seconds_bucket{bus=\"$bus\"}[5m])))")
        received="''${received:-}"
        [ -n "$received" ] && received=$(awk -v v="$received" 'BEGIN{printf "%.0f", v}')
        [ -n "$p50" ] && p50=$(awk -v v="$p50" 'BEGIN{printf "%.2f", v}')
        [ -n "$p99" ] && p99=$(awk -v v="$p99" 'BEGIN{printf "%.2f", v}')

        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
          "$bus" "$mode" "$COUNT" "$elapsed" "$rate" "''${received:-}" "''${p50:-}" "''${p99:-}" >> "$TSV"
        log "$bus: $rate msgs/s published in ''${elapsed}s (received=''${received:-?})"
      }

      IFS=',' read -ra WANT <<< "$BUSES"
      want() { local b; for b in "''${WANT[@]}"; do [ "$b" = "$1" ] && return 0; done; return 1; }

      if want nats; then
        SUB_CMD=(natscli sub -addr "$CLIENT_NODE_IP:$NATS_PORT" -subject bench.nats)
        PUB_CMD=(natscli pub -addr "$CLIENT_NODE_IP:$NATS_PORT" -subject bench.nats)
        run_bus nats core 0
      fi
      if want rabbitmq; then
        SUB_CMD=(rabbitmqcli sub -durable -addr "$CLIENT_NODE_IP:$RABBITMQ_PORT" -subject bench.rmq -pass "$RABBITMQ_PASS")
        PUB_CMD=(rabbitmqcli pub -durable -addr "$CLIENT_NODE_IP:$RABBITMQ_PORT" -subject bench.rmq -pass "$RABBITMQ_PASS")
        run_bus rabbitmq durable 2
      fi
      if want valkey; then
        SENTINELS="$CLIENT_NODE_IP:$VALKEY_SENT0,$CLIENT_NODE_IP:$VALKEY_SENT1,$CLIENT_NODE_IP:$VALKEY_SENT2"
        SUB_CMD=(valkeycli sub -sentinels "$SENTINELS" -subject bench.vk -pass "$VALKEY_PASS")
        PUB_CMD=(valkeycli pub -sentinels "$SENTINELS" -subject bench.vk -pass "$VALKEY_PASS")
        run_bus valkey sentinel 4
      fi
      if want mqtt; then
        SUB_CMD=(mqttcli sub -addr "$CLIENT_NODE_IP:$MQTT_PORT" -subject bench/mqtt)
        PUB_CMD=(mqttcli pub -addr "$CLIENT_NODE_IP:$MQTT_PORT" -subject bench/mqtt)
        run_bus mqtt core 6
      fi

      # Make sure no bench client outlives the harness.
      pkill -x natscli 2>/dev/null || true
      pkill -x rabbitmqcli 2>/dev/null || true
      pkill -x mqttcli 2>/dev/null || true
      pkill -x valkeycli 2>/dev/null || true

      log "writing report"
      {
        echo "# Client benchmark"
        echo
        echo "- Run: $(date)"
        echo "- Messages per publisher: $COUNT (unthrottled), body size: $([ "$MSG_SIZE" -gt 0 ] 2>/dev/null && echo "''${MSG_SIZE}B" || echo default)"
        echo "- Buses: $BUSES"
        if [ "$PROM_OK" = 1 ]; then
          echo "- Consume rate / latency from Prometheus at $PROM_URL."
        else
          echo "- Prometheus was unreachable — consume/latency columns are blank; publish throughput is wall-clock."
        fi
        echo
        echo '| bus | mode | count | publish_s | publish msgs/s | received | p50 ms | p99 ms |'
        echo '|-----|------|-------|-----------|----------------|----------|--------|--------|'
        tail -n +2 "$TSV" | awk -F'\t' '{printf "| %s | %s | %s | %s | %s | %s | %s | %s |\n",$1,$2,$3,$4,$5,($6==""?"-":$6),($7==""?"-":$7),($8==""?"-":$8)}'
        echo
        echo "Publish throughput is count / wall-clock publish time. p50/p99 are"
        echo "from mbclient_request_latency_seconds (recorded where the client times"
        echo "a confirm — RabbitMQ today; blank where a bus records no latency)."
      } > "$REPORT"

      log "done — report:"
      cat "$REPORT"
    '';
  };
}
