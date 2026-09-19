# nix/soak-scripts.nix
#
# Sustained multi-hour soak test for all four message buses.
#
# Runs every client at once, in its HA/failover-designed mode, under steady
# load, while killing one MicroVM at a time on a rotation (cp0 is spared — it
# is the stable client endpoint and hosts Prometheus/Grafana). The NATS concept
# demos are re-run on an interval and their pass/fail recorded. Each long-lived
# client exports OTel metrics (scraped by the in-cluster Prometheus over the
# host bridge); at the end the harness queries Prometheus and writes a per-bus
# report of how each client held up (published/received/loss/reconnects).
#
#   nix run .#k8s-soak-test                       # 4h, faults every 15m
#   nix run .#k8s-soak-test -- --duration 10m --fault-interval 3m
#   nix run .#k8s-soak-test -- --no-faults        # steady load only
#
# For a real multi-hour run, launch it detached so it outlives the terminal:
#   nohup nix run .#k8s-soak-test -- --duration 4h > soak.out 2>&1 &
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
  soakTest = pkgs.writeShellApplication {
    name = "k8s-soak-test";
    runtimeInputs = with pkgs; [
      coreutils gawk gnused procps curl jq
      vmScripts.stopOne vmScripts.startOne vmScripts.ssh
      clients.package
    ];
    text = ''
      # Not -e: a soak must tolerate individual client/publish failures — those
      # are the very thing being measured.
      set -uo pipefail

      # ─── Defaults (from constants.nix) ───────────────────────────────
      DURATION="${constants.soak.defaultDuration}"
      FAULT_INTERVAL=${toString constants.soak.defaultFaultIntervalSec}
      RATE="${constants.soak.defaultRate}"
      DEMO_INTERVAL=${toString constants.soak.defaultDemoIntervalSec}
      LOG_DIR="${constants.soak.defaultLogDir}"
      NODES="${constants.soak.nodes}"
      NO_FAULTS=0
      FAULT_DOWN=30           # seconds a killed node stays down before restart

      CLIENT_NODE_IP="${constants.network.ipv4.cp0}"   # stable endpoint (never killed)
      LEAF_NODE_IP="${constants.network.ipv4.w3}"       # leaf NodePort lives here
      METRICS_IP="${mon.hostBridgeIP}"                  # host bridge — Prometheus scrapes here
      METRICS_BASE=${toString mon.clientMetricsBasePort}
      PROM_URL="http://${constants.network.ipv4.cp0}:${toString mon.prometheus.nodePort}"

      NATS_PORT=${toString mb.nats.nodePort}
      NATS_LEAF_PORT=${toString mb.nats.nodePortLeaf}
      RABBITMQ_PORT=${toString mb.rabbitmq.nodePortAmqp}
      MQTT_PORT=${toString mb.mqtt.nodePort}
      VALKEY_SENT0=${toString (mb.valkey.nodePortSentinelBase + 0)}
      VALKEY_SENT1=${toString (mb.valkey.nodePortSentinelBase + 1)}
      VALKEY_SENT2=${toString (mb.valkey.nodePortSentinelBase + 2)}

      usage() {
        cat <<EOF
Usage: k8s-soak-test [OPTIONS]

Runs all four buses (HA modes) + the NATS demos under load for --duration,
killing one node at a time every --fault-interval (cp0 spared). Clients export
OTel metrics; an end-of-run report is written to --log-dir.

Options:
  --duration=DUR         Total run length, e.g. 4h, 30m, 90s (default: $DURATION)
  --fault-interval=SEC   Seconds between node kills (default: $FAULT_INTERVAL)
  --fault-down=SEC       Seconds a killed node stays down (default: $FAULT_DOWN)
  --rate=N/s             Per-publisher publish rate (default: $RATE)
  --demo-interval=SEC    Seconds between NATS demo re-runs (default: $DEMO_INTERVAL)
  --nodes=LIST           Kill rotation (default: $NODES)
  --no-faults            Steady load only, no node kills
  --log-dir=DIR          Output directory (default: $LOG_DIR)
  -h, --help             Show this help

Requires: cluster running with the monitoring stack deployed; SSH to cp0 for
bus credentials. Launch detached for long runs (see the file header).
EOF
      }

      # Accept both --flag=value and --flag value.
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --duration=*)       DURATION="''${1#*=}" ;;
          --duration)         DURATION="''${2-}"; shift ;;
          --fault-interval=*) FAULT_INTERVAL="''${1#*=}" ;;
          --fault-interval)   FAULT_INTERVAL="''${2-}"; shift ;;
          --fault-down=*)     FAULT_DOWN="''${1#*=}" ;;
          --fault-down)       FAULT_DOWN="''${2-}"; shift ;;
          --rate=*)           RATE="''${1#*=}" ;;
          --rate)             RATE="''${2-}"; shift ;;
          --demo-interval=*)  DEMO_INTERVAL="''${1#*=}" ;;
          --demo-interval)    DEMO_INTERVAL="''${2-}"; shift ;;
          --nodes=*)          NODES="''${1#*=}" ;;
          --nodes)            NODES="''${2-}"; shift ;;
          --log-dir=*)        LOG_DIR="''${1#*=}" ;;
          --log-dir)          LOG_DIR="''${2-}"; shift ;;
          --no-faults)        NO_FAULTS=1 ;;
          -h|--help)          usage; exit 0 ;;
          *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
        esac
        shift
      done

      log() { echo "[soak] $(date +%H:%M:%S) $*"; }
      # Run a command on cp0 with KUBECONFIG set. Non-interactive SSH does
      # not source the profile, so kubectl would otherwise default to
      # localhost:8080 and fail — silently corrupting credential fetches.
      kexec() { k8s-vm-ssh --node=cp0 env KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig "$@"; }

      # DUR → seconds.
      to_seconds() {
        local d="$1" n unit
        n="''${d%[smhd]}"
        unit="''${d#"$n"}"
        case "$unit" in
          s|"") echo "$n" ;;
          m)    echo $(( n * 60 )) ;;
          h)    echo $(( n * 3600 )) ;;
          d)    echo $(( n * 86400 )) ;;
          *)    echo "$n" ;;
        esac
      }

      DUR_SECS="$(to_seconds "$DURATION")"
      RATE_N="''${RATE%%/*}"                       # "20/s" → "20"
      if ! [[ "$RATE_N" =~ ^[0-9]+$ ]]; then RATE_N=20; fi
      COUNT=$(( RATE_N * DUR_SECS + RATE_N ))       # enough to cover the whole run
      START=$(date +%s)
      END=$(( START + DUR_SECS ))

      mkdir -p "$LOG_DIR"
      : > "$LOG_DIR/events.tsv"
      : > "$LOG_DIR/demos.tsv"
      echo -e "ts\tnode\taction" > "$LOG_DIR/events.tsv"
      echo -e "ts\tdemo\tresult" > "$LOG_DIR/demos.tsv"

      log "reading bus credentials from cluster"
      RABBITMQ_PASS="$(kexec kubectl -n ${mb.rabbitmq.namespace} get secret rabbitmq-credentials \
        -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' 2>/dev/null | base64 -d || true)"
      VALKEY_PASS="$(kexec kubectl -n ${mb.valkey.namespace} get secret valkey-credentials \
        -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"

      # ─── Workload supervision ────────────────────────────────────────
      # Each workload runs in a respawn loop so it survives node kills and
      # (for RabbitMQ, which has no auto-reconnect) connection drops.
      declare -a PIDS=()
      supervise() {
        local name="$1"; shift
        local logf="$LOG_DIR/$name.log"
        : > "$logf"
        (
          while [ "$(date +%s)" -lt "$END" ]; do
            "$@" >> "$logf" 2>&1 || true
            echo "[soak] $(date +%H:%M:%S) $name exited; respawning" >> "$logf"
            sleep 1
          done
        ) &
        PIDS+=("$!")
        log "started $name (pid $!) -> $logf"
      }

      mport() { echo "$METRICS_IP:$(( METRICS_BASE + $1 ))"; }

      log "launching workloads: duration=$DURATION rate=$RATE count=$COUNT faults=$([ "$NO_FAULTS" = 1 ] && echo none || echo "$NODES/''${FAULT_INTERVAL}s")"

      # NATS — durable JetStream
      supervise nats-sub natscli sub -jetstream \
        -addr "$CLIENT_NODE_IP:$NATS_PORT" -subject soak.nats -metrics-addr "$(mport 0)"
      supervise nats-pub natscli pub -jetstream \
        -addr "$CLIENT_NODE_IP:$NATS_PORT" -subject soak.nats -rate "$RATE" -count "$COUNT" -msg soak \
        -metrics-addr "$(mport 1)"

      # RabbitMQ — durable quorum queue
      supervise rabbitmq-sub rabbitmqcli sub -durable \
        -addr "$CLIENT_NODE_IP:$RABBITMQ_PORT" -subject soak.rmq -pass "$RABBITMQ_PASS" -metrics-addr "$(mport 2)"
      supervise rabbitmq-pub rabbitmqcli pub -durable \
        -addr "$CLIENT_NODE_IP:$RABBITMQ_PORT" -subject soak.rmq -pass "$RABBITMQ_PASS" \
        -rate "$RATE" -count "$COUNT" -msg soak -metrics-addr "$(mport 3)"

      # ValKey — Sentinel-backed primary discovery
      SENTINELS="$CLIENT_NODE_IP:$VALKEY_SENT0,$CLIENT_NODE_IP:$VALKEY_SENT1,$CLIENT_NODE_IP:$VALKEY_SENT2"
      supervise valkey-sub valkeycli sub -sentinels "$SENTINELS" \
        -subject soak.vk -pass "$VALKEY_PASS" -metrics-addr "$(mport 4)"
      supervise valkey-pub valkeycli pub -sentinels "$SENTINELS" \
        -subject soak.vk -pass "$VALKEY_PASS" -rate "$RATE" -count "$COUNT" -msg soak -metrics-addr "$(mport 5)"

      # MQTT — full-mesh bridged (no HA flag)
      supervise mqtt-sub mqttcli sub \
        -addr "$CLIENT_NODE_IP:$MQTT_PORT" -subject soak/mqtt -metrics-addr "$(mport 6)"
      supervise mqtt-pub mqttcli pub \
        -addr "$CLIENT_NODE_IP:$MQTT_PORT" -subject soak/mqtt -rate "$RATE" -count "$COUNT" -msg soak \
        -metrics-addr "$(mport 7)"

      # ─── NATS concept demos on a loop ────────────────────────────────
      demo_loop() {
        while [ "$(date +%s)" -lt "$END" ]; do
          for demo in subjects request-reply queue-groups; do
            if "$demo" -addr "$CLIENT_NODE_IP:$NATS_PORT" >> "$LOG_DIR/demo-$demo.log" 2>&1; then
              echo -e "$(date +%s)\t$demo\tpass" >> "$LOG_DIR/demos.tsv"
            else
              echo -e "$(date +%s)\t$demo\tfail" >> "$LOG_DIR/demos.tsv"
            fi
          done
          if leaf -hub-addr "$CLIENT_NODE_IP:$NATS_PORT" -leaf-addr "$LEAF_NODE_IP:$NATS_LEAF_PORT" \
              >> "$LOG_DIR/demo-leaf.log" 2>&1; then
            echo -e "$(date +%s)\tleaf\tpass" >> "$LOG_DIR/demos.tsv"
          else
            echo -e "$(date +%s)\tleaf\tfail" >> "$LOG_DIR/demos.tsv"
          fi
          sleep "$DEMO_INTERVAL"
        done
      }
      demo_loop & PIDS+=("$!")
      log "started demo loop (pid $!)"

      # ─── Fault injection: rolling single-node kill ───────────────────
      fault_loop() {
        local -a arr; IFS=',' read -ra arr <<< "$NODES"
        local i=0 node
        while [ "$(date +%s)" -lt "$END" ]; do
          sleep "$FAULT_INTERVAL"
          [ "$(date +%s)" -lt "$END" ] || break
          node="''${arr[$(( i % ''${#arr[@]} ))]}"; i=$(( i + 1 ))
          log "fault: killing $node"
          echo -e "$(date +%s)\t$node\tkill" >> "$LOG_DIR/events.tsv"
          k8s-vm-stop-one --node="$node" >> "$LOG_DIR/faults.log" 2>&1 || log "WARN: stop $node failed"
          sleep "$FAULT_DOWN"
          log "fault: restarting $node"
          k8s-vm-start-one --node="$node" >> "$LOG_DIR/faults.log" 2>&1 || log "WARN: start $node failed"
          echo -e "$(date +%s)\t$node\trestart" >> "$LOG_DIR/events.tsv"
        done
      }
      if [ "$NO_FAULTS" = 0 ]; then
        fault_loop & PIDS+=("$!")
        log "started fault loop (pid $!)"
      else
        log "faults disabled (--no-faults)"
      fi

      # ─── Cleanup + report on exit ────────────────────────────────────
      stopped=0
      cleanup() {
        [ "$stopped" = 1 ] && return; stopped=1
        log "stopping (draining workloads)"
        for p in "''${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
        # Client processes are grandchildren of the supervise subshells; the
        # soak owns these binaries on the host, so stop them by name.
        pkill -x natscli    2>/dev/null || true
        pkill -x rabbitmqcli 2>/dev/null || true
        pkill -x mqttcli    2>/dev/null || true
        pkill -x valkeycli  2>/dev/null || true
      }
      trap 'cleanup' INT TERM

      # ─── Heartbeat until the run ends ────────────────────────────────
      while [ "$(date +%s)" -lt "$END" ]; do
        sleep 60
        now=$(date +%s)
        [ "$now" -lt "$END" ] || break
        log "elapsed $(( (now - START) / 60 ))m / $(( DUR_SECS / 60 ))m"
      done

      cleanup
      sleep 3   # let final scrapes land

      report_metric() { # $1 = metric name → "bus mode value" lines
        curl -s --max-time 5 "$PROM_URL/api/v1/query" \
          --data-urlencode "query=sum by (bus, mode) ($1)" 2>/dev/null \
          | jq -r '.data.result[]? | "\(.metric.bus)\t\(.metric.mode)\t\(.value[1])"' 2>/dev/null
      }

      log "writing report"
      SUMMARY="$LOG_DIR/summary.tsv"
      REPORT="$LOG_DIR/report.md"
      echo -e "bus\tmode\tpublished\treceived\tpublish_errors\treconnects\tgaps" > "$SUMMARY"

      if curl -s --max-time 5 "$PROM_URL/-/ready" >/dev/null 2>&1; then
        # Join the per-(bus,mode) series into one row each via awk.
        {
          report_metric mbclient_published_total      | sed 's/^/PUB\t/'
          report_metric mbclient_received_total       | sed 's/^/REC\t/'
          report_metric mbclient_publish_errors_total | sed 's/^/ERR\t/'
          report_metric mbclient_reconnects_total     | sed 's/^/RCN\t/'
          report_metric mbclient_receive_gaps_total   | sed 's/^/GAP\t/'
        } | awk -F'\t' '
          { key=$2" "$3; kind[$2" "$3]=1
            if($1=="PUB") pub[key]=$4; else if($1=="REC") rec[key]=$4;
            else if($1=="ERR") err[key]=$4; else if($1=="RCN") rcn[key]=$4;
            else if($1=="GAP") gap[key]=$4 }
          END{ for(k in kind){ split(k,a," ");
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", a[1], a[2],
              (pub[k]==""?0:pub[k]), (rec[k]==""?0:rec[k]),
              (err[k]==""?0:err[k]), (rcn[k]==""?0:rcn[k]), (gap[k]==""?0:gap[k]) } }
        ' | sort >> "$SUMMARY"
        PROM_NOTE="Metrics queried from Prometheus at $PROM_URL."
      else
        PROM_NOTE="Prometheus at $PROM_URL was unreachable — per-client metrics unavailable (deploy the monitoring stack). Raw client logs are in $LOG_DIR."
      fi

      demos_pass=$(grep -c $'\tpass$' "$LOG_DIR/demos.tsv" 2>/dev/null || echo 0)
      demos_fail=$(grep -c $'\tfail$' "$LOG_DIR/demos.tsv" 2>/dev/null || echo 0)
      faults_n=$(( $(wc -l < "$LOG_DIR/events.tsv") - 1 ))

      {
        echo "# Soak report"
        echo
        echo "- Started: $(date -d "@$START" 2>/dev/null || date)"
        echo "- Duration: $DURATION ($DUR_SECS s), rate: $RATE per publisher"
        echo "- Faults: $([ "$NO_FAULTS" = 1 ] && echo 'none' || echo "$NODES, every ''${FAULT_INTERVAL}s (down ''${FAULT_DOWN}s)")"
        echo "- Fault events logged: $faults_n"
        echo "- NATS demos: $demos_pass pass / $demos_fail fail"
        echo
        echo "$PROM_NOTE"
        echo
        echo '## Per-client totals'
        echo
        echo '| bus | mode | published | received | pub_errors | reconnects | gaps |'
        echo '|-----|------|-----------|----------|-----------|------------|------|'
        tail -n +2 "$SUMMARY" | awk -F'\t' '{printf "| %s | %s | %s | %s | %s | %s | %s |\n",$1,$2,$3,$4,$5,$6,$7}'
        echo
        echo "Grafana dashboard: http://${constants.network.ipv4.cp0}:${toString mon.grafana.nodePort} (uid: soak)"
      } > "$REPORT"

      log "done — report:"
      cat "$REPORT"
    '';
  };
}
