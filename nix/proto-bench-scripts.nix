# nix/proto-bench-scripts.nix
#
# `k8s-proto-bench` — the host-side harness for the proto-bench benchmark
# (design §9.3). It drives `benchcli` against the four region-agents over their
# NodePorts, assembles a `run.json` (the §8.5 record), and renders it to
# results.tsv / results.md / hgrm via `benchcli report`.
#
# Flow: (1) preflight — cluster reachable, agents + brokers Ready, agent image
# tag matches constants; (2) clock probes per region; (3) build + seed-shuffle
# the cell matrix and write the run.json skeleton; (4) per GC profile: patch the
# agent env (`kubectl set env`) and wait rollout, then per cell × repeat post a
# Grafana annotation, run benchcli under `taskset`, and append the cell record
# (enriched with the agent's GC columns from Prometheus) to run.json; (5) render.
# `fault` cells additionally kill the target pod (the driven region's agent for
# gRPC, the broker's pod-0 for a bus) partway through the window and post a
# `fault`-tagged annotation, then wait for it to recover before the next repeat
# (design §8.2 — reuses the chaos idea from nix/chaos-scripts.nix).
#
#   nix run .#k8s-proto-bench                         # curated default matrix
#   nix run .#k8s-proto-bench -- --dry-run            # print the plan, run nothing
#   nix run .#k8s-proto-bench -- --transports grpc --modes latency,openloop
#
# Like `k8s-client-bench` this is "report, don't assert": a cell that errors, an
# unreachable Prometheus, or a missing credential leaves columns blank rather
# than aborting the run. It needs the cluster up with the monitoring stack and
# the region-agents deployed (P2/P3), and SSH to cp0 for bus credentials.
#
{ pkgs }:
let
  lib       = pkgs.lib;
  constants = import ./constants.nix;
  vmScripts = import ./microvm-scripts.nix { inherit pkgs; };
  clients   = import ./clients.nix { inherit pkgs; };
  mb  = constants.messageBus;
  mon = constants.monitoring;
  wl  = mb.workloads;
  pb  = constants.protoBench;
  net = constants.network.ipv4;

  # region → node index in [cp0,cp1,cp2,w3]: gives the per-region gRPC NodePort
  # (nodePortGrpcBase + index, externalTrafficPolicy: Local) and the node IP the
  # connection must land on for the "Local" policy to reach that region's pod.
  nodeOrder  = [ "cp0" "cp1" "cp2" "w3" ];
  regionRows = lib.imap0 (i: node: {
    region = wl.regions.${node};
    ip = net.${node};
    grpcPort = wl.nodePortGrpcBase + i;
    node = node;
  }) nodeOrder;
  mkCase = f: lib.concatMapStrings (r: "        ${r.region}) echo '${toString (f r)}' ;;\n") regionRows;
in
{
  protoBench = pkgs.writeShellApplication {
    name = "k8s-proto-bench";
    runtimeInputs = with pkgs; [
      coreutils gawk gnused curl jq procps util-linux git
      vmScripts.ssh
      clients.package
    ];
    text = ''
      # Not -e: one cell (or Prometheus, or a credential) failing must not abort
      # the whole matrix — blanks in run.json are valid (design §8.4).
      set -uo pipefail

      # ─── Defaults (curated so a run stays well under the §15 90-min budget) ─
      TRANSPORTS="grpc,nats"
      CODECS="proto,vtproto"
      FIXTURES="small,medium"
      POOLS="none,all"
      GCS="default"
      MODES="latency"
      REGIONS="us-west-2"
      RATE="${pb.defaults.rate}"                 # e.g. 2000/s (openloop/saturation)
      DURATION="${pb.defaults.duration}"         # open-loop wall-clock budget
      WARMUP="${pb.defaults.warmup}"             # settle after each rollout
      REPEATS=${toString pb.defaults.repeats}
      COUNT=10000                                # closed-loop (latency/windowed) request count
      INFLIGHT=${toString pb.defaults.inflight}  # concurrency cap (windowed/openloop)
      INTEGRITY="none"
      ORDER_SEED=7
      CPUSET=""                                  # taskset core list; empty = no pinning (design §8.3)
      LOG_DIR="${pb.defaults.logDir}"
      DRY_RUN=0

      CP0_IP="${net.cp0}"
      PROM_URL="http://${net.cp0}:${toString mon.prometheus.nodePort}"
      GRAFANA_URL="http://${net.cp0}:${toString mon.grafana.nodePort}"

      NATS_ADDR="$CP0_IP:${toString mb.nats.nodePort}"
      RABBITMQ_ADDR="$CP0_IP:${toString mb.rabbitmq.nodePortAmqp}"
      MQTT_ADDR="$CP0_IP:${toString mb.mqtt.nodePort}"
      VK_SENTINELS="$CP0_IP:${toString (mb.valkey.nodePortSentinelBase + 0)},$CP0_IP:${toString (mb.valkey.nodePortSentinelBase + 1)},$CP0_IP:${toString (mb.valkey.nodePortSentinelBase + 2)}"

      # region → gRPC endpoint (node IP + per-region NodePort) and node name.
      region_ip()   { case "$1" in
${mkCase (r: r.ip)}        *) return 1 ;; esac; }
      region_gport() { case "$1" in
${mkCase (r: r.grpcPort)}        *) return 1 ;; esac; }
      region_node() { case "$1" in
${mkCase (r: r.node)}        *) return 1 ;; esac; }

      usage() {
        cat <<EOF
Usage: k8s-proto-bench [OPTIONS]

Drive benchcli across the region-agents, assemble run.json, and render
results.tsv / results.md / hgrm (design §9.3). Report, don't assert.

Matrix (comma-separated subsets):
  --transports=LIST  grpc,nats,rabbitmq,valkey,mqtt      (default: $TRANSPORTS)
  --codecs=LIST      proto,protojson,vtproto             (default: $CODECS)
  --fixtures=LIST    tiny,small,medium,large,…           (default: $FIXTURES)
  --pools=LIST       none,messages,buffers,all           (default: $POOLS)
  --gc=LIST          default,limit                       (default: $GCS)
  --modes=LIST       latency,windowed,openloop,saturation,coldstart,fault (default: $MODES)
                     (fault kills the target pod mid-window; opt-in only)
  --regions=LIST     region label(s); cells drive the FIRST, all are clock-probed
                                                         (default: $REGIONS)
Run knobs:
  --rate=R/s         offered load for openloop/saturation (default: $RATE)
  --duration=D       open-loop wall-clock budget          (default: $DURATION)
  --count=N          closed-loop request count            (default: $COUNT)
  --inflight=N       windowed/openloop concurrency cap     (default: $INFLIGHT)
  --warmup=D         settle after each GC-profile rollout  (default: $WARMUP)
  --repeats=N        repeats per cell                      (default: $REPEATS)
  --order-seed=N     cell-shuffle seed                     (default: $ORDER_SEED)
  --cpuset=LIST      taskset core list for the driver (e.g. 8-11; empty = none)
  --integrity=none|sha256                                 (default: $INTEGRITY)
  --log-dir=DIR      output root                           (default: $LOG_DIR)
  --dry-run          print the plan and exit
  -h, --help         show this help

Requires: cluster up with the monitoring stack + region-agents deployed; SSH to
cp0 for bus credentials. gRPC cells hit the region's own node (NodePort policy
Local); bus cells hit cp0 and select the responder with -region.
EOF
      }

      while [[ $# -gt 0 ]]; do
        case "$1" in
          --transports=*) TRANSPORTS="''${1#*=}" ;;   --transports) TRANSPORTS="''${2-}"; shift ;;
          --codecs=*)     CODECS="''${1#*=}" ;;        --codecs)     CODECS="''${2-}"; shift ;;
          --fixtures=*)   FIXTURES="''${1#*=}" ;;      --fixtures)   FIXTURES="''${2-}"; shift ;;
          --pools=*)      POOLS="''${1#*=}" ;;         --pools)      POOLS="''${2-}"; shift ;;
          --gc=*)         GCS="''${1#*=}" ;;           --gc)         GCS="''${2-}"; shift ;;
          --modes=*)      MODES="''${1#*=}" ;;         --modes)      MODES="''${2-}"; shift ;;
          --regions=*)    REGIONS="''${1#*=}" ;;       --regions)    REGIONS="''${2-}"; shift ;;
          --rate=*)       RATE="''${1#*=}" ;;          --rate)       RATE="''${2-}"; shift ;;
          --duration=*)   DURATION="''${1#*=}" ;;      --duration)   DURATION="''${2-}"; shift ;;
          --count=*)      COUNT="''${1#*=}" ;;         --count)      COUNT="''${2-}"; shift ;;
          --inflight=*)   INFLIGHT="''${1#*=}" ;;      --inflight)   INFLIGHT="''${2-}"; shift ;;
          --warmup=*)     WARMUP="''${1#*=}" ;;        --warmup)     WARMUP="''${2-}"; shift ;;
          --repeats=*)    REPEATS="''${1#*=}" ;;       --repeats)    REPEATS="''${2-}"; shift ;;
          --order-seed=*) ORDER_SEED="''${1#*=}" ;;    --order-seed) ORDER_SEED="''${2-}"; shift ;;
          --cpuset=*)     CPUSET="''${1#*=}" ;;        --cpuset)     CPUSET="''${2-}"; shift ;;
          --integrity=*)  INTEGRITY="''${1#*=}" ;;     --integrity)  INTEGRITY="''${2-}"; shift ;;
          --log-dir=*)    LOG_DIR="''${1#*=}" ;;       --log-dir)    LOG_DIR="''${2-}"; shift ;;
          --dry-run)      DRY_RUN=1 ;;
          -h|--help)      usage; exit 0 ;;
          *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
        esac
        shift
      done

      log()   { echo "[proto-bench] $(date +%H:%M:%S) $*"; }
      kexec() { k8s-vm-ssh --node=cp0 env KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig "$@"; }
      RATE_NUM="''${RATE%/s}"                          # benchcli -rate wants a bare number
      DRIVE_REGION="''${REGIONS%%,*}"                  # cells drive the first region

      # ─── Build the cell matrix (transport/codec/fixture/pool/gc/mode) ──
      # The cell id (design §8.5) is transport/codec/fixture/pool/gc/mode —
      # region is a driver choice, not part of the id. Built before preflight so
      # --dry-run needs no cluster.
      declare -a CELLS=()
      IFS=',' read -ra T <<< "$TRANSPORTS"; IFS=',' read -ra C <<< "$CODECS"
      IFS=',' read -ra F <<< "$FIXTURES";   IFS=',' read -ra P <<< "$POOLS"
      IFS=',' read -ra G <<< "$GCS";        IFS=',' read -ra M <<< "$MODES"
      for t in "''${T[@]}"; do for c in "''${C[@]}"; do for f in "''${F[@]}"; do
        for p in "''${P[@]}"; do for g in "''${G[@]}"; do for m in "''${M[@]}"; do
          if [ "$t" = "mqtt" ] && [ "$m" != "latency" ]; then continue; fi  # mqtt is latency-only
          CELLS+=("$t/$c/$f/$p/$g/$m")
        done; done; done
      done; done; done

      # Deterministic seed-controlled shuffle: order by sha256(seed:cell).
      mapfile -t CELLS < <(
        for cell in "''${CELLS[@]}"; do
          printf '%s\t%s\n' "$(printf '%s:%s' "$ORDER_SEED" "$cell" | sha256sum | cut -c1-16)" "$cell"
        done | sort | cut -f2
      )
      log "matrix: ''${#CELLS[@]} cells × $REPEATS repeats, drive region=$DRIVE_REGION, order-seed=$ORDER_SEED"

      if [ "$DRY_RUN" = 1 ]; then
        printf '  %s\n' "''${CELLS[@]}"
        log "dry-run: nothing executed"; exit 0
      fi

      # ─── (1) Preflight ───────────────────────────────────────────────
      log "preflight: cluster + agents + brokers"
      if ! kexec kubectl get --raw='/readyz' >/dev/null 2>&1; then
        echo "FATAL: cluster not reachable via cp0 (is it up?)" >&2; exit 1
      fi
      NOT_READY="$(kexec kubectl -n ${wl.namespace} get pods \
        -l app.kubernetes.io/name=region-agent \
        -o jsonpath='{range .items[*]}{.metadata.name}{"="}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null \
        | grep -v '=True$' || true)"
      if [ -n "$NOT_READY" ]; then
        log "WARN: some region-agents are not Ready:"; echo "$NOT_READY" >&2
      fi
      # Agent image tag vs the tag benchcli/agent were built at (constants).
      WANT_TAG="${wl.tag}"
      GOT_TAG="$(kexec kubectl -n ${wl.namespace} get deploy region-agent-"$DRIVE_REGION" \
        -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null | sed 's/.*://')"
      if [ -n "$GOT_TAG" ] && [ "$GOT_TAG" != "$WANT_TAG" ]; then
        log "WARN: agent image tag $GOT_TAG != expected $WANT_TAG (results may not match this binary)"
      fi

      PROM_OK=0
      curl -s --max-time 5 "$PROM_URL/-/ready" >/dev/null 2>&1 && PROM_OK=1
      [ "$PROM_OK" = 1 ] || log "WARN: Prometheus unreachable at $PROM_URL — agent GC columns will be blank"

      log "reading bus credentials from cluster"
      RABBITMQ_PASS="$(kexec kubectl -n ${mb.rabbitmq.namespace} get secret rabbitmq-credentials \
        -o jsonpath='{.data.RABBITMQ_DEFAULT_PASS}' 2>/dev/null | base64 -d || true)"
      VALKEY_PASS="$(kexec kubectl -n ${mb.valkey.namespace} get secret valkey-credentials \
        -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"

      # ─── Run directory + IDs ─────────────────────────────────────────
      RUN_ID="$(date +%Y%m%dT%H%M%S)-$(printf '%04x' $((RANDOM)))"
      RUN_DIR="$LOG_DIR/$RUN_ID"
      mkdir -p "$RUN_DIR/hgrm" "$RUN_DIR/cells"
      RUN_JSON="$RUN_DIR/run.json"
      GIT_REV="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
      GIT_DIRTY=false; [ -n "$(git status --porcelain 2>/dev/null || true)" ] && GIT_DIRTY=true

      # ─── run.json skeleton ───────────────────────────────────────────
      jq -n \
        --arg run_id "$RUN_ID" --arg started "$(date -Is)" \
        --arg rev "$GIT_REV" --argjson dirty "$GIT_DIRTY" \
        --argjson seed "$ORDER_SEED" --argjson repeats "$REPEATS" \
        --arg cpuset "$CPUSET" --arg kernel "$(uname -r)" \
        --argjson cellids "$(printf '%s\n' "''${CELLS[@]}" | jq -R . | jq -s .)" \
        '{run_id:$run_id, started_at:$started, finished_at:"",
          git:{rev:$rev, dirty:$dirty},
          versions:{go:"${pkgs.go.version}", protobuf:"${pkgs.protobuf.version}", buf:"${pkgs.buf.version}",
                    "nats-server":"${mb.nats.tag}", rabbitmq:"${mb.rabbitmq.tag}", valkey:"${mb.valkey.tag}", mosquitto:"${mb.mqtt.tag}"},
          host:{driver_cpuset:$cpuset, kernel:$kernel},
          gc_profiles:{default:{GOGC:"100"}, limit:{GOGC:"off", GOMEMLIMIT:"384MiB"}},
          clock:{}, corpus:{seed:42},
          matrix:{order_seed:$seed, repeats:$repeats, cells:$cellids},
          cells:[]}' > "$RUN_JSON"

      # ─── (2) Clock probes per region → run.json .clock ───────────────
      for region in ''${REGIONS//,/ }; do
        rip="$(region_ip "$region" || true)"; rgp="$(region_gport "$region" || true)"
        [ -n "$rip" ] || { log "WARN: unknown region $region — skipping clock probe"; continue; }
        if probe="$(benchcli clockprobe -json -addr "$rip:$rgp" 2>/dev/null)"; then
          tmp="$(mktemp)"
          jq --arg r "$region" --argjson p "$probe" \
            '.clock[$r]={offset_ns:$p.offset_ns, uncertainty_ns:$p.uncertainty_ns, source:$p.clock_source, synced:$p.chrony_synced}' \
            "$RUN_JSON" > "$tmp" && mv "$tmp" "$RUN_JSON"
          log "clock $region: offset $(jq -r --arg r "$region" '.clock[$r].offset_ns' "$RUN_JSON")ns synced=$(jq -r --arg r "$region" '.clock[$r].synced' "$RUN_JSON")"
        else
          log "WARN: clock probe failed for $region"
        fi
      done

      # prom_scalar QUERY [TIME] → single scalar (or "" on miss / Prom down).
      prom_scalar() {
        [ "$PROM_OK" = 1 ] || { echo ""; return; }
        local -a at=()
        [ -n "''${2-}" ] && at=(--data-urlencode "time=$2")
        curl -s --max-time 5 "$PROM_URL/api/v1/query" \
          --data-urlencode "query=$1" "''${at[@]}" 2>/dev/null \
          | jq -r '.data.result[0].value[1] // ""' 2>/dev/null
      }

      # annotate T_START_MS T_END_MS CELL REPEAT — best-effort Grafana region
      # annotation (design §12.4); anonymous-admin Grafana needs no token.
      annotate() {
        curl -s --max-time 5 -XPOST "$GRAFANA_URL/api/annotations" \
          -H 'Content-Type: application/json' \
          -d "$(jq -n --argjson t "$1" --argjson te "$2" --arg id "$3" --arg run "$RUN_ID" --argjson rep "$4" \
                '{dashboardUID:"protobench", time:$t, timeEnd:$te, tags:["protobench","cell:\($id)","run:\($run)"], text:"\($id) repeat \($rep)"}')" \
          >/dev/null 2>&1 || true
      }

      # ─── fault-mode pod-kill orchestration (design §8.2) ─────────────
      # fault_kill_delay: how long into the window to wait before the kill, so
      # the cell captures a pre-fault baseline then the disruption + recovery.
      # ~1/3 of the open-loop duration (min 1s); parses s/m suffixes.
      fault_kill_delay() {
        local d="$DURATION" secs
        case "$d" in
          *s) secs="''${d%s}" ;;
          *m) secs=$(( ''${d%m} * 60 )) ;;
          *)  secs="$d" ;;
        esac
        [[ "$secs" =~ ^[0-9]+$ ]] || secs=15
        local k=$(( secs / 3 )); [ "$k" -lt 1 ] && k=1; echo "$k"
      }

      # fault_target TRANSPORT → sets FAULT_NS + FAULT_TARGET[] (kubectl operand
      # for the pod to kill): the driven region's agent for gRPC, the broker's
      # pod-0 for a bus. Returns 1 for transports with no fault target (mqtt).
      fault_target() {
        case "$1" in
          grpc)     FAULT_NS="${wl.namespace}";          FAULT_TARGET=(pod -l "app=region-agent-$DRIVE_REGION") ;;
          nats)     FAULT_NS="${mb.nats.namespace}";     FAULT_TARGET=(pod/nats-0) ;;
          rabbitmq) FAULT_NS="${mb.rabbitmq.namespace}"; FAULT_TARGET=(pod/rabbitmq-0) ;;
          valkey)   FAULT_NS="${mb.valkey.namespace}";   FAULT_TARGET=(pod/valkey-0) ;;
          *) return 1 ;;
        esac
      }

      # fault_kill TRANSPORT — delete the target pod (best-effort, non-blocking)
      # and post a fault-tagged point annotation at the kill instant.
      fault_kill() {
        local FAULT_NS; local -a FAULT_TARGET=()
        fault_target "$1" || { log "WARN: no fault target for $1"; return 0; }
        local now; now=$(date +%s%3N)
        log "fault: killing $1 target (''${FAULT_TARGET[*]}) in ns=$FAULT_NS"
        kexec kubectl -n "$FAULT_NS" delete "''${FAULT_TARGET[@]}" --wait=false >/dev/null 2>&1 || \
          log "WARN: fault kill failed for $1"
        curl -s --max-time 5 -XPOST "$GRAFANA_URL/api/annotations" \
          -H 'Content-Type: application/json' \
          -d "$(jq -n --argjson t "$now" --arg run "$RUN_ID" --arg txt "fault kill: $1 ''${FAULT_TARGET[*]}" \
                '{dashboardUID:"protobench", time:$t, tags:["protobench","fault","run:\($run)"], text:$txt}')" \
          >/dev/null 2>&1 || true
      }

      # fault_recover TRANSPORT — wait (bounded, best-effort) for the killed
      # target to come back Ready, so the next repeat starts from a clean state.
      fault_recover() {
        case "$1" in
          grpc)     kexec kubectl -n ${wl.namespace} rollout status deploy/region-agent-"$DRIVE_REGION" --timeout=60s >/dev/null 2>&1 || true ;;
          nats)     kexec kubectl -n ${mb.nats.namespace} wait --for=condition=Ready pod/nats-0 --timeout=60s >/dev/null 2>&1 || true ;;
          rabbitmq) kexec kubectl -n ${mb.rabbitmq.namespace} wait --for=condition=Ready pod/rabbitmq-0 --timeout=60s >/dev/null 2>&1 || true ;;
          valkey)   kexec kubectl -n ${mb.valkey.namespace} wait --for=condition=Ready pod/valkey-0 --timeout=90s >/dev/null 2>&1 || true ;;
        esac
      }

      # ─── benchcli invocation for one cell/repeat ─────────────────────
      # Emits the per-cell record to cells/<id>-<rep>.json and the histogram to
      # hgrm/<id>-<rep>.hgrm (paths relative to RUN_DIR). Returns benchcli's rc.
      run_cell() {
        local transport="$1" codec="$2" fixture="$3" pool="$4" gc="$5" mode="$6" repeat="$7"
        local safe="''${transport}-''${codec}-''${fixture}-''${pool}-''${gc}-''${mode}-''${repeat}"
        local outjson="cells/$safe.json" hgrm="hgrm/$safe.hgrm"

        # Mode-specific knobs. latency/coldstart are closed at inflight 1 with -n;
        # windowed/openloop/saturation carry inflight/rate/duration.
        local -a modeargs=()
        case "$mode" in
          latency)             modeargs=(-n "$COUNT") ;;
          coldstart)           modeargs=() ;;   # -conns default (20)
          windowed)            modeargs=(-n "$COUNT" -inflight "$INFLIGHT") ;;
          openloop)            modeargs=(-rate "$RATE_NUM" -duration "$DURATION" -inflight "$INFLIGHT") ;;
          saturation)          modeargs=(-rate "$RATE_NUM" -duration "$DURATION") ;;
          fault)               modeargs=(-rate "$RATE_NUM" -duration "$DURATION" -inflight "$INFLIGHT") ;;
          *) log "WARN: unknown mode $mode — skipping"; return 1 ;;
        esac

        local -a common=(-codec "$codec" -fixture "$fixture" -pool "$pool" -gc "$gc"
          -mode "$mode" -region "$DRIVE_REGION" -run-id "$RUN_ID"
          -out "$outjson" -hgrm "$hgrm" -repeat "$repeat")

        local -a cmd=()
        case "$transport" in
          grpc)
            local rip rgp; rip="$(region_ip "$DRIVE_REGION")"; rgp="$(region_gport "$DRIVE_REGION")"
            cmd=(benchcli grpc -addr "$rip:$rgp" "''${common[@]}" "''${modeargs[@]}") ;;
          nats)
            cmd=(benchcli nats -addr "$NATS_ADDR" "''${common[@]}" "''${modeargs[@]}") ;;
          rabbitmq)
            cmd=(benchcli rabbitmq -addr "$RABBITMQ_ADDR" -pass "$RABBITMQ_PASS" "''${common[@]}" "''${modeargs[@]}") ;;
          valkey)
            cmd=(benchcli valkey -sentinels "$VK_SENTINELS" -pass "$VALKEY_PASS" "''${common[@]}" "''${modeargs[@]}") ;;
          mqtt)
            cmd=(benchcli mqtt -addr "$MQTT_ADDR" "''${common[@]}" "''${modeargs[@]}") ;;
          *) log "WARN: unknown transport $transport — skipping"; return 1 ;;
        esac
        # taskset-pin the driver (design §8.3); GOMAXPROCS matches the core count.
        if [ -n "$CPUSET" ]; then
          local ncores; ncores="$(taskset -c "$CPUSET" nproc 2>/dev/null || echo 0)"
          [ "$ncores" -gt 0 ] 2>/dev/null && export GOMAXPROCS="$ncores"
          cmd=(taskset -c "$CPUSET" "''${cmd[@]}")
        fi
        if [ "$mode" = "fault" ]; then
          # Open-loop pass held straight through a pod kill: run benchcli in the
          # background, kill the target pod partway through the window, and let
          # the driver keep sending (it never aborts — the lost requests become
          # the `missing` integrity counter, design §8.2/§8.4). Then wait for the
          # driver to finish and for the target to recover before the next repeat.
          ( cd "$RUN_DIR" && "''${cmd[@]}" ) >>"$RUN_DIR/cells/$safe.log" 2>&1 &
          local bench_pid=$!
          sleep "$(fault_kill_delay)"
          fault_kill "$transport"
          local rc=0; wait "$bench_pid" || rc=$?
          fault_recover "$transport"
          return "$rc"
        fi
        ( cd "$RUN_DIR" && "''${cmd[@]}" ) >>"$RUN_DIR/cells/$safe.log" 2>&1
      }

      # ─── (4) Execute, grouped by GC profile (patch env once per profile) ──
      declare -A did_patch=()
      log "run $RUN_ID → $RUN_DIR"
      for gc in ''${GCS//,/ }; do
        # Patch the driven-region agent's GC env, then wait rollout (once).
        if [ "$gc" = "limit" ]; then ENV_ARGS=(GOGC=off GOMEMLIMIT=384MiB); else ENV_ARGS=(GOGC=100 GOMEMLIMIT-); fi
        log "gc=$gc: patching region-agent-$DRIVE_REGION env (''${ENV_ARGS[*]})"
        kexec kubectl -n ${wl.namespace} set env deploy/region-agent-"$DRIVE_REGION" "''${ENV_ARGS[@]}" >/dev/null 2>&1 || \
          log "WARN: kubectl set env failed for gc=$gc"
        kexec kubectl -n ${wl.namespace} rollout status deploy/region-agent-"$DRIVE_REGION" --timeout=120s >/dev/null 2>&1 || \
          log "WARN: rollout for gc=$gc did not complete"
        did_patch[$gc]=1
        log "gc=$gc: warmup $WARMUP"; sleep "''${WARMUP%s}" 2>/dev/null || sleep 5

        for cell in "''${CELLS[@]}"; do
          IFS='/' read -r ct cc cf cp cgc cm <<< "$cell"
          [ "$cgc" = "$gc" ] || continue
          for ((rep=0; rep<REPEATS; rep++)); do
            safe="''${ct}-''${cc}-''${cf}-''${cp}-''${cgc}-''${cm}-''${rep}"
            t0=$(date +%s%3N)
            run_cell "$ct" "$cc" "$cf" "$cp" "$cgc" "$cm" "$rep" || \
              log "WARN: cell $cell repeat $rep exited non-zero (see cells/$safe.log)"
            t1=$(date +%s%3N)
            annotate "$t0" "$t1" "$cell" "$rep"

            cellfile="$RUN_DIR/cells/$safe.json"
            [ -f "$cellfile" ] || { log "WARN: no record for $cell repeat $rep"; continue; }

            # Enrich with the agent's GC columns from Prometheus over the cell
            # window (design §8.4; blank if Prom down). agent_region label is set
            # by the workloads-agents scrape job. A cell window is only a few
            # seconds — shorter than the scrape interval — so rate(counter[win])
            # has too few points to resolve. Instead read each counter at the
            # cell's start and end instants (an instant query returns the latest
            # sample within Prometheus's lookback) and take the delta: robust for
            # short windows, and exact (a whole-window increase, not a rate est).
            t0s=$(( t0 / 1000 )); t1s=$(( t1 / 1000 )); win=$(( t1s - t0s )); [ "$win" -lt 1 ] && win=1
            gccpu0="$(prom_scalar "go_cpu_classes_gc_total_cpu_seconds_total{agent_region=\"$DRIVE_REGION\"}" "$t0s")"
            gccpu1="$(prom_scalar "go_cpu_classes_gc_total_cpu_seconds_total{agent_region=\"$DRIVE_REGION\"}" "$t1s")"
            gctot0="$(prom_scalar "go_cpu_classes_total_cpu_seconds_total{agent_region=\"$DRIVE_REGION\"}" "$t0s")"
            gctot1="$(prom_scalar "go_cpu_classes_total_cpu_seconds_total{agent_region=\"$DRIVE_REGION\"}" "$t1s")"
            gccyc0="$(prom_scalar "go_gc_cycles_automatic_gc_cycles_total{agent_region=\"$DRIVE_REGION\"}" "$t0s")"
            gccyc1="$(prom_scalar "go_gc_cycles_automatic_gc_cycles_total{agent_region=\"$DRIVE_REGION\"}" "$t1s")"
            # GC CPU fraction = Δ(gc cpu-seconds) / Δ(total cpu-seconds) over the window.
            gccpu="$(awk -v a="$gccpu0" -v b="$gccpu1" -v c="$gctot0" -v d="$gctot1" \
              'BEGIN{ if(a!=""&&b!=""&&c!=""&&d!=""){ dt=d-c; if(dt>0) printf "%.6f",(b-a)/dt } }')"
            # GC cycles/s = Δ(automatic gc cycles) / window seconds.
            gccyc="$(awk -v a="$gccyc0" -v b="$gccyc1" -v w="$win" \
              'BEGIN{ if(a!=""&&b!=""){ printf "%.4f",(b-a)/w } }')"
            tmp="$(mktemp)"
            jq --arg cpu "$gccpu" --arg cyc "$gccyc" \
              '(if ($cpu|length)>0 then .summary.agent_gc_cpu_fraction=($cpu|tonumber) else . end)
               | (if ($cyc|length)>0 then .summary.agent_gc_cycles_per_s=($cyc|tonumber) else . end)' \
              "$cellfile" > "$tmp" && mv "$tmp" "$cellfile"

            tmp="$(mktemp)"
            jq --slurpfile c "$cellfile" '.cells += $c' "$RUN_JSON" > "$tmp" && mv "$tmp" "$RUN_JSON"
          done
        done
      done

      # Restore the agent to the baseline GC profile so the cluster is left clean.
      if [ -n "''${did_patch[limit]:-}" ]; then
        log "restoring region-agent-$DRIVE_REGION to GOGC=100"
        kexec kubectl -n ${wl.namespace} set env deploy/region-agent-"$DRIVE_REGION" GOGC=100 GOMEMLIMIT- >/dev/null 2>&1 || true
      fi

      # Ensure no driver outlives the harness.
      pkill -x benchcli 2>/dev/null || true

      tmp="$(mktemp)"; jq --arg f "$(date -Is)" '.finished_at=$f' "$RUN_JSON" > "$tmp" && mv "$tmp" "$RUN_JSON"

      # ─── (6) Render ──────────────────────────────────────────────────
      log "rendering results.tsv / results.md"
      benchcli report -run "$RUN_JSON" || log "WARN: report render failed"

      # ─── (7) Summary ─────────────────────────────────────────────────
      log "done — run $RUN_ID"
      echo "  run.json     $RUN_JSON"
      echo "  results.tsv  $RUN_DIR/results.tsv"
      echo "  results.md   $RUN_DIR/results.md"
      echo "  hgrm/        $RUN_DIR/hgrm/"
      [ -f "$RUN_DIR/results.md" ] && { echo; sed -n '1,40p' "$RUN_DIR/results.md"; }
    '';
  };
}
