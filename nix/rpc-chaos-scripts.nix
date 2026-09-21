# nix/rpc-chaos-scripts.nix
#
# `k8s-rpc-chaos` — the §28 failure-injection harness for the RPC lab. It drives
# an open-loop rpc-benchmark run through the same two-gateway topology as
# `k8s-rpc-bench`:
#
#   host rpc-benchmark → gateway-A (host) → gateway-B (NodePort) → rpc-service
#
# and, partway through the run (at duration/3, the proto-bench fault cadence),
# injects one of the §28 failure scenarios, then measures how the run rides
# through the outage and recovers. It integrates with the repository's existing
# chaos approach rather than inventing a new one: it reuses rpc-scripts.nix's
# gateway-A lifecycle and preflight, and proto-bench-scripts.nix's pod-kill /
# recover / duration÷3 orchestration.
#
# Scenarios (--scenario):
#   backend     kill the in-cluster rpc-service pod (§28 "Backend unavailable" /
#               "Gateway B failure": gateway-B's backend vanishes; calls get
#               UNAVAILABLE until the Deployment reschedules). With --retries the
#               run recovers; without, it shows the raw failure window.
#   gateway-b   kill the in-cluster rpc-gateway pod (§28 "Gateway B failure": the
#               ingress gateway-A forwards to disappears mid-request).
#   gateway-a   kill and respawn the host gateway-A child (§28 "Gateway A failure
#               after request": the caller-side hop crashes; a synchronous gRPC
#               response in flight is lost/orphaned — no broker durability — and
#               --retries re-issues once gateway-A is back).
#
#   nix run .#k8s-rpc-chaos                                   # backend, defaults
#   nix run .#k8s-rpc-chaos -- --scenario gateway-b --retries 3
#   nix run .#k8s-rpc-chaos -- --scenario gateway-a --duration 30s
#   nix run .#k8s-rpc-chaos -- --dry-run                      # print the plan only
#
# It needs the cluster up with the rpc gateway + service deployed and the images
# preloaded (`nix run .#k8s-image-import`). Like the other harnesses it reaches
# the nodes through the k8s-vm-ssh wrapper, never raw ssh.
{ pkgs }:
let
  constants = import ./constants.nix;
  vmScripts = import ./microvm-scripts.nix { inherit pkgs; };
  clients   = import ./clients.nix { inherit pkgs; };
  rpc = constants.messageBus.rpc;
  net = constants.network.ipv4;
in
{
  rpcChaos = pkgs.writeShellApplication {
    name = "k8s-rpc-chaos";
    runtimeInputs = with pkgs; [
      coreutils gawk gnused jq procps
      vmScripts.ssh
      clients.package
    ];
    text = ''
      # Not -e: a fault-injected run is expected to log failures; the trap tears
      # down the host gateway-A child on any exit.
      set -uo pipefail

      NODE="cp0"
      SCENARIO="backend"
      RATE=2000
      DURATION="15s"
      TIMEOUT="5s"
      RETRIES=3                # §29 safe retry, so a transient outage is ridden through
      LOCAL_PORT=9432          # host gateway-A listen port (distinct from k8s-rpc-bench's 9431)
      METRICS=0                # expose rpc_* /metrics (§22)
      OUT=""                   # write a run.json here (§35)
      LINGER="10s"             # hold /metrics open after the run for a final scrape
      DRY_RUN=0

      usage() {
        cat <<'EOF'
      k8s-rpc-chaos — inject a §28 failure mid-run and measure ride-through / recovery

      Options:
        --node <n>           node whose NodePort to reach gateway-B on (default cp0)
        --scenario <s>       backend | gateway-b | gateway-a (default backend)
        --rate <n>           offered req/s, open loop (default 2000)
        --duration <d>       wall-clock budget; the fault fires at duration/3 (default 15s)
        --timeout <d>        per-call timeout (default 5s)
        --retries <n>        §29 retries per call so a transient outage recovers (default 3)
        --local-port <p>     host gateway-A listen port (default 9432)
        --metrics            expose rpc_* Prometheus metrics on localhost:${toString rpc.benchMetricsPort} (§22)
        --metrics-linger <d> hold /metrics open after the run (default 10s, with --metrics)
        --out <path>         write a reproducible run.json here (§35)
        --dry-run            print the plan and exit
        -h, --help           this help
      EOF
      }

      while [ $# -gt 0 ]; do
        case "$1" in
          --node) NODE="$2"; shift 2 ;;
          --scenario) SCENARIO="$2"; shift 2 ;;
          --rate) RATE="$2"; shift 2 ;;
          --duration) DURATION="$2"; shift 2 ;;
          --timeout) TIMEOUT="$2"; shift 2 ;;
          --retries) RETRIES="$2"; shift 2 ;;
          --local-port) LOCAL_PORT="$2"; shift 2 ;;
          --metrics) METRICS=1; shift ;;
          --metrics-linger) LINGER="$2"; shift 2 ;;
          --out) OUT="$2"; shift 2 ;;
          --dry-run) DRY_RUN=1; shift ;;
          -h|--help) usage; exit 0 ;;
          *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
        esac
      done

      case "$SCENARIO" in
        backend|gateway-b|gateway-a) ;;
        *) echo "unknown scenario: $SCENARIO (backend|gateway-b|gateway-a)" >&2; exit 2 ;;
      esac

      case "$NODE" in
        cp0) NODE_IP="${net.cp0}" ;;
        cp1) NODE_IP="${net.cp1}" ;;
        cp2) NODE_IP="${net.cp2}" ;;
        w3)  NODE_IP="${net.w3}" ;;
        *) echo "unknown node: $NODE (cp0|cp1|cp2|w3)" >&2; exit 2 ;;
      esac
      GATEWAY_B="$NODE_IP:${toString rpc.gateway.nodePort}"

      # duration/3, the proto-bench fault cadence: fire the fault a third of the
      # way in, leaving two thirds of the run to observe recovery.
      kill_delay() {
        local d="$DURATION" secs
        case "$d" in
          *s) secs="''${d%s}" ;;
          *m) secs=$(( ''${d%m} * 60 )) ;;
          *)  secs="$d" ;;
        esac
        [[ "$secs" =~ ^[0-9]+$ ]] || secs=15
        local k=$(( secs / 3 )); [ "$k" -lt 1 ] && k=1; echo "$k"
      }

      echo "== k8s-rpc-chaos =="
      echo "  scenario               : $SCENARIO"
      echo "  gateway-B (in-cluster) : $GATEWAY_B"
      echo "  gateway-A (host)       : localhost:$LOCAL_PORT"
      echo "  rate/duration          : $RATE/s for $DURATION (fault at +$(kill_delay)s)"
      echo "  timeout/retries        : $TIMEOUT / $RETRIES"

      if [ "$DRY_RUN" = 1 ]; then
        echo "  (dry-run: nothing executed)"
        exit 0
      fi

      kexec() { k8s-vm-ssh --node="$NODE" env KUBECONFIG=/var/lib/kubernetes/pki/admin-kubeconfig "$@"; }

      # ─── Preflight: gateway-B + service Ready ──────────────────────────
      echo "-- waiting for rpc-gateway and rpc-service to be Ready --"
      if ! kexec kubectl -n ${rpc.namespace} wait --for=condition=Ready pod \
             -l app.kubernetes.io/name=rpc-gateway --timeout=60s; then
        echo "!! rpc-gateway not Ready (is the rpc Application synced and the image imported?)" >&2
      fi
      if ! kexec kubectl -n ${rpc.namespace} wait --for=condition=Ready pod \
             -l app.kubernetes.io/name=rpc-service --timeout=60s; then
        echo "!! rpc-service not Ready" >&2
      fi

      # ─── Host gateway-A → gateway-B ────────────────────────────────────
      start_gateway_a() {
        rpc-gateway -grpc-addr ":$LOCAL_PORT" -backend "$GATEWAY_B" &
        GW_A_PID=$!
      }
      echo "-- starting host gateway-A on :$LOCAL_PORT -> $GATEWAY_B --"
      start_gateway_a
      cleanup() { kill "$GW_A_PID" 2>/dev/null || true; }
      trap cleanup EXIT
      sleep 1
      if ! kill -0 "$GW_A_PID" 2>/dev/null; then
        echo "!! host gateway-A failed to start" >&2
        exit 1
      fi

      # ─── Drive the open-loop run in the background ─────────────────────
      ARGS=(-addr "localhost:$LOCAL_PORT" -transport grpc -mode open \
            -rate "$RATE" -duration "$DURATION" -timeout "$TIMEOUT" -retries "$RETRIES")
      if [ "$METRICS" = 1 ]; then
        ARGS+=(-metrics-addr "localhost:${toString rpc.benchMetricsPort}" -metrics-linger "$LINGER")
      fi
      if [ -n "$OUT" ]; then
        ARGS+=(-out "$OUT")
      fi
      echo "-- rpc-benchmark ''${ARGS[*]} --"
      rpc-benchmark "''${ARGS[@]}" &
      BENCH_PID=$!

      # ─── Inject the fault at duration/3 ────────────────────────────────
      sleep "$(kill_delay)"
      kill_ts=$(date +%s)
      recovered=""
      case "$SCENARIO" in
        backend)
          echo "-- fault: killing rpc-service pod (backend unavailable) --"
          kexec kubectl -n ${rpc.namespace} delete pod -l app=rpc-service --wait=false \
            >/dev/null 2>&1 || echo "!! backend kill failed" >&2
          kexec kubectl -n ${rpc.namespace} rollout status deploy/rpc-service --timeout=90s \
            >/dev/null 2>&1 && recovered=$(( $(date +%s) - kill_ts ))
          ;;
        gateway-b)
          echo "-- fault: killing rpc-gateway pod (gateway-B failure) --"
          kexec kubectl -n ${rpc.namespace} delete pod -l app=rpc-gateway --wait=false \
            >/dev/null 2>&1 || echo "!! gateway-B kill failed" >&2
          kexec kubectl -n ${rpc.namespace} rollout status deploy/rpc-gateway --timeout=90s \
            >/dev/null 2>&1 && recovered=$(( $(date +%s) - kill_ts ))
          ;;
        gateway-a)
          echo "-- fault: crashing and respawning host gateway-A (response orphaned; no broker durability) --"
          kill "$GW_A_PID" 2>/dev/null || true
          wait "$GW_A_PID" 2>/dev/null || true
          start_gateway_a
          sleep 1
          if kill -0 "$GW_A_PID" 2>/dev/null; then
            recovered=$(( $(date +%s) - kill_ts ))
          fi
          ;;
      esac

      # ─── Let the run finish, then report ───────────────────────────────
      wait "$BENCH_PID" || true

      echo
      echo "== chaos summary =="
      echo "  scenario   : $SCENARIO"
      if [ -n "$recovered" ]; then
        echo "  recovery   : ''${recovered}s (fault → Ready again)"
      else
        echo "  recovery   : NOT observed within the window"
      fi
      echo "  (the rpc-benchmark line above reports ok/errors/timeouts/non-ok and"
      echo "   retries/idempotent-replays across the outage — §28 measure list)"
    '';
  };
}
