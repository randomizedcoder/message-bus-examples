# nix/rpc-scripts.nix
#
# `k8s-rpc-bench` — the host-side harness for the RPC lab (§17, §25). It drives
# `rpc-benchmark` against the in-cluster GatewayService through the full
# two-gateway topology the proposal describes:
#
#   host rpc-benchmark → gateway-A (host) → gateway-B (NodePort) → rpc-service
#
# gateway-A runs on the host (a plain rpc-gateway process) and forwards to
# gateway-B on its NodePort; gateway-B (in-cluster) forwards to rpc-service on
# its ClusterIP. Every hop speaks GatewayService.Call, so the only thing this
# phase measures is the gRPC transport end to end across the cluster.
#
#   nix run .#k8s-rpc-bench                       # closed-loop default
#   nix run .#k8s-rpc-bench -- --mode open --rate 2000 --duration 10s
#   nix run .#k8s-rpc-bench -- --dry-run          # print the plan, run nothing
#
# It needs the cluster up with the rpc gateway + service deployed (this PR's
# gitops) and the images preloaded (`nix run .#k8s-image-import`). Like the other
# harnesses it uses the k8s-vm-ssh wrapper for kubectl, never raw ssh.
{ pkgs }:
let
  constants = import ./constants.nix;
  vmScripts = import ./microvm-scripts.nix { inherit pkgs; };
  clients   = import ./clients.nix { inherit pkgs; };
  rpc = constants.messageBus.rpc;
  net = constants.network.ipv4;
in
{
  rpcBench = pkgs.writeShellApplication {
    name = "k8s-rpc-bench";
    runtimeInputs = with pkgs; [
      coreutils gawk gnused jq procps
      vmScripts.ssh
      clients.package
    ];
    text = ''
      # Not -e: a failed cell should report, not abort mid-cleanup. The host
      # gateway-A is a child process we always tear down via the trap.
      set -uo pipefail

      NODE="cp0"
      MODE="closed"
      REQUESTS=20000
      CONCURRENCY=64
      RATE=2000
      DURATION="10s"
      TIMEOUT="5s"
      LOCAL_PORT=9431          # host gateway-A listen port
      AS_JSON=0
      DRY_RUN=0

      usage() {
        cat <<'EOF'
      k8s-rpc-bench — drive rpc-benchmark through host gateway-A → in-cluster gateway-B → rpc-service

      Options:
        --node <n>          node whose NodePort to reach gateway-B on (default cp0)
        --mode <m>          closed | open (default closed)
        --requests <n>      closed: total request budget (default 20000)
        --concurrency <n>   closed: worker count (default 64)
        --rate <n>          open: offered req/s (default 2000)
        --duration <d>      open: wall-clock budget (default 10s)
        --timeout <d>       per-call timeout (default 5s)
        --local-port <p>    host gateway-A listen port (default 9431)
        --json              emit the benchmark result as JSON
        --dry-run           print the plan and exit
        -h, --help          this help
      EOF
      }

      while [ $# -gt 0 ]; do
        case "$1" in
          --node) NODE="$2"; shift 2 ;;
          --mode) MODE="$2"; shift 2 ;;
          --requests) REQUESTS="$2"; shift 2 ;;
          --concurrency) CONCURRENCY="$2"; shift 2 ;;
          --rate) RATE="$2"; shift 2 ;;
          --duration) DURATION="$2"; shift 2 ;;
          --timeout) TIMEOUT="$2"; shift 2 ;;
          --local-port) LOCAL_PORT="$2"; shift 2 ;;
          --json) AS_JSON=1; shift ;;
          --dry-run) DRY_RUN=1; shift ;;
          -h|--help) usage; exit 0 ;;
          *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
        esac
      done

      case "$NODE" in
        cp0) NODE_IP="${net.cp0}" ;;
        cp1) NODE_IP="${net.cp1}" ;;
        cp2) NODE_IP="${net.cp2}" ;;
        w3)  NODE_IP="${net.w3}" ;;
        *) echo "unknown node: $NODE (cp0|cp1|cp2|w3)" >&2; exit 2 ;;
      esac
      GATEWAY_B="$NODE_IP:${toString rpc.gateway.nodePort}"

      echo "== k8s-rpc-bench =="
      echo "  gateway-B (in-cluster) : $GATEWAY_B"
      echo "  gateway-A (host)       : localhost:$LOCAL_PORT"
      echo "  mode                   : $MODE"
      if [ "$MODE" = open ]; then
        echo "  rate/duration          : $RATE/s for $DURATION"
      else
        echo "  requests/concurrency   : $REQUESTS / $CONCURRENCY"
      fi

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
      echo "-- starting host gateway-A on :$LOCAL_PORT -> $GATEWAY_B --"
      rpc-gateway -grpc-addr ":$LOCAL_PORT" -backend "$GATEWAY_B" &
      GW_A_PID=$!
      cleanup() { kill "$GW_A_PID" 2>/dev/null || true; }
      trap cleanup EXIT
      sleep 1

      if ! kill -0 "$GW_A_PID" 2>/dev/null; then
        echo "!! host gateway-A failed to start" >&2
        exit 1
      fi

      # ─── Drive the benchmark ───────────────────────────────────────────
      ARGS=(-addr "localhost:$LOCAL_PORT" -transport grpc -mode "$MODE" -timeout "$TIMEOUT")
      if [ "$MODE" = open ]; then
        ARGS+=(-rate "$RATE" -duration "$DURATION")
      else
        ARGS+=(-requests "$REQUESTS" -concurrency "$CONCURRENCY")
      fi
      if [ "$AS_JSON" = 1 ]; then
        ARGS+=(-json)
      fi

      echo "-- rpc-benchmark ''${ARGS[*]} --"
      rpc-benchmark "''${ARGS[@]}"
    '';
  };
}
