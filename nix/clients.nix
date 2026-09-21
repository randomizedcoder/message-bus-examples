# nix/clients.nix
#
# Go pub/sub CLI clients, one per bus, run from the host against each
# bus's NodePort. Built with buildGoModule (Nix-built, like the broker
# images) and exposed as flake apps:
#
#   nix run .#nats-pub     -- -msg "hello"
#   nix run .#nats-sub
#   nix run .#rabbitmq-pub -- -pass "$RABBITMQ_PASS" -msg "hi"
#   nix run .#rabbitmq-sub -- -pass "$RABBITMQ_PASS"
#   nix run .#mqtt-pub     -- -msg "hi"
#   nix run .#mqtt-sub
#   nix run .#valkey-pub   -- -pass "$VALKEY_PASS" -msg "hi"
#   nix run .#valkey-sub   -- -pass "$VALKEY_PASS"
#
# Each binary takes `pub`/`sub` as its first argument; the apps below are
# thin wrappers that prepend it so `nix run .#<bus>-<pub|sub> -- <flags>`
# works directly.
#
# `versions` defaults so the several script modules that import this file with
# just `{ pkgs }` (bench/chaos/soak-scripts) keep working; flake.nix passes the
# shared instance explicitly.
{ pkgs, versions ? import ./versions.nix { inherit pkgs; } }:
let
  clients = pkgs.buildGoModule {
    pname = "message-bus-clients";
    version = "0.1.0";
    src = ../clients;
    # Single source of truth in nix/versions.nix (design §5). Bump there after
    # editing clients/go.mod: `nix build .#message-bus-clients 2>&1 | grep got:`.
    vendorHash = versions.goVendorHash;
    subPackages = [
      "nats/natscli"
      "rabbitmq/rabbitmqcli"
      "mqtt/mqttcli"
      "valkey/valkeycli"
      # Self-contained NATS concept demos (see clients/nats/*/README.md).
      "nats/subjects"
      "nats/request-reply"
      "nats/queue-groups"
      "nats/leaf"
      # proto-bench host driver (codec subcommand in P1; gRPC in P2).
      "workloads/benchcli"
      # proto-bench in-cluster server (gRPC services in P2; bus responders P3).
      "workloads/region-agent"
      # RPC lab (§17): the uniform GatewayService trio — a client, an ingress
      # gateway, and the terminal backend service — all speaking rpc.v1.Call.
      "rpc-client"
      "rpc-gateway"
      "rpc-service"
    ];
    meta = {
      description = "Message-bus pub/sub CLI clients (NATS, RabbitMQ, MQTT, ValKey)";
      mainProgram = "natscli";
    };
  };

  # Same source + vendored deps as `clients`, but built only to run the
  # module's unit tests (`go test ./...` covers internal/cli despite the
  # subPackages restriction above). Wired into `nix flake check`.
  tests = clients.overrideAttrs (_: {
    pname = "message-bus-clients-tests";
    doCheck = true;
    checkPhase = ''
      runHook preCheck
      go test ./...
      runHook postCheck
    '';
    # We only care that the tests passed — skip installing binaries.
    installPhase = "touch $out";
  });

  # (appName, binary, subcommand) triples → wrapper + flake app.
  defs = [
    { app = "nats-pub";     bin = "natscli";     sub = "pub"; }
    { app = "nats-sub";     bin = "natscli";     sub = "sub"; }
    { app = "rabbitmq-pub"; bin = "rabbitmqcli"; sub = "pub"; }
    { app = "rabbitmq-sub"; bin = "rabbitmqcli"; sub = "sub"; }
    { app = "mqtt-pub";     bin = "mqttcli";     sub = "pub"; }
    { app = "mqtt-sub";     bin = "mqttcli";     sub = "sub"; }
    { app = "valkey-pub";   bin = "valkeycli";   sub = "pub"; }
    { app = "valkey-sub";   bin = "valkeycli";   sub = "sub"; }
  ];

  mkWrapper = d: pkgs.writeShellApplication {
    name = d.app;
    text = ''exec ${clients}/bin/${d.bin} ${d.sub} "$@"'';
  };

  busApps = builtins.listToAttrs (map (d: {
    name = d.app;
    value = {
      type = "app";
      program = "${mkWrapper d}/bin/${d.app}";
    };
  }) defs);

  # Self-contained NATS concept demos (sources under clients/nats/<concept>).
  # Unlike the pub/sub CLIs these take no `pub`/`sub` subcommand, so they are
  # exposed as the binary directly (no wrapper). `nix run .#nats-subjects`
  # runs the whole demo and exits. (app name = nats-<concept>, binary = <concept>.)
  exampleDefs = [
    { app = "nats-subjects";      bin = "subjects"; }
    { app = "nats-request-reply"; bin = "request-reply"; }
    { app = "nats-queue-groups";  bin = "queue-groups"; }
    { app = "nats-leaf";          bin = "leaf"; }
  ];

  exampleApps = builtins.listToAttrs (map (d: {
    name = d.app;
    value = {
      type = "app";
      program = "${clients}/bin/${d.bin}";
    };
  }) exampleDefs);

  # `nix run .#clients-bench` — run the hermetic Go micro-benchmarks on the
  # host, printing fresh ns/op + allocs/op each invocation. It copies the
  # module source plus the same vendored deps `clients` uses into a temp dir
  # and runs `go test -bench` there, so it needs no network. (A build
  # derivation would cache the numbers, which is wrong for a benchmark, hence
  # a host-run script rather than a check.) Extra args pass through, e.g.
  # `nix run .#clients-bench -- -benchtime 2s -bench BenchmarkPubLoop`.
  benchApp = pkgs.writeShellApplication {
    name = "clients-bench";
    runtimeInputs = with pkgs; [ go coreutils ];
    text = ''
      set -euo pipefail
      work="$(mktemp -d)"
      trap 'chmod -R u+w "$work" 2>/dev/null || true; rm -rf "$work"' EXIT
      # cp -a preserves the store's read-only modes (and copies src dir attrs
      # onto the dest), so make the tree writable after each copy before the
      # next step needs to create files under it.
      cp -a ${../clients}/. "$work/"
      chmod -R u+w "$work"
      mkdir -p "$work/vendor"
      cp -a ${clients.goModules}/. "$work/vendor/"
      chmod -R u+w "$work"
      cd "$work"
      export HOME="$work" GOFLAGS=-mod=vendor GOCACHE="$work/.gocache" GOTOOLCHAIN=local
      echo "running client micro-benchmarks (go test -bench=. -benchmem)…" >&2
      exec go test -bench=. -benchmem -run '^$' "$@" ./...
    '';
  };

  benchApps = {
    clients-bench = {
      type = "app";
      program = "${benchApp}/bin/clients-bench";
      meta.description = "Run the hermetic Go micro-benchmarks for the message-bus clients";
    };
    # proto-bench host driver. `nix run .#benchcli -- codec --sizes`.
    benchcli = {
      type = "app";
      program = "${clients}/bin/benchcli";
      meta.description = "proto-bench host driver (codec loop + schema demos; transports in later phases)";
    };
  };

  # RPC lab (§17): the reference gRPC trio, host-runnable end to end.
  # `nix run .#rpc-service` (backend :9440) + `nix run .#rpc-gateway` (ingress
  # :9430 -> backend) + `nix run .#rpc-client -- -service customer -method Lookup`.
  rpcApps = {
    rpc-service = {
      type = "app";
      program = "${clients}/bin/rpc-service";
      meta.description = "RPC lab backend: GatewayService whose handlers run the demo methods + idempotency cache";
    };
    rpc-gateway = {
      type = "app";
      program = "${clients}/bin/rpc-gateway";
      meta.description = "RPC lab ingress gateway: forwards routed rpc.v1 envelopes to a backend GatewayService";
    };
    rpc-client = {
      type = "app";
      program = "${clients}/bin/rpc-client";
      meta.description = "RPC lab client: builds a routed rpc.v1 envelope and calls a GatewayService endpoint";
    };
  };

  apps = busApps // exampleApps // benchApps // rpcApps;
in
{
  package = clients;
  inherit tests apps;
}
