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
{ pkgs }:
let
  clients = pkgs.buildGoModule {
    pname = "message-bus-clients";
    version = "0.1.0";
    src = ../clients;
    vendorHash = "sha256-DcyJuJ+yhPgM+e2IohCQAF2lm4Wv9q7q38w7jX4re/Y=";
    subPackages = [
      "cmd/natscli"
      "cmd/rabbitmqcli"
      "cmd/mqttcli"
      "cmd/valkeycli"
      # Self-contained NATS concept demos (see clients/nats/*/README.md).
      "nats/subjects"
      "nats/request-reply"
      "nats/queue-groups"
      "nats/leaf"
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

  apps = busApps // exampleApps;
in
{
  package = clients;
  inherit tests apps;
}
