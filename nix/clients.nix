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
    vendorHash = "sha256-Uc1UIqu9GH3NFZ4g6xqQfVCbdhnLocLXvqoE+8mnGlg=";
    subPackages = [
      "cmd/natscli"
      "cmd/rabbitmqcli"
      "cmd/mqttcli"
      "cmd/valkeycli"
    ];
    meta = {
      description = "Message-bus pub/sub CLI clients (NATS, RabbitMQ, MQTT, ValKey)";
      mainProgram = "natscli";
    };
  };

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

  apps = builtins.listToAttrs (map (d: {
    name = d.app;
    value = {
      type = "app";
      program = "${mkWrapper d}/bin/${d.app}";
    };
  }) defs);
in
{
  package = clients;
  inherit apps;
}
