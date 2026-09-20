# nix/secrets.nix
#
# Reads pre-generated secrets from ./secrets/ and produces K8s Secret
# YAML/JSON manifests that the bootstrap module applies at first boot.
#
# If ./secrets/ does not exist, returns { k8sSecrets = null; } so the
# cluster still builds — Secrets just keep their
# __BOOTSTRAPPED_OUT_OF_BAND__ placeholders.
#
# Secret manifests are emitted as JSON (not YAML) via jq so values with
# shell-hostile characters are never shell-interpreted. kubectl apply
# handles JSON natively.
#
# See docs/secrets.md for the full design.
#
{ pkgs, lib }:
let
  secretsDir = ../secrets;
  hasSecrets = builtins.pathExists secretsDir;
in
if !hasSecrets then { k8sSecrets = null; sshPubKey = null; }
else
let
  # ─── Read raw secrets ────────────────────────────────────────────────
  read = name: lib.trim (builtins.readFile (secretsDir + "/${name}"));

  rabbitmqPassword    = read "rabbitmq-password";
  rabbitmqErlangCookie = read "rabbitmq-erlang-cookie";
  valkeyPassword      = read "valkey-password";

  # SSH public key for MicroVM authorized_keys
  sshPubKeyFile = secretsDir + "/ssh-ed25519.pub";
  sshPubKey = if builtins.pathExists sshPubKeyFile
    then lib.trim (builtins.readFile sshPubKeyFile)
    else null;

  # ─── Build K8s Secret manifests (JSON) ──────────────────────────────
  k8sSecrets = pkgs.runCommand "k8s-secrets" {
    nativeBuildInputs = with pkgs; [ jq coreutils ];

    RABBITMQ_PASS   = rabbitmqPassword;
    RABBITMQ_COOKIE = rabbitmqErlangCookie;
    VALKEY_PASS     = valkeyPassword;
  } ''
    mkdir -p $out

    # ── 1. rabbitmq-credentials (ns: rabbitmq) ────────────────────────
    # Default user/password for the AMQP broker + the shared Erlang
    # cookie every node in the cluster must present to form the cluster.
    jq -n \
      --arg pass   "$RABBITMQ_PASS" \
      --arg cookie "$RABBITMQ_COOKIE" \
      '{apiVersion:"v1", kind:"Secret",
        metadata:{name:"rabbitmq-credentials", namespace:"rabbitmq"},
        stringData:{
          "RABBITMQ_DEFAULT_USER":"admin",
          "RABBITMQ_DEFAULT_PASS":$pass,
          "RABBITMQ_ERLANG_COOKIE":$cookie}}' \
      > $out/rabbitmq-credentials.json

    # ── 2. valkey-credentials (ns: valkey) ────────────────────────────
    # requirepass / masterauth for the primary/replica set + Sentinel.
    jq -n \
      --arg pass "$VALKEY_PASS" \
      '{apiVersion:"v1", kind:"Secret",
        metadata:{name:"valkey-credentials", namespace:"valkey"},
        stringData:{"password":$pass}}' \
      > $out/valkey-credentials.json

    # ── 3+4. proto-bench region-agent copies (ns: workloads) ──────────
    # Secrets are namespaced, and the region agents run in `workloads`, so the
    # RabbitMQ / Valkey credentials are replicated there for the agents' bus
    # responders (the same values; NATS and MQTT are unauthenticated). The
    # RabbitMQ user/pass reach the -amqp URL via $(VAR) expansion; the Valkey
    # password is read from VALKEY_PASSWORD (design §9.2, P3).
    jq -n \
      --arg pass "$RABBITMQ_PASS" \
      '{apiVersion:"v1", kind:"Secret",
        metadata:{name:"rabbitmq-credentials", namespace:"workloads"},
        stringData:{"RABBITMQ_DEFAULT_USER":"admin", "RABBITMQ_DEFAULT_PASS":$pass}}' \
      > $out/rabbitmq-credentials-workloads.json

    jq -n \
      --arg pass "$VALKEY_PASS" \
      '{apiVersion:"v1", kind:"Secret",
        metadata:{name:"valkey-credentials", namespace:"workloads"},
        stringData:{"password":$pass}}' \
      > $out/valkey-credentials-workloads.json

    echo "Generated $(ls $out/*.json | wc -l) Secret manifests"
  '';

in
{
  inherit k8sSecrets sshPubKey;
}
