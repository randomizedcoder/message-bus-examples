# nix/secrets-gen.nix
#
# Generates application secrets into ./secrets/ for offline pre-generation.
#
# Usage:
#   nix run .#k8s-gen-secrets             # generate (refuses if dir exists)
#   nix run .#k8s-gen-secrets -- --force  # regenerate (overwrites)
#
# The files produced here are consumed at Nix build time by
# nix/secrets.nix, which assembles them into K8s Secret manifests.
# See docs/secrets.md for the full design.
#
{ pkgs }:
{
  genSecrets = pkgs.writeShellApplication {
    name = "k8s-gen-secrets";
    runtimeInputs = with pkgs; [ coreutils openssl openssh git ];
    text = ''
      set -euo pipefail

      FORCE="no"
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --force) FORCE="yes"; shift ;;
          *) echo "Usage: k8s-gen-secrets [--force]" >&2; exit 2 ;;
        esac
      done

      DIR="./secrets"

      if [[ -d "$DIR" && "$FORCE" != "yes" ]]; then
        echo "secrets/ already exists ($(find "$DIR" -maxdepth 1 -type f | wc -l) files)."
        echo "Pass --force to regenerate."
        find "$DIR" -maxdepth 1 -type f -printf '%f\n' | sort
        exit 1
      fi

      mkdir -p "$DIR"
      chmod 700 "$DIR"

      gen_hex() {
        # $1 = filename, $2 = byte count (output is 2x hex chars)
        openssl rand -hex "$2" > "$DIR/$1"
      }

      echo "=== Generating message-bus secrets ==="

      # ── RabbitMQ (2 secrets) ─────────────────────────────────────────
      # Default AMQP user password + the shared Erlang cookie that every
      # node in the RabbitMQ cluster must present to peer.
      gen_hex rabbitmq-password 24
      # Erlang cookie: alphanumeric only (Erlang dislikes some symbols).
      openssl rand -hex 20 > "$DIR/rabbitmq-erlang-cookie"

      # ── ValKey (1 secret) ────────────────────────────────────────────
      # requirepass / masterauth for the primary/replica set + Sentinel.
      gen_hex valkey-password 24

      # ── SSH (2 files: private key + public key) ─────────────────────
      # ED25519 key pair for passwordless SSH into the MicroVMs. The
      # public key is baked into each VM at build time; the private key
      # stays on the host in ./secrets/.
      ssh-keygen -t ed25519 -f "$DIR/ssh-ed25519" -N "" -C "k8s-cluster" -q
      # ssh-keygen creates ssh-ed25519 (private) and ssh-ed25519.pub (public)

      chmod 600 "$DIR"/*
      chmod 644 "$DIR/ssh-ed25519.pub"

      # Nix flakes can only read git-tracked files. Stage the secrets
      # so `builtins.pathExists` and `readFile` work during Nix eval.
      # The files are NOT gitignored (secrets/ is excluded from
      # .gitignore to avoid this exact issue). Review discipline and
      # the `git diff --cached` check in your workflow prevent
      # accidental commits of raw key material.
      if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git add "$DIR" 2>/dev/null || true
        echo "(staged secrets/ for Nix — remember to 'git reset secrets/' before committing)"
      fi

      echo ""
      echo "=== Generated $(find "$DIR" -maxdepth 1 -type f | wc -l) files in $DIR/ ==="
      find "$DIR" -maxdepth 1 -type f -printf '%f\n' | sort
      echo ""
      echo "Next: rebuild the cluster to pick up the new secrets."
      echo "  nix run .#k8s-cluster-rebuild"
    '';
  };
}
