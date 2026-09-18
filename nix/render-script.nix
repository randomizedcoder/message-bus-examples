# nix/render-script.nix
#
# Rendered manifests script.
# Builds k8s-manifests and copies output to rendered/ directory for git tracking.
# Supports --check mode for CI: verifies rendered/ matches nix build output.
#
# Usage:
#   nix run .#k8s-render-manifests            # Render and update rendered/
#   nix run .#k8s-render-manifests -- --check # Verify rendered/ is up to date
#
{ pkgs }:
pkgs.writeShellApplication {
  name = "k8s-render-manifests";
  runtimeInputs = with pkgs; [ nix git coreutils findutils diffutils ];
  text = ''
    REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
    RENDERED_DIR="$REPO_ROOT/rendered"
    CHECK_MODE=false

    for arg in "$@"; do
      case "$arg" in
        --check) CHECK_MODE=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
      esac
    done

    echo "=== Building k8s-manifests ==="
    nix build "$REPO_ROOT#k8s-manifests" --out-link "$REPO_ROOT/result"

    if [ "$CHECK_MODE" = true ]; then
      echo "=== Checking rendered/ is up to date ==="
      TMPDIR=$(mktemp -d)
      # shellcheck disable=SC2064
      trap "chmod -R u+w '$TMPDIR' 2>/dev/null; rm -rf '$TMPDIR'" EXIT
      cp -rL "$REPO_ROOT/result/." "$TMPDIR/"
      chmod -R u+w "$TMPDIR"
      rm -f "$TMPDIR/README"

      if diff -rq "$RENDERED_DIR" "$TMPDIR" > /dev/null 2>&1; then
        echo "rendered/ is up to date"
        exit 0
      else
        echo "ERROR: rendered/ is out of date. Run: nix run .#k8s-render-manifests"
        diff -rq "$RENDERED_DIR" "$TMPDIR" || true
        exit 1
      fi
    fi

    echo "=== Updating rendered/ ==="
    mkdir -p "$RENDERED_DIR"
    # Previous render copied files from /nix/store; they're read-only.
    # Make writable before removing.
    chmod -R u+w "$RENDERED_DIR" 2>/dev/null || true
    rm -rf "''${RENDERED_DIR:?}"/*
    cp -rL "$REPO_ROOT/result/." "$RENDERED_DIR/"
    chmod -R u+w "$RENDERED_DIR"
    rm -f "$RENDERED_DIR/README"

    echo ""
    echo "=== Rendered manifests ==="
    find "$RENDERED_DIR" -type f -name '*.yaml' | sort | while read -r f; do
      echo "  ''${f#"$REPO_ROOT/"}"
    done

    echo ""
    if [ -d "$REPO_ROOT/.git" ]; then
      echo "=== Git diff ==="
      git -C "$REPO_ROOT" diff --stat rendered/ || true
    fi
  '';
}
