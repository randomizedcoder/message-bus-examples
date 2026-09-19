# nix/protos/buf-deps.nix
#
# Fixed-output derivation: the buf module cache for the pinned protovalidate
# dependency, keyed on clients/buf.lock (design §4.2). Because buf.lock is
# committed, this resolves the pinned commits into the cache WITHOUT running
# `buf dep update` (which would rewrite the lock file). The sandbox has no
# network except for this FOD, so a missing/stale module fails loudly in the
# consuming checks instead of silently fetching.
{ pkgs, versions }:
let
  # Only the buf-relevant inputs; other client files never affect the cache.
  src = pkgs.lib.fileset.toSource {
    root = ../../clients;
    fileset = pkgs.lib.fileset.unions [
      ../../clients/buf.yaml
      ../../clients/buf.lock
      ../../clients/proto
    ];
  };
in
pkgs.stdenvNoCC.mkDerivation {
  name = "buf-module-cache";
  inherit src;
  nativeBuildInputs = [ versions.buf pkgs.cacert ];

  outputHashAlgo = "sha256";
  outputHashMode = "recursive";
  outputHash = versions.bufDepsHash;

  SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";

  buildPhase = ''
    export HOME=$TMPDIR BUF_CACHE_DIR=$out
    # Resolves the pinned commits from buf.lock into the cache.
    buf build -o $TMPDIR/image.binpb
    # Drop cache-internal lock/marker files so the NAR hash is content-only.
    find $out -type f -name '*.lock' -delete
  '';
  dontInstall = true;
  dontFixup = true;
}
