# nix/protos/default.nix
#
# Proto tooling aggregator: the hermetic buf module cache (FOD), the three
# `nix flake check` gates (lint, breaking, gen-drift), and the impure
# host-side `regen-protos` app. All buf checks share one writable copy of the
# offline module cache; the sandbox has no network, so a missing module fails
# loudly (design §4.2).
{ pkgs, versions }:
let
  src = ../../clients;
  bufDeps = import ./buf-deps.nix { inherit pkgs versions; };
  regenProtos = import ./buf-generate.nix { inherit pkgs versions; };

  mkBufCheck = { name, extraInputs ? [ ], cmd }:
    pkgs.runCommand name { nativeBuildInputs = [ versions.buf ] ++ extraInputs; } ''
      cp -r ${bufDeps} $TMPDIR/bufcache && chmod -R u+w $TMPDIR/bufcache
      export BUF_CACHE_DIR=$TMPDIR/bufcache HOME=$TMPDIR
      cd ${src}
      ${cmd}
      touch $out
    '';

  checks = {
    proto-lint = mkBufCheck {
      name = "proto-lint";
      cmd = "buf lint";
    };
    proto-breaking = mkBufCheck {
      name = "proto-breaking";
      cmd = "buf breaking --against gen/descriptors/workloads.binpb";
    };
    proto-gen-drift = mkBufCheck {
      name = "proto-gen-drift";
      extraInputs = [ versions.protoc-gen-go versions.protoc-gen-go-grpc versions.protoc-gen-go-vtproto ];
      cmd = ''
        buf generate -o "$TMPDIR/out"
        diff -ru gen/go "$TMPDIR/out/gen/go"
      '';
    };
  };
in
{
  inherit bufDeps regenProtos checks mkBufCheck;
}
