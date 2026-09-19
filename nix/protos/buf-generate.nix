# nix/protos/buf-generate.nix
#
# `nix run .#regen-protos` — the ONLY place `buf dep update` runs (it touches
# the network and rewrites clients/buf.lock). Regenerates the checked-in Go
# code and, only under --accept-breaking, refreshes the breaking-change
# baseline descriptor. Impure and host-side by design (design §5).
{ pkgs, versions }:
pkgs.writeShellApplication {
  name = "regen-protos";
  runtimeInputs = [
    versions.buf
    versions.protoc-gen-go
    versions.protoc-gen-go-grpc
    versions.protoc-gen-go-vtproto
    pkgs.git
  ];
  text = ''
    cd "$(git rev-parse --show-toplevel)/clients"
    buf dep update        # the only network touch; pins buf.lock
    buf lint
    buf build
    buf generate
    if [ "''${1:-}" = "--accept-breaking" ]; then
      buf build -o gen/descriptors/workloads.binpb
    else
      buf breaking --against gen/descriptors/workloads.binpb
    fi
    echo "regen-protos: done — review and commit gen/ drift; bump bufDepsHash if buf.lock changed"
  '';
}
