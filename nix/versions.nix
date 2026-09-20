# nix/versions.nix
#
# Single source of truth for the Go/protobuf toolchain and the two content
# hashes the proto-bench build needs. Every other Nix file names its tools
# through this attrset (design §5). Tools come from the pinned nixpkgs; the
# comments record the versions that pin resolves to today.
{ pkgs }:
rec {
  go = pkgs.go; # 1.26.x in the pinned nixpkgs
  buf = pkgs.buf; # 1.72.0
  protoc = pkgs.protobuf; # protoc + well-known-type includes
  protoc-gen-go = pkgs.protoc-gen-go; # 1.36.12
  protoc-gen-go-grpc = pkgs.protoc-gen-go-grpc; # 1.6.2
  protoc-gen-go-vtproto = pkgs.protoc-gen-go-vtproto; # 0.6.0 (plugin)
  grpcurl = pkgs.grpcurl;

  # Bump after editing clients/go.mod:
  #   nix build .#message-bus-clients 2>&1 | grep 'got:'
  goVendorHash = "sha256-AsI5Aijm0DfB7rincl6pv2JraEQpxL8TvsPnM1D5nzA=";

  # Bump when clients/buf.lock (or buf) changes:
  #   nix build .#buf-deps 2>&1 | grep 'got:'
  bufDepsHash = "sha256-VpzZTpMRgNlgwh+dP3fbw/rcstIqObN/397aHOtQd5k=";
}
