# nix/images/region-agent.nix
#
# The proto-bench region-agent OCI image — the first Nix-built Go-binary image
# in this repo (the buses wrap upstream nixpkgs packages; this wraps our own
# `clients` module). Same `mkImage` helper as the buses (nix/images/lib.nix),
# so it flows through imageList → the boot-time preload module and
# `k8s-image-import` unchanged (design §9.1).
#
# Contents: busybox for a /bin userland (healthz/debug via `wget`, a shell for
# `kubectl exec`), the static region-agent binary, and cacert for the TLS root
# store the bus responders need when they connect over TLS (P3).
{ pkgs, lib, versions }:
let
  constants = import ../constants.nix;
  wl = constants.messageBus.workloads;
  mkImage = (import ./lib.nix { inherit pkgs; }).mkImage;

  regionAgent = import ../lib/mkGoBinary.nix { inherit pkgs versions; } {
    pname = "region-agent";
    subPackage = "workloads/region-agent";
    version = wl.tag;
  };
in
mkImage {
  name = wl.image;
  tag = wl.tag;
  contents = with pkgs; [ busybox regionAgent cacert ];
  config = {
    Entrypoint = [ "/bin/region-agent" ];
    Env = [ "PATH=/bin" "GOMAXPROCS=2" ];
    ExposedPorts = {
      "${toString wl.grpcPort}/tcp" = { };
      "${toString wl.metricsPort}/tcp" = { };
    };
  };
}
