# nix/images/rpc-service.nix
#
# The RPC lab terminal backend image (§17): a GatewayService gRPC server whose
# handlers run the demo business methods. Built from the shared `clients` module
# with the same mkGoBinary + mkImage helpers as region-agent, so it flows through
# imageList → the boot-time preload module and `k8s-image-import` unchanged.
#
# Contents: busybox for a /bin userland (a shell for `kubectl exec`) and the
# static rpc-service binary. No cacert — this phase is plaintext h2c only.
{ pkgs, lib, versions }:
let
  constants = import ../constants.nix;
  svc = constants.messageBus.rpc.service;
  mkImage = (import ./lib.nix { inherit pkgs; }).mkImage;

  rpcService = import ../lib/mkGoBinary.nix { inherit pkgs versions; } {
    pname = "rpc-service";
    subPackage = "rpc-service";
    version = svc.tag;
  };
in
mkImage {
  name = svc.image;
  tag = svc.tag;
  contents = with pkgs; [ busybox rpcService ];
  config = {
    Entrypoint = [ "/bin/rpc-service" ];
    Env = [ "PATH=/bin" "GOMAXPROCS=2" ];
    ExposedPorts = {
      "${toString svc.grpcPort}/tcp" = { };
    };
  };
}
