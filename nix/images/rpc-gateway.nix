# nix/images/rpc-gateway.nix
#
# The RPC lab routing gateway image (§17): a GatewayService gRPC ingress that
# forwards routed rpc.v1 envelopes to a backend GatewayService. Deployed as
# gateway-B next to the service; the host driver runs a matching gateway-A.
# Same mkGoBinary + mkImage helpers as region-agent, so it preloads identically.
#
# Contents: busybox for a /bin userland and the static rpc-gateway binary. No
# cacert — this phase is plaintext h2c only.
{ pkgs, lib, versions }:
let
  constants = import ../constants.nix;
  gw = constants.messageBus.rpc.gateway;
  mkImage = (import ./lib.nix { inherit pkgs; }).mkImage;

  rpcGateway = import ../lib/mkGoBinary.nix { inherit pkgs versions; } {
    pname = "rpc-gateway";
    subPackage = "rpc-gateway";
    version = gw.tag;
  };
in
mkImage {
  name = gw.image;
  tag = gw.tag;
  contents = with pkgs; [ busybox rpcGateway ];
  config = {
    Entrypoint = [ "/bin/rpc-gateway" ];
    Env = [ "PATH=/bin" "GOMAXPROCS=2" ];
    ExposedPorts = {
      "${toString gw.grpcPort}/tcp" = { };
    };
  };
}
