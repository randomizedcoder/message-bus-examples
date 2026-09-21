# nix/images/grpc-broker.nix
#
# The gRPC message-bus broker image: grpcbrokerd, a BrokerService gRPC server
# that fans each published Message out to every live subscriber of its topic.
# Built from the shared `clients` module with the same mkGoBinary + mkImage
# helpers as rpc-service / region-agent, so it flows through imageList → the
# boot-time preload module and `k8s-image-import` unchanged.
#
# Contents: busybox for a /bin userland (a shell for `kubectl exec`) and the
# static grpcbrokerd binary. No cacert — plaintext h2c only.
{ pkgs, lib, versions }:
let
  constants = import ../constants.nix;
  bus = constants.messageBus.grpcbus;
  mkImage = (import ./lib.nix { inherit pkgs; }).mkImage;

  broker = import ../lib/mkGoBinary.nix { inherit pkgs versions; } {
    pname = "grpcbrokerd";
    subPackage = "grpcbus/grpcbrokerd";
    version = bus.tag;
  };
in
mkImage {
  name = bus.image;
  tag = bus.tag;
  contents = with pkgs; [ busybox broker ];
  config = {
    Entrypoint = [ "/bin/grpcbrokerd" ];
    Env = [ "PATH=/bin" "GOMAXPROCS=2" ];
    ExposedPorts = {
      "${toString bus.grpcPort}/tcp" = { };
      "${toString bus.metricsPort}/tcp" = { };
    };
  };
}
