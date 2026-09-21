# nix/images/default.nix
#
# Nix-built OCI container images for the four message buses.
#
# Design decision: rather than pulling upstream vendor images, we build
# each broker's image from its nixpkgs package with `dockerTools`. This
# gives us:
#   • customisation  — exactly the binaries + config we need, nothing else
#   • determinism     — byte-for-byte reproducible from the pinned nixpkgs
#   • size            — minimal closures, far smaller than the vendor images
#
# The images are preloaded into each node's containerd at boot
# (nix/image-preload-module.nix) — no registry, no network pull. The
# StatefulSets reference them by the exact `name:tag` below with
# imagePullPolicy: Never.
#
# `nix build .#<bus>-image` builds one image tarball (for size checks:
#   nix path-info -Sh ./result   # closure size
#   du -h ./result               # compressed layer tar size
# ).
#
# `versions` (default-imported) is needed only by the region-agent image, which
# builds a Go binary from the shared `clients` module; the bus/observability
# images wrap nixpkgs packages and don't use it.
{ pkgs, lib, versions ? import ../versions.nix { inherit pkgs; } }:
let
  constants = import ../constants.nix;
  mb = constants.messageBus;
  mon = constants.monitoring;

  # Shared layered-image builder (nix/images/lib.nix), also used by
  # region-agent.nix.
  mkImage = (import ./lib.nix { inherit pkgs; }).mkImage;

  images = {
    # busybox gives a tiny static /bin/sh + coreutils applets for the
    # ordinal-based init scripts; the broker binary is the only other
    # thing in the closure. (Can't be combined with GNU coreutils —
    # duplicate /bin entries would collide in the image layer.)
    nats = mkImage {
      name = mb.nats.image;
      tag  = mb.nats.tag;
      contents = with pkgs; [ busybox nats-server ];
    };

    rabbitmq = mkImage {
      name = mb.rabbitmq.image;
      tag  = mb.rabbitmq.tag;
      # RabbitMQ's boot + rabbitmqctl scripts expect bash + a GNU
      # userland alongside the Erlang runtime (propagated by the package),
      # so this image uses coreutils/bash rather than busybox.
      # dockerTools.binSh provides /bin/sh (→ bash), needed by the
      # StatefulSet's command + readiness probe.
      contents = with pkgs; [
        dockerTools.binSh
        bashInteractive coreutils procps gnused gawk gnugrep findutils util-linux
        rabbitmq-server
      ];
    };

    mosquitto = mkImage {
      name = mb.mqtt.image;
      tag  = mb.mqtt.tag;
      contents = with pkgs; [ busybox mosquitto ];
    };

    valkey = mkImage {
      name = mb.valkey.image;
      tag  = mb.valkey.tag;
      contents = with pkgs; [ busybox valkey ];
    };

    # ─── Observability stack ───────────────────────────────────────────
    # Prometheus, its NATS exporter, and Grafana — same no-registry pattern
    # as the buses. Runtime paths/flags are supplied by the Deployments in
    # nix/gitops/env/monitoring.nix (e.g. Grafana's GF_PATHS_*), so the images
    # only need the binary plus a busybox userland.
    prometheus = mkImage {
      name = mon.prometheus.image;
      tag  = mon.prometheus.tag;
      contents = with pkgs; [ busybox prometheus ];
    };

    prometheus-nats-exporter = mkImage {
      name = mon.natsExporter.image;
      tag  = mon.natsExporter.tag;
      contents = with pkgs; [ busybox prometheus-nats-exporter ];
    };

    grafana = mkImage {
      name = mon.grafana.image;
      tag  = mon.grafana.tag;
      contents = with pkgs; [ busybox grafana ];
    };

    prometheus-redis-exporter = mkImage {
      name = mon.redisExporter.image;
      tag  = mon.redisExporter.tag;
      contents = with pkgs; [ busybox prometheus-redis-exporter ];
    };

    # ─── Mosquitto $SYS → Prometheus sidecar ───────────────────────────
    # Built from our own Go module (like region-agent), not a nixpkgs package —
    # mosquitto has no upstream exporter (design §11.5). Connects to the broker
    # over plain localhost TCP, so no cacert is needed in the closure.
    mosquitto-sysexporter = mkImage {
      name = mon.mosquittoExporter.image;
      tag  = mon.mosquittoExporter.tag;
      contents = [
        pkgs.busybox
        (import ../lib/mkGoBinary.nix { inherit pkgs versions; } {
          pname = "sysexporter";
          subPackage = "mqtt/sysexporter";
          version = mon.mosquittoExporter.tag;
        })
      ];
      config = {
        Entrypoint = [ "/bin/sysexporter" ];
        Env = [ "PATH=/bin" ];
        ExposedPorts = { "${toString mon.mosquittoExporter.port}/tcp" = { }; };
      };
    };

    # ─── proto-bench region-agent ──────────────────────────────────────
    # The one image built from our own Go module (design §9.1). Defined in
    # its own file since it also needs mkGoBinary + versions.
    region-agent = import ./region-agent.nix { inherit pkgs lib versions; };

    # ─── RPC lab gateway + service (§17) ───────────────────────────────
    # Two more Go-binary images from the shared clients module, deployed
    # in-cluster so later bus transports get a gateway-B next to the brokers.
    rpc-gateway = import ./rpc-gateway.nix { inherit pkgs lib versions; };
    rpc-service = import ./rpc-service.nix { inherit pkgs lib versions; };
  };

  # Flat list the preload module + microvm generator consume.
  # Each entry: { name; tag; archive = <docker-archive tarball drv>; }
  imageList = [
    { name = mb.nats.image;     tag = mb.nats.tag;     archive = images.nats; }
    { name = mb.rabbitmq.image; tag = mb.rabbitmq.tag; archive = images.rabbitmq; }
    { name = mb.mqtt.image;     tag = mb.mqtt.tag;     archive = images.mosquitto; }
    { name = mb.valkey.image;   tag = mb.valkey.tag;   archive = images.valkey; }
    { name = mon.prometheus.image;   tag = mon.prometheus.tag;   archive = images.prometheus; }
    { name = mon.natsExporter.image; tag = mon.natsExporter.tag; archive = images.prometheus-nats-exporter; }
    { name = mon.grafana.image;      tag = mon.grafana.tag;      archive = images.grafana; }
    { name = mon.redisExporter.image; tag = mon.redisExporter.tag; archive = images.prometheus-redis-exporter; }
    { name = mon.mosquittoExporter.image; tag = mon.mosquittoExporter.tag; archive = images.mosquitto-sysexporter; }
    { name = mb.workloads.image; tag = mb.workloads.tag; archive = images.region-agent; }
    { name = mb.rpc.gateway.image; tag = mb.rpc.gateway.tag; archive = images.rpc-gateway; }
    { name = mb.rpc.service.image; tag = mb.rpc.service.tag; archive = images.rpc-service; }
  ];
in
{
  inherit images imageList;
}
