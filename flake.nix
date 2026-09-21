#
# flake.nix - message-bus-examples: clustered message buses on a
#             4-Node HA Kubernetes Cluster via NixOS MicroVMs
#
# HA K8s cluster (3 control planes + 1 worker) running as lightweight QEMU
# MicroVMs with TAP networking. PKI generated at build time and baked into
# VM images. Uses Cilium CNI (replacing kube-proxy), dual-stack IPv4/IPv6,
# host-side haproxy for apiserver HA, and GitOps deployment via ArgoCD.
#
# Workloads: four clustered message buses — NATS, RabbitMQ, MQTT
# (Mosquitto, bridged), ValKey (replication + Sentinel) — each running as
# a hand-written StatefulSet from a Nix-built OCI image preloaded into
# containerd (no upstream images, no registry). Go pub/sub CLI clients are
# exposed as flake apps (nix run .#<bus>-pub / -sub).
#
# Architecture:
#   Host ─── k8sbr0 (bridge) ─┬─ k8stap0 → cp0  10.33.33.10  (etcd, apiserver, scheduler, CM)
#            haproxy:6443 ──┐  ├─ k8stap1 → cp1  10.33.33.11  (etcd, apiserver, scheduler, CM)
#            (LB → 3 CPs)  │  ├─ k8stap2 → cp2  10.33.33.12  (etcd, apiserver, scheduler, CM)
#                           └──└─ k8stap3 → w3   10.33.33.13  (kubelet, containerd)
#
# Quick Start:
#   nix develop                           # Dev shell (kubectl, helm, cilium-cli, step-cli, ...)
#   nix run .#k8s-check-host             # Verify host prereqs (tun, vhost-net, bridge)
#   sudo nix run .#k8s-network-setup    # Create bridge + 4 TAPs + NAT + haproxy LB
#   nix run .#k8s-start-all             # Build + start all 4 VMs (CPs first, then worker)
#   nix run .#k8s-vm-ssh -- --node=cp0 kubectl get nodes
#   nix run .#k8s-lifecycle-test-all    # Automated end-to-end test suite
#
# Teardown:
#   nix run .#k8s-vm-stop               # Stop all VMs
#   sudo nix run .#k8s-network-teardown # Remove bridge, TAPs, NAT, haproxy
#
# File Structure:
#   flake.nix                  # This file — orchestrator
#   nix/constants.nix          # IPs, MACs, ports, CIDRs, bus config, timeouts
#   nix/nodes.nix              # Node definitions (cp0, cp1, cp2, w3)
#   nix/microvm.nix            # mkK8sNode parametric VM generator
#   nix/k8s-module.nix         # NixOS module: etcd, apiserver, kubelet, containerd
#   nix/image-preload-module.nix # NixOS module: import Nix bus images into containerd
#   nix/gitops-bootstrap-module.nix # NixOS module: first-boot oneshot
#   nix/network-setup.nix      # Bridge + TAP + NAT + haproxy setup/teardown
#   nix/certs.nix              # Build-time PKI: 3 CAs + per-component certs
#   nix/secrets-gen.nix        # Offline secret generation (→ ./secrets/)
#   nix/secrets.nix            # Build-time: reads ./secrets/, emits K8s Secret manifests
#   nix/microvm-scripts.nix    # VM management (check, stop, ssh, start-all)
#   nix/chaos-scripts.nix      # Message-bus failover test
#   nix/clients.nix            # Go pub/sub CLI clients → flake apps
#   nix/shell.nix              # Dev shell
#   nix/images/                # Nix-built OCI images (nats, rabbitmq, mosquitto, valkey)
#   nix/lifecycle/             # Lifecycle test framework (per-node + cluster)
#   nix/gitops/                # Manifest generator (base, argocd, cilium, storage + 4 buses)
#   clients/                   # Go module: cmd/{natscli,rabbitmqcli,mqttcli,valkeycli}
#
{
  description = "Clustered message buses (NATS, RabbitMQ, MQTT, ValKey) on an HA K8s MicroVM cluster";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    microvm = {
      url = "github:astro/microvm.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      microvm,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        nixDir = ./nix;
        pkgs = nixpkgs.legacyPackages.${system};
        lib = pkgs.lib;

        constants = import (nixDir + "/constants.nix");
        nodes = import (nixDir + "/nodes.nix") { inherit constants; };
        k8sModule = import (nixDir + "/k8s-module.nix");
        preloadModule = import (nixDir + "/image-preload-module.nix");
        bootstrapModule = import (nixDir + "/gitops-bootstrap-module.nix");

        # Import cert generation (build-time PKI)
        certs = import (nixDir + "/certs.nix") { inherit pkgs lib; };

        # Import secrets pre-generation (reads ./secrets/ if it exists)
        secrets = import (nixDir + "/secrets.nix") {
          inherit pkgs lib;
        };
        k8sSecrets = secrets.k8sSecrets;  # null if ./secrets/ doesn't exist
        sshPubKey  = secrets.sshPubKey;   # null if no SSH key generated

        # Secrets generation script
        secretsGen = import (nixDir + "/secrets-gen.nix") { inherit pkgs; };

        # GitOps manifest generator (also consumed by the bootstrap unit)
        gitops = import (nixDir + "/gitops") { inherit pkgs lib; };
        k8sManifests = gitops.packages.k8s-manifests;

        # Toolchain + content-hash single source of truth (design §5).
        versions = import (nixDir + "/versions.nix") { inherit pkgs; };

        # Nix-built message-bus OCI images (preloaded into containerd). Passes
        # `versions` because the region-agent image builds a Go binary from the
        # shared clients module.
        busImagesMod = import (nixDir + "/images") { inherit pkgs lib versions; };
        busImages = busImagesMod.imageList;

        # Proto tooling: buf module cache (FOD), gen-drift/lint/breaking checks,
        # and the impure regen-protos app.
        protos = import (nixDir + "/protos") { inherit pkgs versions; };

        # Go pub/sub CLI clients, packaged as flake apps.
        clients = import (nixDir + "/clients.nix") { inherit pkgs versions; };

        # ─── MicroVM Generator ───────────────────────────────────────────
        mkK8sNode = { nodeName, role }:
          import (nixDir + "/microvm.nix") {
            inherit pkgs lib microvm k8sModule preloadModule bootstrapModule nixpkgs system;
            inherit nodeName role;
            nodePki = certs.mkNodePki { inherit nodeName role; };
            inherit k8sManifests k8sSecrets sshPubKey busImages;
          };

        # Generate MicroVM packages for all nodes
        vmPackages = lib.mapAttrs' (name: def:
          lib.nameValuePair "k8s-microvm-${name}" (mkK8sNode {
            nodeName = name;
            inherit (def) role;
          })
        ) nodes.definitions;

        # Import lifecycle testing framework (Linux only)
        lifecycle = lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux (
          import (nixDir + "/lifecycle") { inherit pkgs lib; }
        );

        # Rendered manifests script
        renderScript = import (nixDir + "/render-script.nix") { inherit pkgs; };

      in
      {
        packages = vmPackages // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux (
          # Lifecycle test packages
          (lifecycle.packages or {})
          # GitOps manifests
          // gitops.packages
          # Cert generation (copies build-time certs to ./certs/ for inspection)
          // { k8s-gen-certs = certs.genCerts; }
          # Raw PKI store (all certs)
          // { k8s-pki = certs.pkiStore; }
          # Nix-built message-bus OCI images. `nix build .#<bus>-image`
          # produces the docker-archive tarball preloaded into containerd.
          // {
            nats-image      = busImagesMod.images.nats;
            rabbitmq-image  = busImagesMod.images.rabbitmq;
            mosquitto-image = busImagesMod.images.mosquitto;
            valkey-image    = busImagesMod.images.valkey;
            # Observability stack images (in-cluster Prometheus + Grafana).
            prometheus-image              = busImagesMod.images.prometheus;
            prometheus-nats-exporter-image = busImagesMod.images.prometheus-nats-exporter;
            grafana-image                 = busImagesMod.images.grafana;
            prometheus-redis-exporter-image = busImagesMod.images.prometheus-redis-exporter;
            mosquitto-sysexporter-image     = busImagesMod.images.mosquitto-sysexporter;
            # proto-bench region-agent (first Nix-built Go-binary image).
            region-agent-image            = busImagesMod.images.region-agent;
          }
          # Go pub/sub CLI clients (all four binaries in one derivation).
          // { message-bus-clients = clients.package; }
          # Hermetic buf module cache (FOD). `nix build .#buf-deps` to refresh
          # bufDepsHash in nix/versions.nix.
          // { buf-deps = protos.bufDeps; }
        );

        # ─── Checks (`nix flake check`) ────────────────────────────────
        # cli-tests: table-driven unit tests for the shared cli package.
        # proto-lint/breaking/gen-drift: buf gates over the workloads.v1 schema
        # and its checked-in generated code. go-vet/gofmt: the client module.
        checks = lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux (
          import (nixDir + "/checks") { inherit pkgs versions clients protos; }
        );

        devShells.default = import (nixDir + "/shell.nix") { inherit pkgs versions; };

        # ─── Apps (Linux only) ─────────────────────────────────────────
        apps = lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux (
          let
            networkScripts = import (nixDir + "/network-setup.nix") { inherit pkgs; };
            vmScripts = import (nixDir + "/microvm-scripts.nix") { inherit pkgs; };
            chaosScripts = import (nixDir + "/chaos-scripts.nix") { inherit pkgs; };
            soakScripts = import (nixDir + "/soak-scripts.nix") { inherit pkgs; };
            benchScripts = import (nixDir + "/bench-scripts.nix") { inherit pkgs; };
            protoBenchScripts = import (nixDir + "/proto-bench-scripts.nix") { inherit pkgs; };
            integrationScripts = import (nixDir + "/integration-scripts.nix") { inherit pkgs; };
            imageImportScripts = import (nixDir + "/image-import.nix") { inherit pkgs; };
            rawApps =
          {
            # Proto codegen (impure; the only place `buf dep update` runs).
            regen-protos = {
              type = "app";
              program = "${protos.regenProtos}/bin/regen-protos";
              meta.description = "Regenerate the checked-in workloads.v1 Go/gRPC/vtproto code from the .proto sources";
            };

            # Network management
            k8s-check-host = {
              type = "app";
              program = "${networkScripts.check}/bin/k8s-check-host";
            };
            k8s-network-setup = {
              type = "app";
              program = "${networkScripts.setup}/bin/k8s-network-setup";
            };
            k8s-network-teardown = {
              type = "app";
              program = "${networkScripts.teardown}/bin/k8s-network-teardown";
            };

            # VM management
            k8s-vm-check = {
              type = "app";
              program = "${vmScripts.check}/bin/k8s-vm-check";
            };
            k8s-vm-stop = {
              type = "app";
              program = "${vmScripts.stop}/bin/k8s-vm-stop";
            };
            k8s-vm-stop-one = {
              type = "app";
              program = "${vmScripts.stopOne}/bin/k8s-vm-stop-one";
            };
            k8s-vm-start-one = {
              type = "app";
              program = "${vmScripts.startOne}/bin/k8s-vm-start-one";
            };
            k8s-vm-ssh = {
              type = "app";
              program = "${vmScripts.ssh}/bin/k8s-vm-ssh";
            };
            k8s-start-all = {
              type = "app";
              program = "${vmScripts.startAll}/bin/k8s-start-all";
            };
            k8s-vm-wipe = {
              type = "app";
              program = "${vmScripts.wipe}/bin/k8s-vm-wipe";
            };
            k8s-cluster-rebuild = {
              type = "app";
              program = "${vmScripts.clusterRebuild}/bin/k8s-cluster-rebuild";
            };

            # Import Nix-built images into a running cluster's containerd
            # (live-cluster analogue of the boot-time preload module).
            k8s-image-import = {
              type = "app";
              program = "${imageImportScripts.imageImport}/bin/k8s-image-import";
            };

            # Certificates (copies build-time certs to ./certs/ for inspection)
            k8s-gen-certs = {
              type = "app";
              program = "${certs.genCerts}/bin/k8s-gen-certs";
            };

            # Secrets pre-generation (offline, into ./secrets/)
            k8s-gen-secrets = {
              type = "app";
              program = "${secretsGen.genSecrets}/bin/k8s-gen-secrets";
            };
          }

          # Rendered manifests
          // {
            k8s-render-manifests = {
              type = "app";
              program = "${renderScript}/bin/k8s-render-manifests";
            };
          }

          # Chaos / failover test
          // {
            k8s-chaos-failover = {
              type = "app";
              program = "${chaosScripts.chaosFailover}/bin/k8s-chaos-failover";
            };
          }

          # Sustained multi-hour soak test (all buses + rolling failover).
          // {
            k8s-soak-test = {
              type = "app";
              program = "${soakScripts.soakTest}/bin/k8s-soak-test";
            };
          }

          # All-clients integration smoke test: pub+sub every bus at once
          # (HA modes) and assert delivery; exits non-zero on any loss.
          // {
            k8s-integration-test = {
              type = "app";
              program = "${integrationScripts.integrationTest}/bin/k8s-integration-test";
              meta.description = "Run all four bus clients at once and assert every message is delivered (exits non-zero on loss)";
            };
          }

          # Per-bus client throughput/latency benchmark (live cluster).
          // {
            k8s-client-bench = {
              type = "app";
              program = "${benchScripts.clientBench}/bin/k8s-client-bench";
            };
          }

          # proto-bench host harness: drives benchcli across the region-agents,
          # assembles run.json, renders results (design §9.3).
          // {
            k8s-proto-bench = {
              type = "app";
              program = "${protoBenchScripts.protoBench}/bin/k8s-proto-bench";
              meta.description = "Drive benchcli across the region-agents and render a proto-bench run (run.json + results.md)";
            };
          }

          # Go pub/sub CLI clients + the hermetic micro-benchmark runner.
          // clients.apps

          # Lifecycle test apps
          // (lifecycle.apps or {});
          in
          # Give every app a `meta.description` so `nix flake check` is
          # warning-clean (covers flake.nix apps, clients.apps, lifecycle.apps).
          lib.mapAttrs (name: app:
            app // {
              meta = (app.meta or { }) // {
                description = app.meta.description or "message-bus-examples: ${name}";
              };
            }
          ) rawApps
        );
      }
    );
}
