# nix/microvm.nix
#
# Parametric MicroVM generator for K8s cluster nodes.
# Creates a MicroVM runner for a given node (cp0, w1, w2, w3).
#
# Certs are generated at Nix build time and baked into the VM image
# via activation scripts (copied from /nix/store to /var/lib/kubernetes/pki/).
#
# Returns microvm.declaredRunner - a script that starts the VM.
#
{
  pkgs,
  lib,
  microvm,
  k8sModule,
  preloadModule,  # NixOS module: preload Nix-built bus images into containerd
  bootstrapModule,
  nixpkgs,
  system,
  nodeName,       # "cp0", "w1", "w2", "w3"
  role,           # "control-plane" or "worker"
  nodePki,        # Per-node PKI bundle (from certs.mkNodePki)
  k8sManifests,   # Rendered k8s manifests derivation
  k8sSecrets ? null,       # Pre-generated Secret manifests (from secrets.nix; null if absent)
  sshPubKey ? null,        # SSH ED25519 public key for authorized_keys (from secrets.nix; null if absent)
  busImages ? [ ],         # List of { name; tag; archive; } Nix-built OCI images to preload
}:
let
  constants = import ./constants.nix;

  hostname = constants.getHostname nodeName;
  consolePorts = constants.getConsolePorts nodeName;
  resources = constants.getVmResources role;

  nodeIp4 = constants.network.ipv4.${nodeName};
  nodeIp6 = constants.network.ipv6.${nodeName};
  mac = constants.network.macs.${nodeName};
  tap = constants.network.taps.${nodeName};

  pki = constants.k8s.pkiDir;

  vmConfig = nixpkgs.lib.nixosSystem {
    inherit system;

    modules = [
      microvm.nixosModules.microvm
      k8sModule
      preloadModule
      bootstrapModule

      ({ config, pkgs, ... }: {
        system.stateVersion = "26.05";
        nixpkgs.hostPlatform = system;

        microvm = {
          hypervisor = "qemu";
          mem = resources.memoryMB;
          vcpu = resources.vcpus;

          shares = [{
            tag = "ro-store";
            source = "/nix/store";
            mountPoint = "/nix/.ro-store";
            proto = "9p";
          }];

          volumes = [{
            image = "${hostname}-data.img";
            mountPoint = "/var/lib";
            size = 20480;  # 20GB writable volume for containerd, etcd, kubelet
          }];

          interfaces = [{
            type = "tap";
            id = tap;
            mac = mac;
          }];

          qemu = {
            serialConsole = false;
            extraArgs = [
              "-name" "${hostname},process=${hostname}"
              "-serial" "tcp:127.0.0.1:${toString consolePorts.serial},server,nowait"
              "-device" "virtio-serial-pci"
              "-chardev" "socket,id=virtcon,port=${toString consolePorts.virtio},host=127.0.0.1,server=on,wait=off"
              "-device" "virtconsole,chardev=virtcon"
            ];
          };
        };

        boot.kernelParams = [
          "console=ttyS0,115200"
          "console=hvc0"
        ];

        networking.hostName = hostname;

        # Static dual-stack IP via systemd-networkd
        systemd.network = {
          enable = true;
          networks."10-tap" = {
            matchConfig.Name = "enp*";
            networkConfig = {
              Address = [ "${nodeIp4}/24" "${nodeIp6}/64" ];
              Gateway = constants.network.gateway4;
              DHCP = "no";
              IPv6AcceptRA = false;
            };
            routes = [
              { Gateway = constants.network.gateway4; }
            ];
          };
        };
        networking.useDHCP = false;

        # SSH: key-based auth (preferred) + password fallback for testing
        services.openssh = {
          enable = true;
          settings = {
            PasswordAuthentication = lib.mkForce true;
            PermitRootLogin = lib.mkForce "yes";
            KbdInteractiveAuthentication = lib.mkForce true;
          };
        };
        users.users.root = {
          password = constants.ssh.password;
          openssh.authorizedKeys.keys =
            lib.optional (sshPubKey != null) sshPubKey;
        };

        # ─── PKI: copy build-time certs to /var/lib/kubernetes/pki/ ────
        # Certs are in /nix/store (read-only). Copy to writable PKI dir at boot.
        system.activationScripts.k8s-pki = ''
          mkdir -p ${pki}
          cp -f ${nodePki}/* ${pki}/
          chmod 600 ${pki}/*.key 2>/dev/null || true
          chmod 644 ${pki}/*.crt ${pki}/*.pub 2>/dev/null || true
          chmod 600 ${pki}/*-kubeconfig 2>/dev/null || true
          chmod 644 ${pki}/kubelet-config.yaml 2>/dev/null || true
        '';

        # K8s services
        services.k8s = {
          enable = true;
          inherit role;
          inherit nodeName;
          nodeIp4 = nodeIp4;
          nodeIp6 = nodeIp6;
        };

        # Preload Nix-built message-bus images into containerd on every
        # node (pods may schedule anywhere), before kubelet starts.
        services.k8s-image-preload = {
          enable = true;
          images = busImages;
        };

        # First-boot GitOps bootstrap only on cp0.
        services.k8s-gitops-bootstrap = {
          enable = (nodeName == "cp0");
          manifestsPath = k8sManifests;
          secretsPath = k8sSecrets;
        };
      })
    ];
  };
in
vmConfig.config.microvm.declaredRunner
