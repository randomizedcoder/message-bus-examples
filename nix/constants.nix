# nix/constants.nix
#
# Shared constants for K8s MicroVM cluster infrastructure.
# All network params, serial ports, k8s CIDRs, cert config, lifecycle timeouts.
#
# Topology: 3 control planes (cp0, cp1, cp2) + 1 worker (w3)
#
rec {
  # ─── Node Configuration ──────────────────────────────────────────────
  nodeNames = [ "cp0" "cp1" "cp2" "w3" ];

  # ─── Network Configuration ──────────────────────────────────────────
  network = {
    bridge = "k8sbr0";

    # Per-node TAP devices
    taps = {
      cp0 = "k8stap0";
      cp1 = "k8stap1";
      cp2 = "k8stap2";
      w3  = "k8stap3";
    };

    # Host bridge addresses (dual-stack)
    gateway4 = "10.33.33.1";
    gateway6 = "fd33:33:33::1";
    subnet4 = "10.33.33.0/24";
    subnet6 = "fd33:33:33::/64";

    # Per-node IP addresses
    ipv4 = {
      cp0 = "10.33.33.10";
      cp1 = "10.33.33.11";
      cp2 = "10.33.33.12";
      w3  = "10.33.33.13";
    };
    ipv6 = {
      cp0 = "fd33:33:33::10";
      cp1 = "fd33:33:33::11";
      cp2 = "fd33:33:33::12";
      w3  = "fd33:33:33::13";
    };

    # Per-node MAC addresses
    macs = {
      cp0 = "02:00:0a:21:21:10";
      cp1 = "02:00:0a:21:21:11";
      cp2 = "02:00:0a:21:21:12";
      w3  = "02:00:0a:21:21:13";
    };
  };

  # ─── Kubernetes Network CIDRs ──────────────────────────────────────
  k8s = {
    podCidr4 = "10.244.0.0/16";
    podCidr6 = "fd44:44:44::/48";
    serviceCidr4 = "10.96.0.0/12";
    serviceCidr6 = "fd96:96:96::/108";

    # First service IP (kubernetes.default)
    apiServiceIp = "10.96.0.1";

    # API endpoint via host-side load balancer (haproxy on bridge IP)
    apiEndpoint = "https://${network.gateway4}:6443";

    # DNS
    clusterDomain = "cluster.local";
    dnsServiceIp = "10.96.0.10";

    # PKI directory inside VMs
    pkiDir = "/var/lib/kubernetes/pki";

    # Cert output directory on host
    certDir = "./certs";
  };

  # ─── Serial Console Configuration ──────────────────────────────────
  # Each node gets 10 ports starting at base 25500.
  # +0 = serial (ttyS0), +1 = virtio (hvc0), +2-9 = reserved
  console = {
    portBase = 25500;
    serialOffset = 0;
    virtioOffset = 1;

    nodeBlocks = {
      cp0 = 0;    # 25500-25509
      cp1 = 10;   # 25510-25519
      cp2 = 20;   # 25520-25529
      w3  = 30;   # 25530-25539
    };
  };

  # ─── VM Resources ──────────────────────────────────────────────────
  vm = {
    controlPlane = {
      memoryMB = 8191;  # 8GB (avoid exact power-of-2 — QEMU hangs)
      vcpus = 4;
    };
    worker = {
      memoryMB = 6143;  # 6GB (avoid exact power-of-2 — QEMU hangs)
      vcpus = 2;
    };
  };

  # ─── Observability ─────────────────────────────────────────────────
  nodeExporter = {
    port = 9100;
    listenAddress = "0.0.0.0";  # firewall disabled; bridge reachable
  };

  hubble = {
    uiNodePort    = 31234;  # Hubble UI (HTTP)
    relayNodePort = 31245;  # Hubble Relay gRPC (for `hubble` CLI)
    # Metrics ports live on cilium-agent's host network (hostNetwork=true).
    agentMetricsPort    = 9962;
    operatorMetricsPort = 9963;
    hubbleMetricsPort   = 9965;
  };

  # ─── Cilium Ingress + L2 announcements ─────────────────────────────
  # Cilium runs the cluster's only L7 proxy (Envoy). The built-in
  # ingress controller exposes a single LoadBalancer Service
  # (`cilium-ingress` in kube-system) whose IP is pulled from the
  # LoadBalancer IP pool below and advertised to the LAN via L2 ARP.
  # Host /etc/hosts points every matrix/element/hookshot/maubot name
  # at `vip`. Phase-2: swap L2 for BGP, same VIP.
  cilium = {
    ingress = {
      # Single-IP LB pool. Kept to one IP so the cilium-ingress Service
      # assignment is deterministic — host /etc/hosts entries point
      # every Matrix hostname at this IP. Expand the range when a
      # second LB Service is added.
      vip      = "10.33.33.50";
      vipStart = "10.33.33.50";
      vipStop  = "10.33.33.50";
      # VM-side NIC name (cloud-init renames virtio-net to enp0s4 on
      # these guests; verify with `ip -br link` before first apply if
      # you change the VM image).
      nic      = "enp0s4";
    };
  };

  # ─── Helm chart pins (rendered at Nix build time) ──────────────────
  # Update these by running:
  #   nix-prefetch-url --type sha256 <url>
  #   nix hash convert --hash-algo sha256 --to sri <raw>
  helmCharts = {
    cilium = {
      version = "1.19.3";
      url     = "https://helm.cilium.io/cilium-1.19.3.tgz";
      hash    = "sha256-yOBd+eq/kBnmL1ED4fNYFLTxtDkW+IUZ5a5ONsaapCs=";
    };
    argocd = {
      version = "9.5.11";  # appVersion v3.3.9 — fixes GHSA-3v3m-wc6v-x4x3 (secret extraction via ServerSideDiff)
      url     = "https://github.com/argoproj/argo-helm/releases/download/argo-cd-9.5.11/argo-cd-9.5.11.tgz";
      hash    = "sha256-TyvlRDv3PifSR0mcO/un/24CJo2UzIBHeu8j4a6osB8=";
    };
  };

  # ─── Rancher local-path-provisioner (PV backend for bus PVCs) ──────
  localPathProvisioner = {
    version = "v0.0.34";
    url  = "https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.34/deploy/local-path-storage.yaml";
    hash = "sha256-+rjW6JM+RPivc5hgP7YxIuTqZJDwr4NUkQjWhkft2ek=";
  };

  # ─── Message buses ─────────────────────────────────────────────────
  # All four run as Nix-built OCI images (nix/images/*.nix) preloaded
  # into containerd at boot (nix/image-preload-module.nix), deployed as
  # hand-written clustered StatefulSets (nix/gitops/env/*.nix). Each
  # exposes a host-reachable NodePort for the Go CLI clients.
  #
  # `image`/`tag` here must match what nix/images/<bus>.nix builds and
  # what the StatefulSet references (imagePullPolicy: Never).
  # NOTE: image names carry a dotted "registry host" prefix
  # (messagebus.local) so containerd does NOT normalise them to
  # docker.io/*. The preloaded ref then matches the StatefulSet's
  # `image:` string exactly, and imagePullPolicy: Never never reaches
  # out to a registry.
  messageBus = {
    nats = {
      namespace   = "nats";
      image       = "messagebus.local/nats";
      tag         = "2.14.6";
      replicas    = 3;
      clientPort  = 4222;
      clusterPort = 6222;
      monitorPort = 8222;
      nodePort    = 30422;   # client 4222 → host :30422
    };
    rabbitmq = {
      namespace     = "rabbitmq";
      image         = "messagebus.local/rabbitmq";
      tag           = "4.2.5";
      replicas      = 3;
      amqpPort      = 5672;
      mgmtPort      = 15672;
      distPort      = 25672;  # inter-node Erlang distribution
      epmdPort      = 4369;
      nodePortAmqp  = 30567;  # AMQP 5672 → host :30567
      nodePortMgmt  = 30672;  # mgmt UI 15672 → host :30672
    };
    # MQTT: Eclipse Mosquitto, 3-node full-mesh bridged. Each broker
    # forwards its locally-published messages OUT to the other two
    # (topic # out + try_private), so a publish on any node reaches
    # subscribers on the others without loops or duplicate delivery, and
    # the set survives a node loss. (NanoMQ was dropped — insecure in
    # nixpkgs; EMQX isn't packaged.)
    mqtt = {
      namespace = "mqtt";
      image     = "messagebus.local/mosquitto";
      tag       = "2.1.2";
      replicas  = 3;
      mqttPort  = 1883;
      nodePort  = 30883;   # MQTT 1883 → host :30883
    };
    valkey = {
      namespace    = "valkey";
      image        = "messagebus.local/valkey";
      tag          = "9.1.1";
      replicas     = 3;      # 1 primary + 2 replicas
      clientPort   = 6379;
      sentinelPort = 26379;
      masterName   = "mymaster";
      nodePort     = 30637;  # round-robin client 6379 → host :30637 (default path)
      # Per-pod NodePorts for host-reachable Sentinel primary discovery: pod
      # valkey-N is reachable at <node>:${nodePortClientBase+N} (client) and
      # <node>:${nodePortSentinelBase+N} (sentinel). Pods announce these so a
      # host FailoverClient can follow failover.
      nodePortClientBase   = 30640;  # valkey-{0,1,2} client   → 30640/30641/30642
      nodePortSentinelBase = 30650;  # valkey-{0,1,2} sentinel → 30650/30651/30652
    };
  };

  # ─── Chaos / failover test defaults ────────────────────────────────
  chaos = {
    defaultRounds         = 10;
    defaultIntervalSec    = 60;
    defaultPostRoundWait  = 60;
    defaultWarmupSec      = 15;
    defaultLogDir         = "./chaos-logs";
  };

  # ─── ArgoCD service (NodePort reachable from host) ─────────────────
  argocd = {
    nodePortHttps = 30443;
  };

  # ─── SSH Configuration ─────────────────────────────────────────────
  ssh = {
    password = "k8s";
    user = "root";
  };

  # ─── Lifecycle Test Configuration ──────────────────────────────────
  lifecycle = {
    pollInterval = 1;

    timeouts = {
      build = 900;
      processStart = 5;
      serialReady = 30;
      virtioReady = 45;
      sshReady = 90;
      certInject = 30;
      serviceReady = 90;
      k8sHealth = 90;
      shutdown = 30;
      waitExit = 60;
    };

    # Cluster-level test timeouts
    clusterTimeouts = {
      nodesReady = 180;
      ciliumReady = 120;
      workloadsReady = 120;
    };
  };

  # ─── GitOps Configuration ────────────────────────────────────────────
  gitops = {
    repoURL = "https://github.com/randomizedcoder/message-bus-examples.git";
    targetRevision = "main";
    renderedPath = "rendered";
  };

  # ─── Helper Functions ──────────────────────────────────────────────

  # Get console ports for a node
  getConsolePorts = node: {
    serial = console.portBase + console.nodeBlocks.${node} + console.serialOffset;
    virtio = console.portBase + console.nodeBlocks.${node} + console.virtioOffset;
  };

  # Get hostname for a node
  getHostname = node: "k8s-${node}";

  # Get process name for pgrep matching
  getProcessName = node: getHostname node;

  # Get timeout for a phase (no per-node overrides for now)
  getTimeout = _node: phase: lifecycle.timeouts.${phase};

  # Get all node IPv4 addresses as a list
  allNodeIps4 = builtins.map (n: network.ipv4.${n}) nodeNames;

  # Get all node IPv6 addresses as a list
  allNodeIps6 = builtins.map (n: network.ipv6.${n}) nodeNames;

  # Get VM resources for a role
  getVmResources = role:
    if role == "control-plane" then vm.controlPlane
    else vm.worker;
}
