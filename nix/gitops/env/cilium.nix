# nix/gitops/env/cilium.nix
#
# Cilium — rendered-manifests pattern.
#
# At Nix build time: `helm template` is run against the pinned Cilium chart
# tarball with the values below. The result is written to
# rendered/cilium/install.yaml as plain YAML (CRDs included).
# ArgoCD reads that directory via a path-source Application — no in-cluster
# Helm templating.
#
# For the first-boot bootstrap, `install.yaml` is also applied directly by
# the gitops-bootstrap systemd unit on cp0 (since we need Cilium up before
# ArgoCD can sync anything).
#
{ pkgs, lib, helm }:
let
  constants = import ../../constants.nix;

  valuesYaml = ''
    # Cilium Helm values — rendered at Nix build time.
    kubeProxyReplacement: true
    k8sServiceHost: "${constants.network.gateway4}"
    k8sServicePort: "6443"

    ipam:
      mode: kubernetes

    ipv4:
      enabled: true
    ipv6:
      enabled: true

    bpf:
      masquerade: true

    # ─── Hubble: flow visibility, UI, metrics ───────────────────────
    hubble:
      enabled: true
      # Test env — mTLS disabled so `hubble` CLI from host just works.
      tls:
        enabled: false
      peerService:
        enabled: true
      relay:
        enabled: true
        startupProbe:
          failureThreshold: 40
        service:
          type: NodePort
          nodePort: ${toString constants.hubble.relayNodePort}
      ui:
        enabled: true
        service:
          type: NodePort
          nodePort: ${toString constants.hubble.uiNodePort}
      metrics:
        enableOpenMetrics: true
        enabled:
          - dns
          - drop
          - tcp
          - flow
          - port-distribution
          - icmp
          - "httpV2:exemplars=true;labelsContext=source_ip,source_namespace,source_workload,destination_ip,destination_namespace,destination_workload,traffic_direction"

    # ─── Expose cilium-agent + operator Prometheus endpoints ────────
    prometheus:
      enabled: true
      port: ${toString constants.hubble.agentMetricsPort}

    operator:
      replicas: 1
      prometheus:
        enabled: true
        port: ${toString constants.hubble.operatorMetricsPort}
      resources:
        requests:
          cpu: 50m
          memory: 128Mi

    resources:
      requests:
        cpu: 100m
        memory: 256Mi

    # ─── Ingress (replaces ingress-nginx) ───────────────────────────
    # Cilium's built-in Envoy serves the cluster's Ingress objects.
    # Exposed through a single LoadBalancer Service (`cilium-ingress`
    # in kube-system) whose ExternalIP is assigned from the
    # CiliumLoadBalancerIPPool below and advertised on the LAN via L2
    # ARP announcements. Phase-2: swap l2announcements → bgpControlPlane,
    # same VIP, same Service, same Ingress — no further rewrite.
    ingressController:
      enabled: true
      default: true              # make "cilium" the default IngressClass
      loadbalancerMode: shared   # one cilium-ingress Service for all Ingresses
      enforceHttps: false        # redirect at the app layer if needed
      service:
        type: LoadBalancer

    # ─── L2 announcements (LAN-scoped VIP advertisement) ────────────
    # Needed for the LoadBalancer Service above to actually be
    # reachable from the host without a real cloud LB. Cilium elects a
    # single agent to ARP-reply for the VIP; on node loss another takes
    # over (see chaos-failover test).
    l2announcements:
      enabled: true

    # L2 announcements talk to the K8s API a lot (leases for VIP
    # ownership). Bump the client-side QPS/burst per Cilium docs to
    # avoid throttling with a small cluster + tight leases.
    k8sClientRateLimit:
      qps: 10
      burst: 20
  '';

  rendered = helm.renderChart {
    name        = "cilium";
    releaseName = "cilium";
    namespace   = "kube-system";
    chart       = constants.helmCharts.cilium;
    values      = valuesYaml;
  };
in
{
  manifests = [
    # Fully-rendered multi-doc YAML from `helm template`.
    {
      name = "cilium/install.yaml";
      source = "${rendered}/install.yaml";
    }
    # Audit copy of the values used at render time.
    {
      name = "cilium/values.yaml";
      content = valuesYaml;
    }
    # ─── LoadBalancer IP pool for cilium-ingress ──────────────────────
    # Kept as raw YAML (not Helm-templated) so the pool/policy are
    # co-located with the module that enables the feature. Block is
    # deliberately small — we only need one VIP today (ingress).
    {
      name = "cilium/lb-ip-pool.yaml";
      content = ''
        # v2 is stable for CiliumLoadBalancerIPPool in 1.19; policy below
        # stays at v2alpha1 because CiliumL2AnnouncementPolicy hasn't
        # graduated yet.
        apiVersion: cilium.io/v2
        kind: CiliumLoadBalancerIPPool
        metadata:
          name: lab-lb-pool
          # No sync-wave — must land in the same wave as install.yaml
          # (wave 0). Otherwise ArgoCD blocks waiting for the
          # cilium-ingress Service to become healthy before applying
          # the pool that assigns its LoadBalancerIP. Circular dep.
        spec:
          blocks:
          - start: "${constants.cilium.ingress.vipStart}"
            stop:  "${constants.cilium.ingress.vipStop}"
      '';
    }
    # ─── L2 announcement policy ────────────────────────────────────────
    # Cilium labels the auto-created cilium-ingress Service with
    # `cilium.io/ingress: "true"` (confirmed in the rendered install.yaml).
    # Scoping the selector to that label means we only ARP-announce the
    # ingress VIP for now — future LB Services need their own policy
    # (or a broadened selector) to opt in.
    # The interface name is the VM-side NIC (verified via `ip -br link`
    # on cp0: enp0s4, the virtio-net device cloud-init renames to).
    {
      name = "cilium/l2-announcement-policy.yaml";
      content = ''
        apiVersion: cilium.io/v2alpha1
        kind: CiliumL2AnnouncementPolicy
        metadata:
          name: lab-l2
          # Same wave as lb-ip-pool.yaml (wave 0): the pool feeds the
          # Service EXTERNAL-IP, the policy makes that IP ARP-reachable.
          # ArgoCD won't mark the Service healthy without both.
        spec:
          serviceSelector:
            matchLabels:
              cilium.io/ingress: "true"
          interfaces:
          - ${constants.cilium.ingress.nic}
          externalIPs: true
          loadBalancerIPs: true
      '';
    }
    # Path-source Application — ArgoCD applies install.yaml and ignores the
    # Application CR file itself (directory.exclude).
    {
      name = "cilium/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: cilium
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/cilium
            directory:
              recurse: false
              exclude: '{application.yaml,values.yaml}'
          destination:
            server: https://kubernetes.default.svc
            namespace: kube-system
          syncPolicy:
            automated:
              prune: true
              selfHeal: true
            syncOptions:
              - ServerSideApply=true
              - CreateNamespace=true
      '';
    }
  ];
}
