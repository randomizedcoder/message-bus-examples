# nix/gitops/env/monitoring/targets.nix
#
# Prometheus scrape-target lists, built from constants. Every job below uses
# static_configs (the targets are known, so no kubernetes_sd/RBAC is needed);
# this module just assembles the quoted-target YAML arrays consumed by the
# prometheus-config scrape jobs. Split out of the old monolithic monitoring.nix
# with no behaviour change — the strings are byte-identical.
{ lib }:
let
  constants = import ../../../constants.nix;
  mon   = constants.monitoring;
  domain = "svc.${constants.k8s.clusterDomain}";
  natsC = constants.messageBus.nats;
  rmqC  = constants.messageBus.rabbitmq;
  vkC   = constants.messageBus.valkey;
  mqttC = constants.messageBus.mqtt;
  wlC   = constants.messageBus.workloads;
  rpcC  = constants.messageBus.rpc;
  pb    = constants.protoBench;

  # Inline YAML array of quoted targets: ['a:1', 'b:2'].
  mkTargets = items: "[" + builtins.concatStringsSep ", " (map (t: "'${t}'") items) + "]";
in
{
  node = mkTargets (map (ip: "${ip}:${toString constants.nodeExporter.port}") constants.allNodeIps4);
  ciliumAgent    = mkTargets (map (ip: "${ip}:${toString constants.hubble.agentMetricsPort}") constants.allNodeIps4);
  ciliumOperator = mkTargets (map (ip: "${ip}:${toString constants.hubble.operatorMetricsPort}") constants.allNodeIps4);
  hubble         = mkTargets (map (ip: "${ip}:${toString constants.hubble.hubbleMetricsPort}") constants.allNodeIps4);
  rabbitmq = mkTargets (map
    (i: "rabbitmq-${toString i}.rabbitmq-headless.${rmqC.namespace}.${domain}:${toString rmqC.prometheusPort}")
    (lib.range 0 (rmqC.replicas - 1)));
  # Host-side soak clients: hostBridgeIP:(base+0 .. base+count-1). Absent ones
  # just show DOWN — the soak harness fills the range as it launches clients.
  clients = mkTargets (map
    (i: "${mon.hostBridgeIP}:${toString (mon.clientMetricsBasePort + i)}")
    (lib.range 0 (mon.clientMetricsCount - 1)));

  # NATS exporter sidecars — one prometheus-nats-exporter per NATS pod, each
  # polling only its OWN server's monitoring endpoint over localhost (see the
  # sidecar in nats.nix). Prometheus scrapes each pod over the headless Service
  # by pod FQDN (:7777), the same per-pod pattern as redis_exporter/Valkey.
  # Running one exporter per server (co-located) is the exporter's own guidance:
  # a single aggregating exporter returns HTTP 500 for the WHOLE scrape whenever
  # any one monitored server is briefly unreachable, so under the soak's rolling
  # node faults it was down ~60% of the time and gapped every NATS panel. Per
  # pod, a single node fault gaps only that one server's target.
  nats = mkTargets (
    (map (i: "nats-${toString i}.nats-headless.${natsC.namespace}.${domain}:${toString mon.natsExporter.port}")
      (lib.range 0 (natsC.replicas - 1)))
    ++ [ "nats-leaf-0.nats-leaf-headless.${natsC.namespace}.${domain}:${toString mon.natsExporter.port}" ]);

  # redis_exporter sidecars, one per Valkey pod, scraped over the headless
  # Service by pod FQDN (:9121).
  redis = mkTargets (map
    (i: "valkey-${toString i}.valkey-headless.${vkC.namespace}.${domain}:${toString mon.redisExporter.port}")
    (lib.range 0 (vkC.replicas - 1)));

  # mosquitto sysexporter sidecars, one per MQTT pod, scraped over the headless
  # Service by pod FQDN (:9234). Each exporter bridges only its own broker's
  # $SYS tree ($SYS is never bridged), so per-pod scraping gives per-broker
  # stats — the same per-pod pattern as the NATS/redis exporters.
  mqtt = mkTargets (map
    (i: "mqtt-${toString i}.mqtt-headless.${mqttC.namespace}.${domain}:${toString mon.mosquittoExporter.port}")
    (lib.range 0 (mqttC.replicas - 1)));

  # proto-bench region agents — one pod per region, each addressable by a
  # stable DNS name via the headless `region-agents` Service (hostname +
  # subdomain on the Deployment pods). Scraped per-pod on the metrics port.
  workloads = mkTargets (map
    (node: "region-agent-${wlC.regions.${node}}.region-agents.${wlC.namespace}.${domain}:${toString wlC.metricsPort}")
    constants.nodeNames);

  # proto-bench host driver(s): hostBridgeIP:(base+0 .. base+count-1), one port
  # per concurrent benchcli process. Absent ones just show DOWN (like the soak
  # client range), so the harness can fill them as it launches drivers.
  driver = mkTargets (map
    (i: "${mon.hostBridgeIP}:${toString (pb.hostMetricsBasePort + i)}")
    (lib.range 0 (pb.hostMetricsCount - 1)));

  # Host rpc-benchmark rpc_* /metrics (§22): a single host port over the k8sbr0
  # bridge, past the proto-bench driver's 9300-9307 block. DOWN until a benchmark
  # runs with -metrics-addr, exactly like the driver/soak host ranges.
  rpcBench = mkTargets [ "${mon.hostBridgeIP}:${toString rpcC.benchMetricsPort}" ];
}
