# Resilience / failover testing

`nix run .#k8s-chaos-failover` exercises each clustered bus's ability to
keep messages flowing while a node is lost.

## What it does

Each round:

1. Starts a host-side **subscriber** per bus (the Go CLI clients),
   connected to a *stable* node's NodePort (cp0, which is never killed —
   this isolates in-cluster failover from host↔node reachability).
2. Kills one MicroVM (`k8s-vm-stop-one`), rotating through `cp1,cp2,w3`.
3. Publishes a uniquely-tagged probe to each bus once per second until the
   subscriber receives it, recording the **recovery time** (seconds from
   kill to first post-kill message delivered).
4. Restarts the node (`k8s-vm-start-one`) and settles before the next
   round.

Results are written to `chaos-logs/summary.tsv` (`round`, `node`, `bus`,
`recovery_sec`).

## Usage

```bash
nix run .#k8s-chaos-failover -- \
  --rounds=6 --interval=45 --buses=nats,mqtt,valkey,rabbitmq
```

Requires the cluster to be up and SSH-reachable at cp0 (used to read the
RabbitMQ/ValKey passwords from their Secrets).

## Expected behaviour per bus

| Bus | Failover mechanism | Notes |
|-----|--------------------|-------|
| **NATS** | JetStream Raft re-election; client auto-reconnect | Fast recovery; the client library reconnects to a surviving route. |
| **RabbitMQ** | `pause_minority` + quorum queues; AMQP reconnect | The client reconnects; a queue's leader is re-elected among survivors. |
| **MQTT (Mosquitto)** | Full-mesh bridges between the 3 brokers | A surviving broker still serves; the paho client auto-reconnects to the NodePort. |
| **ValKey** | Sentinel promotes a replica to primary | **Caveat:** Valkey pub/sub is fire-and-forget and not persisted — messages in flight during the promotion window can be dropped. This is expected. |
