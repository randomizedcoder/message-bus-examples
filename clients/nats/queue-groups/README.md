# nats-queue-groups — load-balanced subscribers

A self-contained demo of NATS **queue groups**: when subscribers share a queue
name, each message goes to only **one** member of the group — automatic load
balancing across competing consumers.

> NATS docs: [Queue Groups](https://docs.nats.io/concepts/queue-groups)

## Topology

N workers all `QueueSubscribe` to `tasks` under the queue group `workers`. The
publisher sends M messages; NATS delivers each to exactly one randomly chosen
worker.

```mermaid
flowchart LR
  P[publisher] -->|M messages| N(("NATS<br/>cluster"))
  N -. "one message each" .-> W0[worker 0]
  N -. .-> W1[worker 1]
  N -. .-> W2[worker 2]
  subgraph G["queue group &quot;workers&quot;"]
    W0
    W1
    W2
  end
```

Contrast with plain pub/sub (see [`../subjects`](../subjects)), where
**every** subscriber receives **every** message.

## What it demonstrates

- Members of a queue group are **competing consumers**: each message is
  delivered once, to one member.
- The group name is chosen by the **subscribers** (`QueueSubscribe(subj, "workers", …)`),
  not configured on the server.
- Distribution adjusts automatically as members join or leave, with no
  configuration change.
- Over many messages the load spreads across the whole group (the demo asserts
  more than one worker participated).

## Run it

```bash
nix run .#nats-queue-groups                                          # defaults to 127.0.0.1:30422
nix run .#nats-queue-groups -- -count 12 -workers 4 -addr 10.33.33.10:30422
```

Flags: `-count` (messages to publish, default 9), `-workers` (group members,
default 3), `-addr`.

> **Off-box addressing.** The bus is reachable on any node IP at NodePort
> `30422`; pass `-addr <node-ip>:30422` when there is no tunnel to
> `127.0.0.1`.

The program is **self-verifying**: it exits non-zero unless every message was
handled exactly once and the load spread across more than one worker.

## Sample output

```
  [worker 0] handled task-3
  [worker 3] handled task-1
  ...
summary (messages handled per worker):
  worker 0: 5
  worker 1: 2
  worker 2: 2
  worker 3: 3
total handled: 12/12 across 4/4 workers
PASS: each message was delivered to exactly one worker, load balanced across the group
```
