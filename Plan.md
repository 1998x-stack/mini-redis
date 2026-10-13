# mini-redis — Architecture-Driven Development Plan

> `main` must always represent the newest integrated, validated state. Historical `v0.x` branches are frozen learning snapshots.

## 1. Dependency-first roadmap

```text
v0.1 request/state/persistence semantics
  ↓
v0.2 event-loop scheduling
  ↓
v0.3 dictionary internals
  ↓
v0.4 logical types + physical encodings
  ↓
v0.5 persistence depth
  ↓
v0.6 memory pressure / eviction
  ↓
v0.7 transactions + Pub/Sub
  ↓
v0.8 replication
  ↓
v0.9 Sentinel-style failover
  ↓
v0.10 Cluster / hash slots
  ↓
v0.11 RESP3
  ↓
v0.12 observability/performance
```

## 2. Completed milestones

### v0.1 — Core vertical slice
RESP2 → command registry → Store → String/TTL → AOF.

### v0.2 — Nonblocking networking
Linux epoll reactor, client buffers, partial frames/writes, pipelines, backpressure and timers.

### v0.3 — Dictionary internals
Explicit buckets/collisions, `ht[0]/ht[1]`, bounded incremental rehash and Object metadata.

### v0.4 — Core types and adaptive encodings
List/Hash/Set/ZSet, compact/general encodings, WRONGTYPE, atomic collection mutation and SkipList rank.

### v0.5 — Persistence depth
Logical SnapshotEntry boundary, checksummed RDB-like snapshot, absolute TTL, crash-safe publication, buffered AOF rewrite and startup precedence.

### v0.6 — Memory pressure and sampled eviction

Primary question:

> Why are expiration and eviction separate, and why approximate LRU/LFU with samples instead of maintaining perfect global ordering?

Implemented:

- deterministic logical-byte estimator distinct from Go heap/RSS;
- runtime `memorypolicy.Manager` layered above the durable Store;
- `maxmemory`, `noeviction`, `allkeys-random`, sampled `allkeys-lru` and sampled `allkeys-lfu`;
- last-access metadata, probabilistic/sublinear LFU counters and time decay;
- bounded candidate scoring per victim selection;
- expiration cleanup counted separately from eviction;
- logical snapshot rollback for rejected noeviction memory-growing writes;
- removal-only commands remain usable while above noeviction limit;
- eviction-generated logical `DEL` appended to AOF;
- policy metadata excluded from RDB/AOF;
- deterministic clock/RNG tests and `make eviction-demo`.

Critical invariants:

- time expiry is not an eviction success;
- rejected noeviction writes leave logical state and AOF unchanged;
- eviction removes complete key + TTL metadata;
- restart reconstructs eviction results without reconstructing access heat;
- per-selection candidate scoring is bounded by configured samples.

## 3. v0.7 — Transactions + Pub/Sub

### Primary question

**How can Redis queue commands, detect optimistic conflicts, execute a transaction as one ordered unit, and support connections whose role changes into Pub/Sub delivery?**

### Required implementation

#### MULTI / EXEC / DISCARD
- per-client transaction state owned by the network/session layer;
- `MULTI` enters queueing mode;
- queue command syntax/arity errors before execution;
- `EXEC` runs queued operations in original order inside one explicit server transaction boundary;
- `DISCARD` clears transaction state;
- define command errors inside EXEC without rolling back already executed Redis-style operations.

#### WATCH / UNWATCH
- Store-level logical key version counters independent of physical Dict layout;
- mutations increment watched versions including deletion/expiration/eviction where appropriate;
- `WATCH` captures versions before MULTI;
- `EXEC` aborts when a watched key version changed;
- `UNWATCH`, successful EXEC and DISCARD clear watch state.

#### AOF / maxmemory interaction
- define how EXEC's successful mutations are emitted as one ordered AOF batch;
- keep noeviction/eviction semantics consistent inside transaction execution;
- make the transaction boundary explicit rather than calling normal client execution recursively.

#### Pub/Sub
- basic `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`;
- broker/channel registry separated from durable Store state;
- subscribed connection enters an explicit delivery mode;
- message fan-out must not block unrelated reactor clients;
- Pub/Sub messages are ephemeral and never enter RDB/AOF.

### Critical invariants

- queued commands do not mutate state before EXEC;
- transaction order matches queue order;
- WATCH observes logical mutations, not physical rehash/encoding changes;
- EXEC conflict abort performs no queued mutations;
- Pub/Sub state is per connection/runtime only;
- publishing does not become persistence state.

### Non-goals

- no rollback transaction model;
- no Lua scripting;
- no Streams;
- no replication yet.

## 4. v0.8 — Replication
Primary/replica roles, full sync, replication offsets, backlog ring buffer, reconnect/partial resync and convergence tests.

## 5. v0.9 — Sentinel-inspired HA
Health monitoring, subjective/objective failure, quorum/election, promotion and topology reconfiguration.

## 6. v0.10 — Cluster
16384 hash slots, ownership, `MOVED`, migration/importing state, `ASK` and basic gossip.

## 7. v0.11 — RESP3
Protocol negotiation, richer scalar/container types and push-style messages.

## 8. v0.12 — Observability and performance
Command latency, event-loop counters, expiration/eviction metrics, persistence timings, replication lag and repeatable profiling.

## 9. Version completion rule

```text
mechanism → invariant/error tests → integration/smoke → race/recovery
→ docs/version → exact branch CI → fast-forward main → same-SHA main CI
```

## 10. Current directive

Current integrated milestone after validation: **v0.6 — memory pressure and sampled eviction**.

Next active version: **v0.7 — transactions, optimistic WATCH and basic Pub/Sub**.

Do not begin replication until transaction boundaries, key-version semantics and ephemeral Pub/Sub state are explicit.
