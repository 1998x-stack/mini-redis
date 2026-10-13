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

The sequence follows dependency order, not command popularity.

## 2. Completed milestones

### v0.1 — Core vertical slice
RESP2 → command registry → Store → String/TTL → AOF. Goroutine-per-client reference transport.

### v0.2 — Nonblocking networking
Linux epoll Poller, reactor, per-client buffers, incremental RESP parsing, pipelines, partial writes, backpressure and legacy comparison engine.

### v0.3 — Dictionary internals
Explicit buckets, collision chains, `ht[0]/ht[1]`, load-factor resize and bounded incremental rehash.

### v0.4 — Core data types and adaptive encodings
List/Hash/Set/ZSet, compact→general transitions, WRONGTYPE, TTL preservation and SkipList rank.

### v0.5 — Persistence depth
Stable logical `SnapshotEntry`, deterministic MRDB, crash-safe publication, AOF rewrite and post-snapshot mutation buffering.

### v0.6 — Memory pressure and sampled eviction
Deterministic logical memory accounting, expiration/eviction separation, `noeviction`, random/LRU/LFU sampled policies, rollback boundary and eviction AOF persistence.

### v0.7 — Transactions, WATCH and Pub/Sub

Primary question:

> How can Redis queue commands and detect optimistic conflicts while keeping transactions different from database rollback, and how does Pub/Sub turn a normal connection into a push-oriented session?

Implemented:

- explicit `clientSession` state shared by reactor and legacy transports;
- `MULTI`, ordered queueing, `EXEC`, `DISCARD`;
- queue-time validation and dirty-transaction `EXECABORT`;
- execution-time errors as EXEC array elements without rollback;
- `WATCH` / `UNWATCH` with key versions, global epoch and existence checks;
- WATCH invalidation on intervening writes, eviction and TTL disappearance;
- RESP2 null-array conflict reply;
- `persistenceMu` as EXEC non-interleaving boundary;
- maintenance and RDB snapshot capture serialized with EXEC;
- AOF `MULTI ... EXEC` framing for crash-truncated transaction-tail recovery;
- `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` live fan-out;
- subscribed connection mode and disconnect cleanup;
- Pub/Sub output integration with legacy writer mutex and reactor output buffers;
- direct semantics tests, AOF restart/crash tests and both-engine network tests;
- `make session-demo`.

Critical invariants:

- MULTI queue state belongs to one client, not the Store;
- EXEC commands cannot be interleaved with another logical mutation;
- runtime command errors do not roll back successful siblings;
- queue-time errors abort before execution;
- WATCH detects intervening writes even if a value is later restored;
- a watched key expiring before EXEC causes conflict;
- RDB/AOF observers do not capture a half-EXEC state;
- a truncated AOF transaction frame replays none of its commands;
- Pub/Sub messages are ephemeral and never become persisted keyspace state;
- disconnect removes subscriptions.

## 3. v0.8 — Replication

### Primary question

**How can a replica establish a consistent starting state, follow an ordered mutation stream, and reconnect using only the missing suffix when the primary still has it?**

### Required implementation

- explicit primary/replica role configuration;
- replication identity and monotonically increasing logical byte/stream offset;
- primary replication feed coupled to acknowledged logical mutation order;
- replica handshake and capability/state exchange;
- full synchronization from one logical snapshot boundary;
- buffering of primary mutations that occur after the full-sync snapshot boundary;
- replica application path that does not re-append replicated commands as independent local client writes;
- bounded backlog/ring buffer keyed by replication offset;
- reconnect request containing replica's known identity + offset;
- partial resynchronization when requested suffix remains in backlog;
- fallback to full sync when identity/offset/backlog cannot satisfy request;
- transaction framing preserved through replication as one ordered unit;
- expiration semantics designed so replicas do not invent independent divergent lifetimes;
- convergence tests under writes during full sync and disconnect/reconnect;
- deterministic demo for full sync versus partial resync.

### Critical invariants

- snapshot base + buffered suffix equals primary logical order;
- replication offsets advance in exactly one canonical stream order;
- replica never applies a suffix before its base state;
- reconnect does not duplicate already-applied mutations;
- backlog wrap only changes whether partial resync is possible, never correctness;
- client writes are rejected on a read-only replica in the initial teaching model;
- replication is not implemented by serializing private Object/Dict memory layout.

### Non-goals

- no Sentinel/election yet;
- no automatic promotion;
- no Cluster/hash slots;
- no diskless production optimization claims.

## 4. Later milestones

### v0.9 — Sentinel-inspired HA
Health monitoring, subjective/objective failure, quorum/election, replica promotion and topology reconfiguration.

### v0.10 — Cluster
16384 hash slots, key→slot mapping, ownership, `MOVED`, migration/importing state, `ASK` and basic topology gossip.

### v0.11 — RESP3
Protocol negotiation, richer scalar/container types and push-style messages where useful.

### v0.12 — Observability and performance
Command/session latency, event-loop counters, expiration/eviction metrics, persistence/replication timings, lag and repeatable profiling.

## 5. Version completion rule

A version is complete only after:

```text
mechanism implemented
      ↓
invariant + error-path tests
      ↓
integration / smoke / demo
      ↓
race/recovery checks
      ↓
docs + version.md
      ↓
exact branch commit passes CI
      ↓
fast-forward into main
      ↓
main passes CI on same SHA
```

## 6. Current directive

Current freeze candidate: **v0.7 — connection sessions, optimistic WATCH transactions and live Pub/Sub**.

Next active version after v0.7 validation: **v0.8 — replication full sync, ordered propagation, offsets/backlog and partial resync**.

Do not begin Sentinel or Cluster work until replication convergence/reconnect semantics are explicit and tested.
