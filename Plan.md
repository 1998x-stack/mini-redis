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

RESP2 → command registry → Store → String/TTL → AOF. Goroutine-per-client transport is the reference baseline.

### v0.2 — Nonblocking networking

Linux epoll Poller, reactor, per-client buffers, incremental RESP parsing, pipeline ordering, partial writes, backpressure, timers and legacy comparison engine.

### v0.3 — Dictionary internals

Explicit buckets, collision chains, `ht[0]/ht[1]`, load-factor resize policy, bounded entry-level incremental rehash, event-loop maintenance and Object type/encoding metadata.

### v0.4 — Core data types and adaptive encodings

List/Hash/Set/ZSet, compact→general encoding transitions, `WRONGTYPE`, Store-level atomic collection mutation, TTL preservation and span-aware SkipList rank.

### v0.5 — Persistence depth: RDB + AOF rewrite

Primary question:

> How can logical in-memory state be persisted and history compacted without publishing a partially written replacement file or losing writes during rewrite?

Implemented:

- logical `SnapshotEntry` serialization boundary independent of `Object.Encoding`;
- deterministic project-specific `MRDB` v1 format;
- all current logical types and absolute expiry deadlines;
- magic/version/length/checksum validation;
- canonical ZSet score wire representation including infinities;
- snapshot restore without resurrecting expired keys;
- same-directory temp file + file fsync + atomic rename + directory fsync publication;
- compact AOF reconstruction command stream;
- mutation buffer for writes after the rewrite snapshot boundary;
- named server `persistenceMu` coordination boundary;
- explicit AOF-vs-RDB startup precedence;
- crash-window, corruption, restart and truncated-tail recovery tests;
- `make persistence-demo`.

Critical invariants:

- physical in-memory encodings never define the persistence format;
- a relative TTL crosses persistence only as an absolute deadline;
- expired keys are never resurrected;
- failure before rename leaves the old target valid;
- AOF rewrite result is snapshot state plus every later acknowledged mutation;
- AOF rewrite removes redundant history without changing logical restart state.

## 3. v0.6 — Expiration + maxmemory + eviction

### Primary question

**What is the difference between expiration and eviction, and why does Redis use approximate/sampled victim selection instead of maintaining a perfectly sorted global LRU/LFU structure?**

### Required implementation

- keep expiration as time-based validity, separate from memory-pressure eviction;
- add inspectable approximate memory accounting at the Store/object boundary;
- add `maxmemory` configuration and no-eviction failure semantics;
- add a simple random eviction baseline;
- add sampled LRU metadata and victim selection;
- add sampled LFU counters/decay and victim selection;
- distinguish all-keys and volatile-key policy pools where implemented;
- integrate eviction with the existing keyspace/expiry dictionaries without breaking rehash invariants;
- make one command's memory-pressure decision an explicit atomic Store/server boundary;
- add deterministic policy tests with injectable clock/randomness;
- add a memory-pressure demo comparing exact state validity vs policy-driven removal.

### Critical invariants

- expired keys are removed for time validity, not counted as successful eviction choices;
- value and expiry metadata disappear together on eviction;
- eviction never leaves duplicate/inconsistent Dict state during rehash;
- command failure under `noeviction` does not silently leave a partially applied mutation;
- sampling budget is bounded and observable;
- LRU/LFU metadata is policy state, not persistence-format state.

### Non-goals

- no transactions/PubSub yet;
- no replication;
- no exact production Redis byte accounting;
- no claim that sampled policies reproduce Redis' exact tuning constants.

## 4. v0.7 — Transactions + Pub/Sub

Implement `MULTI/EXEC/DISCARD`, `WATCH/UNWATCH`, optimistic version checks and basic `SUBSCRIBE/UNSUBSCRIBE/PUBLISH`.

## 5. v0.8 — Replication

Implement primary/replica roles, full synchronization, replication offsets, backlog ring buffer, reconnect/partial resync and convergence tests.

## 6. v0.9 — Sentinel-inspired HA

Health monitoring, subjective/objective failure, quorum/election, replica promotion and topology reconfiguration.

## 7. v0.10 — Cluster

16384 hash slots, key→slot mapping, ownership, `MOVED`, migration/importing state, `ASK` and basic topology gossip.

## 8. v0.11 — RESP3

Protocol negotiation, richer scalar/container types and push-style messages where useful.

## 9. v0.12 — Observability and performance

Command latency, connection/event-loop counters, expiration/eviction metrics, persistence timings, replication lag and repeatable profiling/benchmark methodology.

## 10. Version completion rule

A version is complete only after:

```text
mechanism implemented
      ↓
invariant + error-path tests
      ↓
integration / smoke
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

## 11. Current directive

Current integrated milestone after validation: **v0.5 — logical snapshots, RDB-like persistence and buffered AOF rewrite**.

Next active version: **v0.6 — maxmemory and sampled eviction policies**.

Do not begin transactions, replication or Cluster work until v0.6 has a clear memory-accounting and eviction atomicity boundary.
