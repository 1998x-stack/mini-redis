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

Primary question:

> Why can one logical Redis type switch physical representation without changing command behavior?

Implemented:

- List: compact slice → chunked quicklist;
- Hash: compact field/value array → custom Dict;
- Set: sorted intset → custom Dict;
- ZSet: sorted compact array → member-score Dict + span-aware SkipList;
- Redis-style `WRONGTYPE`;
- one-way compact→general conversion;
- atomic Store-level collection mutations;
- TTL preservation and empty-key cleanup;
- collection command subset and encoding-aware smoke tests.

Critical invariants:

- conversion preserves logical results;
- a key has one logical type regardless of encoding;
- collection mutation preserves TTL;
- deleting the final element deletes value + expiry;
- ZSet Dict and SkipList stay synchronized;
- equal ZSet scores use deterministic member tie-break ordering.

## 3. v0.5 — Persistence depth: RDB + AOF rewrite

### Primary question

**How does an in-memory database trade write cost, restart speed and durability while replacing persistence files safely?**

### Required implementation

#### RDB-like snapshot

- deterministic educational snapshot format;
- all current logical types;
- absolute expiry timestamps;
- format version/magic;
- checksum/corruption detection;
- save/load;
- temp file + fsync + atomic rename;
- blocking snapshot reference path first.

#### AOF rewrite

- emit a compact command representation of current logical state;
- preserve absolute expiry semantics;
- capture mutations that occur while rewrite is being prepared;
- atomic replacement of old AOF;
- restart correctness after rewrite;
- interrupted rewrite must leave a valid old file.

#### Startup policy

Explicitly define:

- AOF vs RDB precedence;
- missing/corrupt file behavior;
- truncated AOF tail behavior;
- version incompatibility behavior.

### Critical invariants

- final persistence path never exposes a partially written snapshot/rewrite;
- reload produces the same logical type/content/TTL state;
- AOF rewrite removes redundant history without changing state;
- physical in-memory encodings are not serialized as unstable Go implementation details;
- expired keys are not resurrected.

### Non-goals

- no production fork/COW snapshot in the first reference implementation;
- no maxmemory/eviction;
- no replication.

## 4. v0.6 — Expiration + maxmemory + eviction

Implement time-budgeted active expiry, approximate memory accounting, `maxmemory`, random policies, sampled LRU and sampled LFU. Keep expiration (time invalidation) distinct from eviction (memory-pressure removal).

## 5. v0.7 — Transactions + Pub/Sub

Implement `MULTI/EXEC/DISCARD`, `WATCH/UNWATCH`, optimistic version checks and basic `SUBSCRIBE/UNSUBSCRIBE/PUBLISH`.

## 6. v0.8 — Replication

Implement primary/replica roles, full synchronization, replication offsets, backlog ring buffer, reconnect/partial resync and convergence tests.

## 7. v0.9 — Sentinel-inspired HA

Health monitoring, subjective/objective failure, quorum/election, replica promotion and topology reconfiguration.

## 8. v0.10 — Cluster

16384 hash slots, key→slot mapping, ownership, `MOVED`, migration/importing state, `ASK` and basic topology gossip.

## 9. v0.11 — RESP3

Protocol negotiation, richer scalar/container types and push-style messages where useful.

## 10. v0.12 — Observability and performance

Command latency, connection/event-loop counters, expiration/eviction metrics, persistence timings, replication lag and repeatable profiling/benchmark methodology.

## 11. Version completion rule

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

## 12. Current directive

Current integrated milestone after validation: **v0.4 — core data types and adaptive encodings**.

Next active version: **v0.5 — RDB-like snapshot and AOF rewrite**.

Do not begin eviction, replication or Cluster work until v0.5 has a clear logical serialization boundary for every v0.4 object type.
