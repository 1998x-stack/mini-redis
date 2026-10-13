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
Deterministic logical memory accounting, expiration/eviction separation, `noeviction`, random/LRU/LFU sampled policies, rollback boundary and eviction persistence.

### v0.7 — Transactions, WATCH and Pub/Sub
Per-client session state, optimistic WATCH versions, MULTI/EXEC ordering, queue-time versus execution-time errors, AOF transaction framing and live Pub/Sub.

### v0.8 — Replication
Logical full-sync snapshot, canonical RESP mutation stream, repl-id/byte offset, frame-aware backlog, partial resync, FULLRESYNC fallback, replica READONLY and transaction-frame propagation.

### v0.9 — Sentinel-inspired HA

Primary question:

> Once replicas already contain copies of the data, how can multiple observers distinguish one observer's network problem from an objective primary failure, select a sufficiently healthy/fresh replica, promote it once for one failover epoch, and repair the topology?

Implemented:

- Sentinel control plane separated from the v0.8 replication data plane;
- independent monitor-local health histories with last success/failure and failure start;
- SDOWN only after one monitor's `down-after` interval expires;
- ODOWN only when configured quorum has SDOWN state for the current primary;
- monotonically increasing failover epoch;
- deterministic teaching leader: lexicographically first SDOWN voter for the epoch;
- candidate eligibility requiring quorum reachability and quorum observation as a replica;
- candidate ranking by reachable votes, replication offset, then stable node ID;
- runtime `PromoteToPrimary` with fresh repl-id/backlog/listener;
- runtime `FollowPrimary` with READONLY replica transition and safe FULLRESYNC boundary to a different primary;
- role-specific replication contexts so old follow/listen goroutines are cancelled during topology changes;
- surviving replicas reconfigured to the promoted primary;
- old primary reconfigured as replica after it becomes reachable again;
- explicit rejection of dynamic demotion when local AOF or local maxmemory policy would violate v0.8 replica invariants;
- monitor-specific fault injection for isolated-observer and primary-partition tests;
- fake-node tests for SDOWN/ODOWN/quorum/epoch/ranking/rejoin;
- real three-data-node integration test proving promotion, reconfiguration, replication convergence and old-primary return under `go test -race`;
- deterministic `make sentinel-demo` showing SDOWN → ODOWN → election → promotion → topology repair.

Critical invariants:

- one monitor's timeout is never sufficient for ODOWN;
- no failover epoch starts without quorum-backed ODOWN;
- one Tick is serialized, so one controller cannot execute overlapping failovers;
- an unavailable candidate cannot win solely because its old replication offset was high;
- promotion changes topology but does not invent a second database-copy mechanism;
- runtime role changes share `Server.persistenceMu`, so a logical mutation cannot cross a role transition halfway through;
- promoted primary receives a fresh replication identity;
- switching to a different primary resets remembered repl-id/offset and safely re-enters v0.8 FULLRESYNC;
- repeated healthy observations of the newly chosen primary do not create another promotion epoch;
- old-primary return is topology repair, not merge of two divergent histories.

Intentional teaching simplifications:

- deterministic leader choice is not Redis Sentinel's complete election protocol;
- monitor state/epoch is memory-only;
- no fencing or lease prevents an unreachable old primary from remaining writable during a partition;
- therefore split brain is possible and strong consistency is not claimed;
- dynamically demotable nodes use `appendonly=false` and `maxmemory=0` in v0.9;
- client-side primary discovery is outside the current teaching scope;
- no production-compatible Sentinel wire protocol is claimed.

## 3. v0.10 — Cluster / hash slots

### Primary question

**How can the keyspace be partitioned across multiple primaries while clients receive explicit routing information during steady state and slot migration?**

### Required implementation

- Redis-style 16384 logical hash slots;
- deterministic key → slot calculation, including hash-tag semantics where useful;
- slot ownership table separated from ordinary key/value Store state;
- routing gate before command execution;
- `MOVED` response when a client contacts the wrong owner;
- migration/importing state for one slot;
- `ASK` + one-request acknowledgement semantics during migration;
- safe key transfer without silently serving two authoritative owners under the teaching policy;
- basic cluster-node identity/topology metadata;
- bounded topology/gossip teaching mechanism;
- tests for routing, ownership changes, migration and multi-key slot constraints;
- deterministic cluster demo.

### Critical invariants

- every routed key maps to exactly one logical slot;
- normal steady-state slot ownership has one authoritative owner;
- migration state is explicit rather than hidden inside Store;
- `MOVED` means stable ownership; `ASK` means temporary migration routing;
- Cluster work reuses replication/failover mechanisms where applicable rather than replacing them.

### Non-goals

- no production Redis Cluster protocol completeness;
- no geographically distributed consensus claim;
- no transparent multi-key transactions across unrelated slots.

## 4. Later milestones

### v0.11 — RESP3
Protocol negotiation, richer scalar/container types and push-style messages where useful.

### v0.12 — Observability and performance
Command/session latency, event-loop counters, expiration/eviction metrics, persistence/replication/failover timings, lag and repeatable profiling.

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

Current freeze candidate: **v0.9 — Sentinel-inspired SDOWN/ODOWN, failover epoch, deterministic replica promotion and topology repair**.

Next active version after v0.9 exact-SHA validation: **v0.10 — Cluster hash-slot ownership, MOVED/ASK and migration**.

Do not begin Cluster work until v0.9 proves that topology control changes reuse the existing v0.8 replication data plane and the failover limitations are explicit.