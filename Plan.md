# mini-redis — Architecture-Driven Development Plan

> `main` is the newest integrated, validated state. Historical `v0.x` branches are frozen learning snapshots.

## 1. Dependency-first roadmap

```text
v0.1 request/state/AOF
  ↓
v0.2 event loop
  ↓
v0.3 dictionary internals
  ↓
v0.4 logical types / physical encodings
  ↓
v0.5 persistence depth
  ↓
v0.6 memory pressure / eviction
  ↓
v0.7 transactions / PubSub
  ↓
v0.8 replication
  ↓
v0.9 Sentinel-style failover
  ↓
v0.10 Cluster / hash slots
  ↓
v0.11 RESP3
  ↓
v0.12 observability / performance
```

The sequence follows mechanism dependency, not command popularity.

## 2. Completed milestones

### v0.1 — Core vertical slice
RESP2 → command registry → Store → String/TTL → AOF, with goroutine-per-client reference transport.

### v0.2 — Nonblocking networking
Linux epoll Poller/reactor, per-client buffers, incremental RESP parsing, pipelines, partial writes, backpressure and legacy comparison engine.

### v0.3 — Dictionary internals
Explicit buckets/collisions, `ht[0]/ht[1]`, resize and bounded incremental rehash.

### v0.4 — Core data types and adaptive encodings
List/Hash/Set/ZSet, compact→general encodings, WRONGTYPE, TTL preservation and SkipList rank.

### v0.5 — Persistence depth
Logical `SnapshotEntry`, deterministic RDB-like snapshots, crash-safe publication, AOF rewrite and post-snapshot buffering.

### v0.6 — Memory pressure
Deterministic logical accounting, `noeviction`, random/LRU/LFU sampling, rollback boundary and eviction persistence.

### v0.7 — Transactions and Pub/Sub
Connection-local session state, WATCH, MULTI/EXEC semantics, AOF transaction framing and live Pub/Sub.

### v0.8 — Replication
Full sync from logical state, canonical mutation stream, repl-id/offset, frame-aware backlog, partial resync and READONLY replicas.

### v0.9 — Sentinel-inspired HA
Monitor-local SDOWN, quorum-backed ODOWN, failover epoch, deterministic candidate ranking, runtime promotion/demotion and topology repair.

### v0.10 — Cluster / hash slots
16384 CRC16/XMODEM slots, hash tags, CROSSSLOT, MOVED, ASK/ASKING, logical SnapshotEntry migration, resumable partial migration and bounded topology dissemination.

### v0.11 — RESP3
Connection-local HELLO 2/3 negotiation, richer logical reply types, explicit RESP2 downgrade, Pub/Sub Push, nested incremental value parsing and unchanged command/Cluster semantics.

### v0.12 — Observability / performance

Primary question:

> Once the mechanisms are explicit, how can they be measured without allowing instrumentation to become database state or policy?

Implemented:

- `internal/observability` atomic Counter/Gauge primitives;
- dynamic counter/gauge maps with synchronized label creation and copy-based snapshots;
- fixed-bucket bounded duration histograms with count/sum/max;
- server command count/error/latency metrics;
- RESP2/RESP3 request/reply and Push counters;
- accepted/active connection metrics;
- actual legacy `net.Conn` read/write byte metrics;
- actual reactor `syscall.Read/Write` byte metrics;
- reactor poll call/event/wait duration metrics;
- input/output backpressure events;
- expiration, eviction, maintenance and rehash-work counters;
- AOF append, RDB save and AOF rewrite operations/bytes/failures/durations;
- primary replication identity/offset/backlog/full/partial-sync/subscriber snapshot;
- replica-local link failures, sync mode, applied frame/byte/offset evidence;
- Cluster routing, MOVED/ASK/CROSSSLOT, migration and bounded gossip metrics;
- Sentinel Tick/probe/failover/promotion/reconfiguration metrics and bounded latency histograms;
- immutable `Server.MetricsSnapshot`, `Topology.MetricsSnapshot` and `Controller.MetricsSnapshot` surfaces;
- read-only `INFO [section]` teaching view;
- real TCP legacy/reactor observability tests;
- control-plane Cluster/Sentinel metric tests against real decision paths;
- `make observability-demo` cross-layer teaching demo;
- `make benchmark-observability` microbenchmarks;
- `docs/PERFORMANCE.md` workload/profile discipline and pprof workflow.

Critical invariants:

- metrics never enter Store, SnapshotEntry, AOF, RDB, replication frames or slot metadata;
- metrics never participate in success/failure, eviction, persistence, routing or failover decisions;
- established mechanisms still work when instrumentation is absent;
- snapshots expose copies/atomic loads, not mutable internal metric maps;
- histogram memory is bounded independent of request count;
- network byte counters reflect actual transport reads/writes rather than encoded-size estimates;
- persistence timing wraps real persistence calls;
- replication reports only information the v0.8 protocol can truthfully know;
- because v0.8 has no replica ACK, the primary does not claim authoritative remote applied-offset lag;
- benchmarks state workload/configuration instead of making context-free speed claims.

Intentional teaching simplifications:

- no Prometheus/OpenTelemetry exporter;
- no distributed metric aggregation;
- no production alerting/SLO system;
- no claim of zero instrumentation overhead;
- no production Redis performance-parity claim.

## 3. Original learning arc complete

v0.1–v0.12 now form one complete mechanism chain:

```text
request/protocol
  ↓
network scheduling
  ↓
dictionary + object encodings
  ↓
persistence + memory pressure
  ↓
sessions / transactions / PubSub
  ↓
replication / HA / sharding
  ↓
richer protocol representation
  ↓
measurement / profiling
```

A future v0.13 should not be chosen merely because the number follows v0.12. It should start a new arc from one measured bottleneck or one explicit new systems question.

Potential future arcs, only after evidence:

- active defragmentation / allocator behavior;
- IO threads and command-execution ownership;
- replica ACK and authoritative lag tracking;
- production-style Cluster bus/failure detection;
- Lua/functions and deterministic replication semantics;
- storage tiering or disk-backed structures.

None of these are part of v0.12.

## 4. Version completion rule

```text
mechanism implemented
      ↓
invariant + error-path tests
      ↓
integration / wire / demo
      ↓
race + historical regression checks
      ↓
docs + version.md
      ↓
exact version SHA passes CI
      ↓
fast-forward main
      ↓
same SHA passes CI on main
```

## 5. Current directive

Freeze candidate: **v0.12 — observer-only bounded metrics, immutable snapshots/INFO and repeatable profiling workflow**.

After v0.12 is green on one identical branch/main SHA, treat the original mini-redis learning roadmap as complete.
