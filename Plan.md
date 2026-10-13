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

Primary question:

> How can a connection negotiate richer response types and asynchronous Push frames without changing the storage, persistence, replication, failover or Cluster mechanisms underneath it?

Implemented:

- connection-local `Protocol2` / `Protocol3` state;
- default RESP2 compatibility;
- `HELLO 3` / `HELLO 2` negotiation;
- HELLO metadata as logical Map with RESP2 flat-array downgrade;
- richer `resp.Value` variants for Null, Boolean, Double, Big Number, Verbatim String, Map, Set and Push;
- protocol-aware encoding shared by legacy and reactor transports;
- explicit RESP2 downgrade for richer logical values;
- response-view adaptation after command execution for HGETALL/SMEMBERS/SISMEMBER/ZSCORE;
- Pub/Sub Push frames in RESP3 while retaining RESP2 array behavior;
- ordinary commands while subscribed in RESP3 mode;
- incremental nested RESP2/RESP3 value parser with bounded depth;
- byte-boundary fragmentation tests;
- Cluster endpoint HELLO negotiation while retaining MOVED/ASK/CROSSSLOT authorization order;
- deterministic `make resp3-demo` using a real TCP session;
- regression validation for every prior demo and both network engines.

Critical invariants:

- each connection starts RESP2;
- HELLO changes only the current connection;
- command requests and canonical mutation frames remain independent of client response protocol;
- Store/SnapshotEntry/AOF/replication/topology never contain negotiated protocol state;
- protocol adaptation occurs after command and Cluster semantics;
- RESP3 cannot bypass wrong-owner routing;
- asynchronous Pub/Sub delivery remains one mechanism; only its representation changes;
- RESP2 wire behavior remains backward compatible;
- incomplete nested RESP frames are never partially consumed.

Intentional teaching simplifications:

- not every RESP3 type or streamed aggregate is implemented;
- no complete Redis HELLO AUTH/SETNAME option set;
- no client tracking/invalidation Push subsystem;
- no claim of edge-case production Redis protocol compatibility.

## 3. v0.12 — Observability / performance

### Primary question

**Once every major Redis mechanism is explicit, how can we measure where latency, memory, queueing, durability cost, replication lag and topology work are actually spent?**

### Required implementation

- command counters and latency histograms;
- per-network-engine connection/read/write/backpressure counters;
- key expiration and eviction counters;
- AOF/RDB operation duration/bytes/failure metrics;
- replication full/partial sync counts, backlog state and replica lag;
- Sentinel probe/failover timing and decisions;
- Cluster redirect/migration/gossip counters and migration duration;
- protocol-mode / RESP3 Push counters where useful;
- an inspectable `INFO`/metrics-style snapshot that does not leak mutable internals;
- repeatable benchmarks for command path, reactor vs legacy, Dict and selected end-to-end workloads;
- pprof/profiling workflow and documentation;
- explicit separation of measurement overhead from measured mechanisms;
- deterministic observability/performance demo.

### Critical invariants

- instrumentation must not become business/database state;
- metrics observation must not alter command semantics or mutation ordering;
- counters must name their synchronization boundary;
- benchmarks must report workload/configuration, not context-free numbers;
- performance claims require repeatable evidence rather than anecdotes.

### Non-goals

- no production monitoring stack;
- no promise that this educational implementation competes with Redis performance;
- no optimization that hides or removes the mechanisms the project exists to teach.

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

Freeze candidate: **v0.11 — RESP3 negotiation, richer views, RESP2 downgrade and Push messages**.

Next milestone after exact-SHA validation: **v0.12 — Observability / Performance**.
