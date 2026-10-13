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

Primary question:

> How can multiple writable primaries partition one logical keyspace so every key has a deterministic slot/owner and clients receive explicit stable or migration-time routing information?

Implemented:

- Redis-style 16384 logical slots;
- CRC-16/XMODEM key hashing;
- non-empty `{...}` hash-tag semantics;
- slot ownership table separate from Store values;
- node identity/address metadata;
- routing gate before Store execution;
- CROSSSLOT rejection for multi-key commands spanning slots;
- stable wrong-owner `MOVED` responses;
- explicit slot migration target state;
- source-side existing-key execution during migration;
- source-side missing-key `ASK` redirection;
- target-side one-request `ASKING` authorization;
- per-connection ASKING state on the cluster RESP endpoint;
- resumable slot migration;
- key transfer through logical `SnapshotEntry` rather than private Object encodings;
- source `persistenceMu` held from export through target acknowledgement and source delete;
- target reconstruction through existing command/AOF/replication publication path;
- absolute TTL preservation across migration;
- stable ownership switch only after all remaining source keys transfer;
- bounded `GossipPage(start, limit)` topology dissemination;
- explicit topology epochs with deterministic last-writer-wins merge policy;
- real RESP/TCP cluster-aware endpoint via `Server.ServeCluster`;
- unit, real-server and wire-level tests under `go test -race`;
- deterministic `make cluster-demo`.

Critical invariants:

- every routed key maps to exactly one slot;
- non-empty equal hash tags map related keys to the same slot;
- multi-key requests cannot cross slot boundaries silently;
- stable wrong-owner routing uses MOVED;
- migration-time temporary routing uses ASK;
- ASKING grants exactly one subsequent request on one connection;
- migration state is topology metadata, never Store payload state;
- source does not delete before target import succeeds;
- partial migration remains visibly in migration state and is resumable;
- stable owner changes only after source has no remaining slot keys;
- physical encodings never cross the migration boundary;
- routing existence check and authorized execution share `Server.persistenceMu`;
- bounded gossip does not imply consensus.

Intentional teaching simplifications:

- cluster routing is exposed through an explicit v0.10 cluster endpoint/facade rather than silently changing the historical standalone endpoint;
- topology epochs are memory-only;
- merge is deterministic LWW, not distributed consensus;
- no automatic coupling between Sentinel failover and cluster slot ownership;
- no production Redis Cluster bus/protocol compatibility;
- no fencing against conflicting topology writers;
- no cluster MULTI/EXEC support in this version;
- no production anti-entropy/failure detector.

## 3. v0.11 — RESP3

### Primary question

**How can protocol negotiation and richer response types evolve without disturbing the command/storage/replication/cluster mechanism boundaries already established?**

### Required implementation

- HELLO-style protocol negotiation;
- RESP3 scalar types useful to the current command subset;
- map/set/container replies where they improve semantics;
- push messages for Pub/Sub and future async notifications;
- one connection protocol mode owned by session state;
- backward-compatible RESP2 path;
- parser/writer tests for fragmented inputs and nested containers;
- both network engines validated;
- deterministic RESP2-vs-RESP3 demo.

### Non-goals

- no attempt to implement every Redis RESP3 command;
- no protocol change that bypasses existing command semantics or cluster routing.

## 4. v0.12 — Observability / performance

Command/session latency, reactor counters, expiration/eviction metrics, persistence/replication/failover/cluster migration timing, lag and repeatable profiling.

## 5. Version completion rule

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

## 6. Current directive

Freeze candidate: **v0.10 — 16384 slots, MOVED/ASK/ASKING, resumable logical key migration and bounded topology dissemination**.

Next milestone after exact-SHA validation: **v0.11 — RESP3**.
