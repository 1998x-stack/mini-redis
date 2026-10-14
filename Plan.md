# mini-redis — Architecture-Driven Development Plan

> `main` is the newest integrated, validated state. Historical `v0.x` branches are frozen learning snapshots.

## 1. Dependency-first roadmap

```text
Original architecture-learning arc

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

Second evidence-driven systems arc

v0.12 reveals an unmeasurable quantity:
primary cannot know replica applied progress
  ↓
v0.13 replica ACK / authoritative lag
```

The sequence follows mechanism dependency and evidence, not command popularity or version-number momentum.

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

- atomic counter/gauge primitives and synchronized labeled maps;
- fixed-bucket bounded duration histograms;
- command/protocol/network/reactor/backpressure/state/persistence metrics;
- Cluster and Sentinel control-plane metrics;
- immutable server/topology/controller snapshots;
- read-only `INFO [section]`;
- observability demo, microbenchmarks and pprof workflow;
- repeatable performance reporting discipline.

Critical result for the next arc:

> v0.12 could report primary canonical offset and replica-local applied offset, but the primary could not truthfully report a remote replica's applied-offset lag because v0.8 had no receiver→sender acknowledgement.

That limitation is the evidence that justifies v0.13.

## 3. v0.13 — Replication ACK / authoritative lag

### Primary question

**How can the sender distinguish bytes it has produced from work the receiver has actually applied?**

### Mechanism

```text
primary canonical stream
      ↓
FULLRESYNC / CONTINUE / live frame
      ↓
replica installs/applies complete boundary
      ↓
REPLCONF ACK <connection-replica-id> <applied-offset>
      ↓
primary validates under replication.Primary.mu
      ↓
live per-replica progress
      ↓
lag_bytes = primary_offset - acknowledged_offset
```

Implemented scope:

- one fresh connection-local replica ID per accepted replication connection;
- replica ID included in FULLRESYNC and CONTINUE handshake lines;
- bidirectional replication TCP connection with independent data and ACK directions;
- ACK after successful full-sync snapshot install;
- ACK after every successfully applied complete replication frame;
- immediate ACK of the retained local offset on CONTINUE reconnect;
- primary ACK registry colocated with live subscribers under `replication.Primary.mu`;
- accepted ACK invariant `0 <= previous <= new <= primary offset`;
- explicit wrong-ID, negative, regression and ahead-of-primary rejection;
- disconnect/backpressure subscriber removal also removes live progress;
- immutable replica progress snapshots with remote address, sync mode, acknowledged offset, lag bytes and ACK age;
- INFO replication view for live replica progress;
- ACK received/rejected counters;
- real-replica, manual delayed-ACK and reconnect tests under race detector;
- deterministic `make replication-ack-demo`.

### Critical invariants

- ACK frames never enter AOF, RDB, canonical replication backlog or offset accounting;
- primary canonical offset means mutation-stream bytes, not network control traffic;
- replica sends progress only after a complete apply boundary;
- a rejected ACK cannot change the last accepted progress;
- disconnect removes live progress instead of leaving stale lag records;
- reconnect gets a new connection-local replica ID even when CONTINUE succeeds;
- `lag_bytes` is authoritative according to the protocol's receiver-reported applied offset;
- ACK is not independently verifiable proof against a malicious peer;
- ACK is not disk durability because teaching replicas remain memory-only;
- `ack_age_ms` is age of the last applied-progress ACK, not heartbeat age;
- Sentinel does not consume ACK age/lag for failure detection or candidate ranking in v0.13.

### Intentional teaching simplifications

- no Redis WAIT / WAITAOF;
- no client write waiting for replica ACKs;
- no replica fsync acknowledgement;
- no durable replica identity across reconnects;
- no periodic heartbeat/equal-ACK timer;
- no quorum durability or failover fencing;
- no production Redis REPLCONF compatibility claim beyond the teaching ACK form.

## 4. Evidence-driven second arc

v0.13 establishes the rule for later versions:

```text
observe a real limitation
        ↓
name one systems question
        ↓
add the minimum mechanism needed to answer it
        ↓
measure again
```

Potential later questions remain deliberately uncommitted. Examples include:

- should client writes wait for N replica ACKs, and what consistency promise would that actually provide?
- should replicas periodically report progress/heartbeat during idle periods?
- how should Sentinel distinguish replication freshness from network liveness?
- what does disk durability acknowledgement require beyond memory-applied ACK?
- where do profiles justify IO-thread or allocator work?

A future version must select one such question explicitly rather than bundle them.

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
      ↓
main and version branch identical
```

## 6. Current directive

Freeze candidate: **v0.13 — receiver ACKs, primary-visible applied offsets and authoritative byte lag while preserving v0.8 stream/backlog semantics**.

Do not introduce WAIT, failover policy based on lag, or replica disk durability into v0.13; those are separate future questions.
