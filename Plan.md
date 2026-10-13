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
Per-client session state, optimistic WATCH versions, MULTI/EXEC ordering, queue-time versus execution-time failure semantics, AOF transaction framing, live Pub/Sub and both-engine push integration.

### v0.8 — Replication

Primary question:

> How can a replica establish one consistent base state, follow exactly the primary's logical mutation order, and reconnect from its last applied position without always copying the whole database again?

Implemented:

- explicit `standalone`, `primary` and `replica` roles;
- separate educational replication TCP plane beside normal client transport;
- random primary replication identity (`repl-id`);
- monotonically increasing byte offset over canonical RESP mutation bytes;
- one primary mutation feed coupled to the existing `persistenceMu` logical-order boundary;
- full synchronization from logical `SnapshotEntry[]` state;
- snapshot/offset/live-subscriber capture under one mutation-ordering lock;
- buffered post-snapshot live frames so writes cannot fall through the snapshot handoff gap;
- frame-aware bounded backlog that evicts complete logical frames, never partial RESP commands;
- `PSYNC <repl-id> <offset>` reconnect request;
- `CONTINUE` partial resynchronization when identity and retained frame boundary match;
- fallback `FULLRESYNC` when identity changed or backlog history wrapped;
- replica-side full snapshot install plus ordered frame application;
- replica offset advancement only after a complete frame applies;
- read-only client gate on replicas;
- absolute TTL reconstruction and eviction-generated DEL propagation through the canonical mutation stream;
- MULTI/EXEC preserved as one replication frame;
- full/live/partial/fallback/transaction integration tests under `go test -race`;
- deterministic `make replication-demo` showing FULLRESYNC → live follow → CONTINUE → READONLY.

Critical invariants:

- snapshot base + every post-boundary frame equals primary logical mutation order;
- no write can occur between snapshot capture and live subscriber attachment;
- replication offsets count only canonical mutation payload bytes, not TCP envelope bytes;
- replicas request resume only from complete-frame boundaries;
- backlog wrap changes availability of partial resync, never correctness;
- a stale replication identity forces full sync;
- a replica never applies a suffix before installing its base snapshot;
- offset advances only after a whole frame succeeds;
- successful EXEC mutations remain one frame across both live follow and catch-up;
- replicated commands bypass the client READONLY gate but do not run an independent local eviction policy;
- private Dict/Object physical representation never crosses the replication protocol.

Intentional teaching simplifications:

- replica replication identity/offset is memory-only;
- replica local AOF/maxmemory are disabled in v0.8;
- a new primary process generates a new identity and therefore causes full sync;
- slow replica handling favors disconnect/reconnect over production-grade output-buffer policy;
- no acknowledgement quorum, WAIT semantics, promotion or failover yet.

## 3. v0.9 — Sentinel-inspired HA

### Primary question

**Once replication exists, how can multiple observers decide that a primary is actually unavailable, select a replica to promote, and reconfigure the topology without confusing a local network failure with a cluster-wide failure?**

### Required implementation

- explicit Sentinel/monitor process or subsystem separated from data-serving state;
- periodic health probes with observable last-success/last-failure state;
- subjective-down state based on one monitor's timeout;
- objective-down state requiring configured quorum agreement;
- term/epoch-like election metadata to avoid repeated stale promotion decisions;
- candidate replica ranking using health and replication freshness;
- deterministic leader/election rule for one failover attempt;
- replica promotion to writable primary;
- surviving replicas reconfigured to follow the promoted primary;
- old primary rejoined as replica when it returns;
- failure simulation for primary loss, isolated monitor and stale replica;
- deterministic demo distinguishing SDOWN, ODOWN, election, promotion and topology repair.

### Critical invariants

- one monitor timing out a primary is insufficient for objective failure;
- promotion never selects an unavailable/stale candidate when a healthier candidate exists under the teaching policy;
- a failover decision belongs to one monotonically advancing election epoch;
- topology reconfiguration follows the chosen promoted primary consistently;
- replication correctness remains the v0.8 mechanism; Sentinel does not create a second data-copy path.

### Non-goals

- no Cluster hash-slot ownership yet;
- no geographically distributed consensus claim;
- no production-compatible Sentinel protocol completeness;
- no client-side service discovery beyond what the teaching demo requires.

## 4. Later milestones

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

Current freeze candidate: **v0.8 — replication full sync, canonical mutation stream, offsets/backlog and partial resynchronization**.

Next active version after v0.8 exact-SHA validation: **v0.9 — Sentinel-inspired health, quorum, election, promotion and topology reconfiguration**.

Do not begin Cluster/hash-slot work until failover builds cleanly on the v0.8 replication topology rather than bypassing it.
