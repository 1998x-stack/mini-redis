# mini-redis v0.12 — Observability / Performance

## 1. Identity

- Historical learning branch: `v0.12`
- Previous snapshot: `v0.11`
- Version label: `v0.12.0`
- Theme: observable mechanisms, bounded metrics, latency evidence and repeatable profiling

## 2. Primary question

**Once Redis mechanisms are explicit, how can we measure where time, queueing, durability cost, memory pressure, replication work and topology work are spent without changing their semantics?**

```text
client / reactor / legacy
        ↓
existing Redis mechanisms
        ↓
observation points only
        ↓
atomic counters + bounded histograms
        ↓
immutable MetricsSnapshot
        ↓
INFO / demo / benchmark / profile
```

The dominant lesson is:

> **Instrumentation is an observer of mechanism state and events. It is never database state, routing state or mutation policy.**

## 3. Delta from v0.11

Planned additions:

- reusable observability package with atomic counters and fixed-bucket latency histograms;
- command request/error/latency metrics;
- legacy/reactor connection, byte, poll and backpressure metrics;
- expiration and eviction counters;
- AOF/RDB operation counts, bytes, failures and duration metrics;
- replication full/partial sync, backlog window and replica applied-offset visibility;
- Sentinel probe/failover decision/timing metrics;
- Cluster MOVED/ASK/CROSSSLOT, migration and gossip metrics;
- RESP2/RESP3 request/reply/Push counters;
- immutable server `MetricsSnapshot` plus an `INFO` teaching view;
- deterministic observability demo;
- repeatable benchmark/profile workflow including pprof guidance.

Unchanged:

- command semantics and mutation ordering;
- Store/Object encodings;
- AOF/RDB serialization formats;
- replication wire frames/offset semantics;
- Sentinel election/failover policy;
- Cluster ownership/migration semantics;
- RESP2/RESP3 negotiation semantics.

## 4. Instrumentation invariants

- metrics never enter Store, SnapshotEntry, AOF, RDB, replication frames or Cluster topology;
- metrics never decide whether a command succeeds, evicts, expires, replicates, redirects or fails over;
- observation of counters is snapshot/copy based;
- hot-path counters use bounded synchronization primitives;
- latency histograms use fixed buckets, not unbounded per-request samples;
- failed instrumentation cannot turn a successful database operation into a failure;
- INFO is read-only and must not expose mutable internal maps/slices;
- performance comparisons always state engine/workload/concurrency/operation count.

## 5. Planned observable surfaces

### Command/session

- total client commands;
- errors;
- per-command counts;
- command latency histogram;
- active/accepted connections;
- RESP2/RESP3 requests;
- Push frames.

### Network

- bytes read/written;
- reactor poll calls/events/wait duration;
- input/output buffer limit events;
- legacy/reactor accepted connections.

### State / memory

- expired keys;
- evicted keys;
- key count at snapshot time;
- configured maxmemory policy metadata.

### Persistence

- AOF append operations/bytes/failures/duration;
- RDB save operations/bytes/failures/duration;
- AOF rewrite operations/bytes/failures/duration.

### Replication

- primary repl-id/end offset/backlog start/window;
- full/partial sync counts;
- live subscribers;
- replica repl-id/applied offset;
- reconnect/link failure counters where observable without changing protocol.

### Sentinel / Cluster

- probe attempts/failures and Tick/failover duration;
- SDOWN/ODOWN/election/promotion/reconfigure event counts;
- MOVED/ASK/CROSSSLOT/CLUSTERDOWN counters;
- migration attempts/keys/failures/duration;
- gossip pages/records/merge counts.

## 6. Performance evidence

v0.12 must keep two kinds of measurements separate:

```text
microbenchmark
  → one mechanism / code path

end-to-end workload
  → real TCP + parser + command + Store + reply
```

No context-free statement such as “reactor is X% faster” is valid without workload parameters.

## 7. Tests that must prove the mechanism

- concurrent counter/histogram updates are race-free;
- histogram bucket/count/sum snapshots are monotonic and bounded;
- INFO is read-only and snapshot-based;
- command latency/count metrics increase on both network engines;
- RESP2/RESP3 and Push counters reflect real wire sessions;
- expiration/eviction counters reflect existing mechanisms without changing results;
- persistence metrics distinguish success/failure and expose byte/duration evidence;
- replication snapshots expose full/partial sync and backlog state without changing PSYNC behavior;
- Sentinel/Cluster metrics are observational only;
- historical demos remain green;
- deterministic observability demo prints a coherent cross-layer snapshot.

## 8. Explicit non-goals

v0.12 does not claim:

- Prometheus/OpenTelemetry compatibility;
- distributed metrics aggregation;
- production alerting/SLO infrastructure;
- production Redis performance parity;
- zero instrumentation overhead;
- benchmark portability across machines without normalization;
- optimization by deleting educational mechanisms.

## 9. Definition of Done

- observability package and snapshots implemented;
- cross-layer observation points tested under `-race`;
- INFO and demo implemented;
- benchmark/profile workflow documented and executable;
- existing command/persistence/replication/Sentinel/Cluster/RESP3 semantics unchanged;
- VERSION/docs synchronized;
- exact `v0.12` SHA passes complete CI;
- `main` fast-forwarded to same SHA;
- same SHA passes CI on `main`;
- `main` and `v0.12` compare identical.

## 10. Read this version in order

```text
internal/observability/metrics.go
  ↓
internal/server/metrics.go
  ↓
network / command / persistence observation points
  ↓
replication / sentinel / cluster snapshots
  ↓
examples/observability/main.go
  ↓
benchmarks + docs/PERFORMANCE.md
```

## 11. Next step

v0.12 is the planned completion of the original architecture-learning sequence. Any v0.13 should begin a new learning arc only after v0.12 evidence identifies a specific mechanism worth optimizing or replacing.
