# mini-redis — Architecture-Driven Development Plan

> Canonical roadmap for `main`. `main` must always be the newest integrated, validated state; `v0.x` branches are frozen educational snapshots.

## 1. Why this project exists

mini-redis removes one Redis layer of “magic” at a time: protocol, scheduling, dictionary internals, object encodings, persistence, memory policy and distributed topology. The desired codebase should explain why each boundary exists rather than maximize command count.

## 2. Architectural reference model

```text
Client / redis-cli
      ↓
TCP / event loop
      ↓
RESP
      ↓
command registry / semantics
      ↓
Store API
 ┌────┼────┐
 ▼    ▼    ▼
Object TTL Dict
      ↓
memory state
  ┌───┴───┐
  ▼       ▼
 AOF     RDB

later: replication -> Sentinel / Cluster
```

## 3. Repository / completion model

```text
main ───────────────────────────────► newest validated
  ├── v0.1 request/state/persistence
  ├── v0.2 nonblocking I/O + epoll
  ├── v0.3 Dict + rehash + object metadata
  ├── v0.4 collections + encodings + skip list
  └── ...
```

A version completes only after mechanism implementation, invariant/error/integration tests, race/recovery checks where applicable, docs/version update, and synchronization of the validated state into `main`.

## 4. Completed versions

### v0.1 — Request → state → persistence

Primary question: what happens between `SET key value` bytes and durable observable state?

Implemented RESP2 request/response paths, command registry, String Store, core String/key/TTL commands, lazy + active expiration, RESP AOF, fsync policies, replay and absolute expiry rewrite. Networking baseline was one goroutine/client; Go maps intentionally hid dictionary internals.

### v0.2 — Nonblocking networking + event loop

Primary question: why can one reactor serve many sockets without one blocking thread per connection?

Implemented Poller abstraction, Linux epoll, nonblocking listener/clients, one reactor, per-client buffers, incremental RESP parsing, pipeline ordering, partial writes, writable-interest control, buffer ceilings/backpressure, expiry/idle timers, wake-pipe shutdown, legacy comparison path and dual-engine tests/benchmark harness.

### v0.3 — Redis-like Dict, incremental rehash and object metadata

Primary question: how can a hash table resize while the event loop keeps serving commands without one stop-the-world resize?

```text
Dict
 ├── ht[0] source/current
 ├── ht[1] target during rehash
 └── rehashIndex

lookup: ht[0] -> miss -> ht[1]
maintenance: bounded RehashStep(N)
```

Implemented:

- generic explicit bucket tables with separate chaining;
- hash-function boundary + deterministic FNV-1a reference hasher;
- power-of-two capacities and visible grow/shrink policy;
- two-table migration state;
- entry-level bounded work: one unit moves at most one entry or skips one empty bucket;
- correct lookup/update/delete while keys occupy either table;
- no duplicate logical-key ownership across rehash tables;
- explicit no-mutation-inside-`Range` policy;
- opportunistic rehash plus timer-driven `RehashStepper` maintenance;
- keyspace and expiry metadata both using the custom Dict;
- logical Object type separated from physical encoding;
- String `raw` and canonical `int` encodings;
- `OBJECT ENCODING` inspection;
- forced-collision rehash demo, randomized Go-map reference model and dictionary benchmarks;
- AOF replay validation across multiple resizes.

Non-goals: no collection commands/compact encodings and no persistence redesign.

## 5. v0.4 — Core data types and internal encodings

Primary question:

> Why can one Redis logical type use multiple physical representations?

Required logical types and educational command subset:

- List: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`;
- Hash: `HSET`, `HGET`, `HDEL`, `HLEN`, `HGETALL`;
- Set: `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`;
- ZSet: `ZADD`, `ZSCORE`, `ZRANK`, `ZRANGE`, `ZREM`.

Required architecture:

- `WRONGTYPE` behavior before broad APIs;
- stable logical Object API independent of encoding;
- explicit representation thresholds and conversion tests;
- List compact/reference representation progression;
- Hash compact → Dict;
- Set integer-set-like → Dict;
- ZSet member→score Dict + ordered skip list;
- identical logical answers before/after conversion.

## 6. v0.5 — Persistence depth: RDB + AOF rewrite

Primary question: how does an in-memory database trade write cost, restart speed and durability?

Implement deterministic educational RDB with type + absolute expiry metadata, corruption detection, blocking snapshot reference path, atomic temp-file replacement, AOF rewrite from logical state, mutation capture during rewrite, atomic replacement, startup precedence and corruption policy. Fork/COW comes only as an isolated experiment/concept after correctness.

## 7. v0.6 — Expiration, maxmemory and eviction

Implement time-budgeted active expiry, approximate memory accounting, `maxmemory`, no-eviction/random policies and sampled LRU/LFU approximations. Keep expiration (time-invalid state) conceptually distinct from eviction (valid state removed under memory policy).

## 8. v0.7 — Transactions + Pub/Sub

Implement `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, queued uninterrupted execution and optimistic key versioning; then `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` and subscribed-client/output-buffer behavior. Redis transactions are not SQL rollback transactions.

## 9. v0.8 — Replication + partial resynchronization

Implement primary/replica roles, initial full sync, replication offset, backlog ring buffer, command propagation, reconnect/partial resync when backlog covers the gap, fallback full sync otherwise, and convergence tests.

## 10. v0.9 — Sentinel-inspired high availability

Implement educational health monitoring, subjective/objective failure, quorum, election decision, replica promotion, topology reconfiguration and explicit split-brain limitations.

## 11. v0.10 — Cluster / hash slots

Implement 16384 slots, deterministic key→slot mapping, ownership, `MOVED`, migration/importing state, `ASK`, basic topology gossip and resharding experiment.

## 12. v0.11 — RESP3

Add richer scalar/container types, push messages where useful, protocol negotiation and compatibility tests only after server/data/distributed concepts are stable.

## 13. v0.12 — Observability and performance engineering

Add command latency, connection/event-loop counters, expiration/eviction metrics, persistence timing, replication lag/offset metrics, CPU/memory profiling examples and repeatable benchmark methodology. A performance result matters only when the mechanism causing it is explained.

## 14. Sequencing invariant

```text
request semantics
   ↓
event-loop scheduling
   ↓
dictionary internals
   ↓
objects / collections / encodings
   ↓
persistence depth
   ↓
memory policy
   ↓
transactions / fan-out
   ↓
replication
   ↓
failover
   ↓
sharding
   ↓
protocol / observability refinement
```

Do not skip forward because a later feature is more visible; the learning value is the dependency chain.

## 15. Current directive

Current integrated version after validation: **v0.3**.

Next active version: **v0.4**, created from latest validated `main`.

v0.4 must build collection types on the logical type / physical encoding boundary introduced in v0.3. Do not skip directly to persistence/distribution before representation conversion and `WRONGTYPE` semantics are explicit.
