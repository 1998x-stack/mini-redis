# mini-redis v0.6 — Memory Pressure and Sampled Eviction

## 1. Identity

- Integrated branch after validation: `main`
- Historical learning snapshot: `v0.6`
- Version label: `v0.6.0`
- Previous snapshot: `v0.5`
- Status: implemented and validated

## 2. Primary question

**Why are TTL expiration and maxmemory eviction different mechanisms, and why does Redis approximate LRU/LFU by sampling instead of maintaining one perfect global ordering?**

v0.6 adds a runtime policy layer above the stable Store:

```text
command
   ↓
server persistence / admission boundary
   ↓
memorypolicy.Manager
   ├── logical-byte estimator
   ├── last-access metadata
   ├── logarithmic LFU metadata
   └── sampled victim selection
   ↓
versionedmemory.Memory
   ├── values
   ├── TTL
   └── incremental Dicts
```

The policy layer is deliberately outside `Object` and `SnapshotEntry`, so access heat is runtime state rather than durable database state.

## 3. Expiration vs eviction

Expiration asks whether a key is still valid because time passed. Eviction asks which still-valid key may be sacrificed because the configured memory budget is exceeded.

`EnforceMaxMemory()` first performs expiry cleanup and counts those removals separately. Only later policy-selected live keys are reported as evictions.

## 4. Memory accounting

`ApproxSnapshotEntryBytes` provides deterministic educational accounting for key bytes, logical payload bytes, expiry overhead and small fixed object metadata constants.

It is intentionally **not** Go heap accounting, allocator accounting or process RSS. The goal is repeatable policy experiments, not pretending to reproduce Redis' allocator statistics.

## 5. Policies

Implemented:

- `noeviction` — reject memory-growing writes that still exceed the limit;
- `allkeys-random` — choose a random live key;
- `allkeys-lru` — sample candidates and evict the oldest access timestamp;
- `allkeys-lfu` — sample candidates and evict the lowest decayed frequency.

The sample size is configurable. Candidate scoring is bounded by that sample; the implementation intentionally does not maintain a globally sorted LRU/LFU list.

## 6. LFU teaching model

LFU metadata uses an 8-bit counter. A hotter counter becomes progressively harder to increment, giving sublinear/logarithmic-style growth. Time-based decay reduces stale historical heat.

This preserves the important Redis idea without claiming byte-for-byte production LFU constants.

## 7. noeviction atomicity

For commands classified as capable of growing memory, the server captures the existing v0.5 logical snapshot before execution. If the post-command state exceeds maxmemory under `noeviction`, that logical state is restored and the command is not appended to AOF.

Removal-only commands such as `DEL`, pops and member deletion remain usable even when a process starts above the configured limit.

This full-snapshot rollback is intentionally simple and expensive; it makes the correctness boundary visible before later performance work.

## 8. Eviction + AOF

When an eviction policy removes live keys, the acknowledged mutation and one logical `DEL <victims...>` are appended as one AOF batch. Restart therefore reconstructs the same surviving logical keyspace.

LRU/LFU metadata itself is never emitted to AOF or RDB. After restart, access heat starts fresh.

## 9. Configuration

```text
-maxmemory <bytes>                  # 0 disables memory-pressure enforcement
-maxmemory-policy noeviction|allkeys-random|allkeys-lru|allkeys-lfu
-maxmemory-samples <n>
-lfu-decay <duration>
-eviction-seed <int64>              # deterministic teaching/test seed
```

## 10. Validation

The exact v0.6 candidate must pass:

```bash
gofmt -d .
go vet ./...
go test -race ./...
go build ./cmd/mini-redis-server
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

Mechanism tests prove:

- expiry cleanup is not counted as eviction;
- `noeviction` does not delete live keys;
- rejected memory-growing commands roll back logical state and leave AOF unchanged;
- removal-only commands work above the noeviction limit;
- sampled LRU chooses the oldest candidate first when the sample covers the set;
- sampled LFU protects a repeatedly accessed key;
- candidate scoring obeys the configured sampling budget;
- policy metadata does not cross the snapshot boundary;
- eviction-generated `DEL` survives AOF restart.

## 11. Important source files

```text
internal/store/memory_policy.go
internal/store/memorypolicy/manager.go
internal/server/server.go
internal/config/config.go
internal/command/registry.go
examples/eviction/main.go
```

## 12. Non-goals

- no exact Go heap/RSS or allocator accounting;
- no `volatile-*` eviction policy family;
- no claim of production Redis LFU constants;
- no optimized pre-write memory delta estimator;
- no transactions or Pub/Sub;
- no replication.

## 13. Next version

v0.7 moves from single-command atomicity to transactions and message fan-out:

```text
WATCH versions → MULTI queue → EXEC / DISCARD
                         +
               SUBSCRIBE / PUBLISH
```

Primary question: **how can Redis queue commands, detect optimistic conflicts, execute a transaction as one ordered unit, and support connections whose role changes into Pub/Sub delivery?**
