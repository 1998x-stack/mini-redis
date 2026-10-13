# Architecture — v0.6

## 1. Current stack

```text
redis-cli / client
      ↓
RESP2 + reactor/legacy transport
      ↓
command.Registry
      ↓
Server.persistenceMu
      ↓
memorypolicy.Manager
      ↓
versionedmemory.Memory
      ├── keyspace Dict[string → Object]
      └── expires  Dict[string → deadline]
                    │
                    ▼
            logical SnapshotEntry[]
               ┌────┴────┐
               ▼         ▼
              RDB       AOF
```

v0.6 inserts runtime memory policy above the durable Store without changing the logical persistence representation.

## 2. Why not put LRU/LFU inside Object?

Doing so would blur two layers:

```text
logical durable state: key, type, value, expiry
runtime policy state: last access, LFU heat, sampling counters
```

`memorypolicy.Manager` wraps the Store, while `Snapshot()` simply delegates to the underlying logical store. `RestoreSnapshot()` deliberately clears access metadata.

Therefore restart preserves data but not historical access heat.

## 3. Approximate memory accounting

v0.6 estimates bytes from logical `SnapshotEntry` values plus small fixed teaching overheads for key/object/expiry/access metadata.

This yields deterministic tests and demos. It does **not** equal Go allocator bytes, heap profiles or process RSS. Exact allocator accounting is intentionally deferred.

## 4. Expiration vs eviction

```text
expiration
  cause: deadline passed
  meaning: key is no longer valid

 eviction
  cause: used estimate > maxmemory
  meaning: a still-valid key is sacrificed by policy
```

Pressure enforcement runs full expiry cleanup first. Expired keys are counted separately and are never reported as policy victims.

## 5. Policy model

### noeviction

Memory-growing commands execute behind `Server.persistenceMu` with a pre-command logical snapshot available for rollback. If the result remains over maxmemory, the snapshot is restored, OOM is returned, and the rejected command never reaches AOF.

Removal-only mutations remain usable above the limit.

### allkeys-random

Choose one live key uniformly from the current candidate set.

### sampled LRU

Select at most `maxmemory-samples` candidates for one victim decision and choose the oldest `lastAccess` among them.

No global exact LRU linked list/tree is maintained.

### sampled LFU

Use the same bounded candidate sample, but prefer the lowest decayed frequency. The counter is 8-bit and increments probabilistically: hotter values become harder to increment. Elapsed decay periods reduce stale heat.

The exact constants are educational equivalents, not claims of production Redis tuning.

## 6. Sampling boundary

The implementation may enumerate logical keys to construct the educational candidate set, but **scoring/comparison work for one victim selection is bounded by the configured sample count**. This is the explicit v0.6 bounded-work claim.

Later performance versions may make candidate discovery itself more incremental.

## 7. Server / persistence ordering

For a successful memory-growing command under an eviction policy:

```text
lock persistenceMu
   ↓
execute logical mutation
   ↓
expire invalid keys
   ↓
if over limit: sample + evict complete live keys
   ↓
build AOF batch:
  original mutation
  DEL victim...
   ↓
append batch
   ↓
unlock / acknowledge
```

For `noeviction`, an OOM path restores the pre-command logical snapshot before returning and does not append the rejected mutation.

This reference implementation favors a visible correctness boundary over efficient pre-admission byte deltas.

## 8. Eviction and key metadata

Eviction calls the normal Store delete path, so value and expiry dictionary entries disappear together. The wrapper also removes its access metadata for that key.

Incremental Dict rehash invariants therefore remain owned by the existing Store rather than being reimplemented by the policy layer.

## 9. Persistence interaction

AOF needs to persist eviction because eviction changes logical state. The server emits `DEL` for selected victims after the triggering mutation.

RDB and AOF do not persist LRU/LFU metadata. On recovery, base logical state is restored/replayed first; only then is the memory-policy wrapper installed.

That ordering prevents replaying historical commands from accidentally manufacturing access heat.

## 10. Failure and edge cases

- a rejected noeviction command restores logical state and does not change AOF;
- reads never trigger eviction;
- removal-only writes remain available above a noeviction limit;
- if expiry alone frees enough memory, no live key is evicted;
- restored keys without fresh access metadata are naturally considered cold when sampled after startup;
- AOF failure after an already-applied acknowledged-path mutation remains the existing educational durability limitation and is not hidden by v0.6.

## 11. Demo

```bash
make eviction-demo
```

It demonstrates:

1. an expired key removed without counting as eviction;
2. noeviction returning OOM while the live key remains;
3. sampled LRU choosing the oldest candidate;
4. sampled LFU protecting a repeatedly accessed key.

## 12. Next architectural layer

v0.7 introduces per-client state and optimistic transactions:

```text
WATCH versions
      ↓
MULTI queue
      ↓
EXEC ordered unit / DISCARD

plus

channel broker ↔ subscribed connections
```

Transaction/watch state and Pub/Sub subscriptions are runtime/session state, not normal durable key values.
