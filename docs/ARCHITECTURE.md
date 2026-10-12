# Architecture — v0.3

## 1. Architectural goal

v0.3 keeps the v0.2 event loop and public String/TTL/AOF behavior, but replaces the memory engine's opaque Go maps with a visible Redis-like dictionary.

```text
redis-cli
   ↓
RESP / reactor
   ↓
command.Registry
   ↓
store.Store
   ↓
versionedmemory.Memory
   ├── data    Dict[Object]
   └── expires Dict[time.Time]
   ↓
successful mutation
   ↓
AOF
```

Command code still knows only the Store interface. Dictionary mechanics remain a storage implementation detail.

## 2. Dict structure

Each table is an array of bucket-chain heads:

```text
buckets
 [0] ─► entry(key,value) ─► entry ─► nil
 [1] ─► nil
 [2] ─► entry ─► nil
```

Separate chaining makes collisions explicit without introducing open-addressing tombstones as a second lesson. A `Hasher` boundary keeps the algorithm replaceable; deterministic FNV-1a is the educational default, not an attempt to match Redis' exact hash implementation.

Power-of-two sizes make bucket selection visible:

```text
bucket = hash & (size - 1)
```

## 3. Two hash tables during rehash

A Dict owns `ht[0]`, `ht[1]` and `rehashIndex`.

Stable:

```text
ht[0] = live table
ht[1] = empty
rehashIndex = -1
```

Rehashing:

```text
              old/current                       target
              ht[0]                             ht[1]
       ┌────────────────┐                ┌────────────────────┐
       │ bucket 0       │                │ bucket 0           │
       │ bucket 1       │    migrate     │ bucket 1           │
       │ ...            │ ─────────────► │ ...                │
       └────────────────┘                └────────────────────┘
                 ▲
           rehashIndex
```

Lookups search both. New keys go to `ht[1]`. Existing keys update the table that already owns them. That owner rule prevents duplicate logical keys during migration.

## 4. Entry-level incremental migration

Moving one whole bucket is not strongly bounded when a collision chain is long. v0.3 defines one work unit as:

```text
non-empty source bucket -> move ONE head entry
empty source bucket     -> advance rehashIndex once
```

Thus `RehashStep(N)` consumes at most `N` migration/scan units. A chain `A -> B -> C -> D` requires four entry-migration units. This makes the event-loop maintenance budget directly inspectable.

## 5. Rehash scheduling

Two mechanisms make progress.

### Opportunistic

`Dict.Get`, `Set` and `Delete` each attempt one work unit before the normal operation.

### Time-event maintenance

The existing maintenance point becomes:

```text
maintenance timer
   ├── ExpireDue(sample)
   └── RehashStep(work budget)
```

The server depends only on optional `store.RehashStepper`; Store depends on Dict; Dict never imports server/runtime code.

## 6. Grow / shrink policy

Reference policy:

```text
grow:   used >= size
         target = size * 2

shrink: used / size < 10%
         keep headroom
         never below initial size
```

Mechanism and policy are separate: policy determines *when/what size*; rehash determines *how migration happens*.

## 7. Iteration during migration

A rehashing dictionary has live entries in two tables. `Range` visits both; its callback must not mutate that Dict. Memory paths that may delete expired keys first snapshot names with `Keys()` and mutate afterward.

## 8. Keyspace and expiry metadata

The two conceptual dictionaries are preserved:

```text
keyspace Dict: key -> Object
expires Dict:  key -> absolute deadline
```

A persistent key exists only in the first; a volatile key exists in both. They may resize independently.

## 9. Object type and encoding

The keyspace value is now:

```text
Object
  Type
  Encoding
  payload
```

Current String representations:

```text
raw  -> string payload
int  -> int64 payload
```

`NewString` uses `int` only for byte-for-byte canonical integer text. Therefore `"42"` may be encoded as int, while `"001"`, `"+1"` and `"-0"` remain raw. Commands use accessors rather than switching directly on Go payload types.

This is the architectural bridge to v0.4, where one logical List/Hash/Set/ZSet can use multiple physical encodings.

## 10. Atomicity and concurrency

Memory still owns a mutex; Dict is deliberately not concurrency-safe. This keeps hashing/rehash separate from synchronization policy. Reactor commands remain serial as in v0.2, while the legacy goroutine engine safely shares the Store through the mutex.

Because even `Get` may progress rehash or lazily expire a key, Memory uses its write lock for key access in v0.3.

## 11. Persistence remains above representation

AOF records logical commands, not buckets, object payload layout or `rehashIndex`.

```text
AOF SET key value
      ↓ replay
NewString(value)
      ↓
current encoding
      ↓
current Dict layout
```

Restart may reconstruct a different physical layout while preserving logical state. Integration tests replay enough keys to cross multiple resizes and verify the restored state.

## 12. Reference demo

```bash
make rehash-demo
```

The example forces every key into one bucket and prints the state after each `RehashStep(1)`, making the bounded-work invariant visible.

## 13. Next layer

v0.4 uses the Object/Encoding boundary for collections:

```text
logical object
      ↓
encoding choice
      ↓
compact representation OR general representation
```

Only after this boundary exists does it make sense to teach listpack/quicklist-like structures, integer sets, hash representation conversion and ZSet's dictionary + skip-list pairing.
