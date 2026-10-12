# mini-redis v0.3 — Dict, Incremental Rehash and Object Encoding

## 1. Identity

- Integrated branch after validation: `main`
- Historical learning snapshot: `v0.3`
- Version label: `v0.3.0`
- Previous snapshot: `v0.2`
- Status: implemented and validated

## 2. Version theme

**Replace Go's hidden keyspace map with an explicit Redis-like dictionary that can resize incrementally without one large stop-the-world copy.**

v0.2 made scheduling visible:

```text
many sockets -> epoll -> one event loop
```

v0.3 makes the central in-memory lookup structure visible:

```text
command
   ↓
store.Store
   ↓
Memory
   │
   ├── data Dict[Object]
   │     ├── ht[0]
   │     ├── ht[1]
   │     └── rehashIndex
   │
   └── expires Dict[time.Time]
         ├── ht[0]
         ├── ht[1]
         └── rehashIndex
```

Primary question:

> How can a hash table resize while the event loop keeps serving commands, and how can logical Redis values remain stable while their physical representation changes?

## 3. What changed relative to v0.2, and why?

v0.2 deliberately left `map[string]store.Object` and `map[string]time.Time` inside the memory engine. Those maps hide hash buckets, collision chains, load factor, resize triggers and migration cost.

v0.3 replaces those two maps with `internal/dict.Dict[T]` while preserving the command-facing `store.Store` contract. Networking, RESP, command registration, TTL semantics and AOF remain conceptually unchanged, making dictionary mechanics the independent variable.

A second boundary is added inside values:

```text
logical type              physical encoding
────────────              ─────────────────
string "hello"   ─────►   raw string
string "42"      ─────►   int64
```

`GET` still returns the same logical string. `OBJECT ENCODING key` exposes the representation for learning.

## 4. Dictionary architecture

### 4.1 Explicit buckets and collision chains

Each table is an array of bucket-chain heads:

```text
bucket 0 -> entry -> entry -> nil
bucket 1 -> nil
bucket 2 -> entry -> nil
```

The implementation uses separate chaining. The hash function is injected through a `Hasher` boundary; the default educational implementation is deterministic FNV-1a. Bucket count is a power of two, so selection is explicit:

```text
index = hash(key) & (size - 1)
```

### 4.2 Two-table resize state

Normal state:

```text
ht[0] = current table
ht[1] = empty
rehashIndex = -1
```

Rehash state:

```text
ht[0] = source table
ht[1] = target table
rehashIndex >= 0
```

During migration, lookups search both tables; updates modify whichever table owns the key; new keys go to `ht[1]`; deletes search both. The same logical key is never intentionally duplicated.

## 5. Strong bounded-work rule

Moving one whole bucket per step is not strongly bounded if that bucket has a long collision chain. v0.3 therefore defines one work unit as:

> move at most one entry, or advance past one empty source bucket.

For a collision chain `A -> B -> C -> D`, four separate migration units are required. Therefore `RehashStep(N)` consumes at most `N` explicit migration/scan units before returning to the server loop.

This is an educational strengthening of the incremental-rehash idea, not a claim that production Redis uses this exact work unit.

## 6. Resize policy

Reference thresholds:

- initial size: 8 buckets;
- grow when `used >= size`;
- grow target: 2×;
- shrink below 10% utilization, never below initial size;
- target sizes remain powers of two.

The lesson is the separation between resize **policy** and rehash **mechanism**.

## 7. Rehash progress sources

Normal dictionary operations make opportunistic progress:

```text
GET / SET / DELETE
       ↓
RehashStep(1)
       ↓
normal lookup/mutation
```

Periodic server maintenance also advances bounded work:

```text
maintenance timer
   ├── ExpireDue(sample)
   └── RehashStep(budget)
```

Runtime policy:

```text
-rehash-work-per-cycle 64
```

`0` disables timer-driven rehash; active traffic still progresses rehash. The server uses the optional `store.RehashStepper` capability rather than depending on the concrete memory implementation.

## 8. Iterator policy

During rehash, `Dict.Range` visits both tables. Its callback must not mutate the same Dict. Callers needing mutation take a key-name snapshot first. This explicit rule avoids relying on accidental iterator behavior during migration.

## 9. Keyspace and expiry dictionaries

Values and expiration metadata remain separate:

```text
keyspace Dict: key -> Object
expires Dict:  key -> absolute deadline
```

Persistent keys live only in the keyspace dictionary; volatile keys live in both. The two dictionaries can rehash independently.

## 10. Object type vs encoding

`store.Object` now models:

```text
Object
├── Type       logical API kind
├── Encoding   physical representation
└── payload    encoding-specific data
```

Current type is `ObjectString`; encodings are `raw` and canonical `int`.

`NewString` chooses integer encoding only when parse + format round-trips byte-for-byte:

```text
"42"    -> int
"-7"    -> int
"001"   -> raw
"+1"    -> raw
"-0"    -> raw
"hello" -> raw
```

This prevents representation changes from altering observable `GET` bytes. `INCR` accepts raw numeric or int-encoded strings and writes its canonical result as integer encoding.

## 11. Observable command

v0.3 adds:

```text
OBJECT ENCODING key
```

Example:

```text
SET a 42
OBJECT ENCODING a
"int"

SET b 001
OBJECT ENCODING b
"raw"

GET b
"001"
```

## 12. Critical invariants proved by tests

- collisions preserve all keys;
- lookup works while keys are split across both tables;
- update finds the existing owner rather than duplicating a key;
- delete works from either table;
- `RehashStep(1)` moves at most one colliding entry;
- iteration visits every logical key once during migration;
- grow and sparse shrink complete;
- 5000 randomized operations match a Go-map reference model while rehash is active;
- String commands remain correct during rehash;
- expiry metadata remains correct while the expiry Dict rehashes;
- maintenance advances bounded work;
- AOF replay across multiple resizes reconstructs the complete logical state;
- raw/int encoding does not change `GET` semantics.

## 13. Read order and demo

```text
internal/dict/dict.go
internal/dict/dict_test.go
        ↓
internal/store/store.go
        ↓
internal/store/versionedmemory/memory.go
        ↓
internal/command/core.go
        ↓
internal/server/server.go
internal/server/reactor_linux.go
```

Then run:

```bash
make rehash-demo
```

The demo forces all keys into one collision bucket and prints one-entry-at-a-time migration.

## 14. Validation

Validated on the exact implementation with:

```bash
gofmt -d .
go vet ./...
go test ./...
go test -race ./...
go build ./cmd/mini-redis-server
NETWORK_ENGINE=reactor make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
make rehash-demo
```

Darwin and Windows cross-compilation were also checked locally. Optional exploration:

```bash
make benchmark-dict
make benchmark
```

Benchmark results are machine/runtime observations, not universal Redis performance claims.

## 15. Mechanisms intentionally unchanged / non-goals

v0.3 does not redesign RESP, the epoll reactor, command registry, AOF format/fsync, TTL public semantics or the single logical database. It intentionally does not add List/Hash/Set/ZSet, compact collection encodings, RDB, AOF rewrite, eviction, transactions/PubSub or distributed features.

## 16. Next version

v0.4 builds on the visible Object/Dict boundaries to add logical collections and multiple physical encodings:

```text
Object Type
   ├── String  raw/int
   ├── List    compact/reference
   ├── Hash    compact -> dict
   ├── Set     integer-set-like -> dict
   └── ZSet    member→score dict + skip list
```

Primary question:

> Why can one stable Redis logical type switch physical representations as its size/content changes?
