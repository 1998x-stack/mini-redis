# mini-redis v0.4 — Core Data Types and Adaptive Encodings

## 1. Identity

- Integrated branch after validation: `main`
- Historical learning snapshot: `v0.4`
- Version label: `v0.4.0`
- Previous snapshot: `v0.3`
- Status: implemented and validated

## 2. Primary question

**Why can one Redis logical type use different physical data structures without changing command semantics?**

v0.3 introduced `Object{Type, Encoding, payload}`. v0.4 turns that boundary into a real mechanism:

```text
logical command
      ↓
Object.Type
      ↓
Object.Encoding
      ↓
compact representation ── threshold/content ──► general representation
```

## 3. What changed relative to v0.3?

v0.3 made the top-level key dictionary explicit. v0.4 keeps RESP, event loop, Dict, TTL and AOF behavior stable, but enriches each key's value with collection semantics and encoding transitions.

The key idea is that public behavior belongs to the logical type while memory/performance trade-offs belong to the physical encoding.

## 4. Encoding matrix

| Logical type | Compact/reference encoding | General encoding | Upgrade trigger |
| --- | --- | --- | --- |
| String | `int` | `raw` | value shape |
| List | `listpack`-like slice | chunked `quicklist` | > 8 entries |
| Hash | `listpack`-like field/value array | custom `Dict` (`hashtable`) | > 8 fields |
| Set | sorted `intset` | custom `Dict` (`hashtable`) | non-canonical integer or > 8 members |
| ZSet | sorted compact array | member→score `Dict` + span-aware `SkipList` | > 8 members |

The threshold `8` is intentionally tiny so representation changes are observable in tests and demos. It is not a production Redis default.

Upgrades are one-way in v0.4. Automatic downgrade is deferred because it adds policy complexity without improving the current learning goal.

## 5. Commands added

### List

- `LPUSH`, `RPUSH`
- `LPOP`, `RPOP`
- `LLEN`, `LRANGE`

### Hash

- `HSET`, `HGET`
- `HDEL`, `HLEN`, `HGETALL`

### Set

- `SADD`, `SREM`
- `SISMEMBER`, `SMEMBERS`, `SCARD`

### Sorted Set

- `ZADD`, `ZSCORE`
- `ZRANK`, `ZRANGE`, `ZREM`

`OBJECT ENCODING key` exposes the current physical representation.

## 6. SkipList learning mechanism

Large ZSets use two structures at once:

```text
member ─► score Dict       O(1)-style membership/score lookup
              +
score/member SkipList      ordered traversal + rank
```

The SkipList uses probabilistic levels and per-level spans. Equal scores are ordered lexicographically by member, giving one deterministic total order.

Tests compare 500 randomly scored members against a normal sorted reference and validate every rank.

## 7. Atomicity, TTL and empty-key rules

Collection read-modify-write logic lives inside `versionedmemory.Memory`, not command handlers.

```text
command
   ↓
Store collection method
   ↓ one Memory lock
lazy-expire → type check → mutate encoding → persist object
```

Invariants:

- collection mutation preserves an existing TTL;
- `WRONGTYPE` is returned for incompatible logical types;
- when the last collection element is removed, the key and its expiry metadata disappear together;
- encoding conversion does not change logical results.

## 8. AOF boundary

AOF still stores logical commands rather than object memory layouts:

```text
RPUSH list a b c
HSET hash f v
SADD set 1 2
ZADD z 1 a
```

Replay reconstructs whichever encoding the current implementation chooses. AOF therefore remains independent of listpack/quicklist/intset/skiplist layout.

## 9. Validation

The exact v0.4 candidate is required to pass:

```bash
gofmt -d .
go vet ./...
go test -race ./...
go build ./cmd/mini-redis-server
make rehash-demo
make encodings-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

The smoke suite triggers real encoding upgrades through TCP on both network engines.

## 10. Important source files

```text
internal/store/store.go
internal/store/collections.go
internal/skiplist/skiplist.go
internal/store/versionedmemory/memory.go
internal/command/core.go
examples/encodings/main.go
```

Recommended read order:

```text
Object type/encoding
      ↓
collection encoding implementations
      ↓
SkipList
      ↓
Memory atomic collection API
      ↓
commands
      ↓
smoke + invariant tests
```

## 11. Non-goals

- no production byte-level listpack implementation;
- no quicklist compression;
- no automatic encoding downgrade;
- no `WITHSCORES` / advanced range syntax;
- no set algebra or blocking-list commands;
- no RDB;
- no AOF rewrite;
- no maxmemory/eviction;
- no transactions, replication, Sentinel or Cluster.

## 12. Next version

v0.5 moves from memory representation to persistence depth:

```text
current logical state ──► RDB-like snapshot
AOF history            ──► rewrite / compaction
                              ↓
                       atomic replacement
```

Primary question: **how does an in-memory database trade write cost, restart speed and durability while replacing persistence files safely?**
