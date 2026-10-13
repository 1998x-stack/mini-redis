# Architecture — v0.4

## 1. Current stack

```text
redis-cli / client
      ↓
RESP2 + reactor/legacy transport
      ↓
command.Registry
      ↓
store.Store
      ↓
versionedmemory.Memory
      ├── keyspace Dict[string → Object]
      └── expires  Dict[string → deadline]
                    │
                    ▼
            Object{Type, Encoding, payload}
```

v0.4 adds complexity below the Object boundary without coupling protocol or networking to concrete representations.

## 2. Logical type vs physical encoding

A Redis-style object has two separate questions:

```text
What operations are legal?      → Type
How is the value represented?   → Encoding
```

Current matrix:

```text
String : int | raw
List   : listpack → quicklist
Hash   : listpack → hashtable
Set    : intset   → hashtable
ZSet   : listpack → skiplist (+ member-score Dict)
```

A command checks logical type; internal methods choose representation-specific algorithms.

## 3. List

Small lists use a contiguous slice. It has excellent conceptual locality and makes compact storage visible. Above the educational threshold it becomes a chunked doubly-linked quicklist:

```text
node [a b c d] ⇄ node [e f g h] ⇄ node [i ...]
```

Each node stores a small contiguous chunk rather than one heap node per element. This demonstrates why Redis moved beyond a naive linked list.

## 4. Hash

Small hashes use a field/value array. Lookup is linear but memory structure is compact and simple. Above the threshold fields are copied into v0.3's custom incremental `Dict`.

```text
small: [f1,v1,f2,v2,...]
              ↓ upgrade
large: Dict[field → value]
```

The public `HGET/HSET` behavior is unchanged across the transition.

## 5. Set

If every member is canonical integer text, the compact representation stores sorted `int64` values:

```text
[1, 2, 7, 19]
```

A non-canonical member such as `"01"` cannot be collapsed to integer `1` without changing identity, so the Set upgrades to a string-key Dict. Size can also trigger the upgrade.

## 6. Sorted Set

Small ZSets use a sorted slice. Large ZSets combine:

```text
Dict[member → score]
        +
SkipList[(score, member) ordered]
```

The Dict answers membership/score lookup efficiently. The SkipList answers order/rank traversal. Updating a member's score deletes the old ordered node and inserts the new one.

### SkipList spans

Each forward link stores a span. Traversing upper levels accumulates skipped element counts, allowing rank calculation without scanning level 0 from the beginning.

Tie-break order is `(score, member)` so every member has a deterministic rank.

## 7. Encoding transition policy

v0.4 uses `CompactMaxEntries = 8` to make conversions easy to observe:

```text
compact encoding
     │
     ├── size > 8
     └── content incompatible with compact form
              ↓
        general encoding
```

Transitions are one-way. Downgrade/hysteresis is a later policy question.

## 8. Store atomicity and TTL

Command handlers never pull an Object out, mutate it independently and write it back. Collection mutations are Store operations performed while Memory owns its mutex.

This preserves:

- operation-level atomicity in the legacy shared-store engine;
- serial command semantics in the reactor;
- existing TTL metadata during collection mutations;
- coordinated deletion of value and TTL when a collection becomes empty.

## 9. WRONGTYPE

A key has one logical type. Therefore:

```text
SET k hello
LPUSH k x
```

returns `WRONGTYPE` rather than interpreting a String payload as a List. The error belongs to logical type validation, not physical encoding.

## 10. Persistence stays logical

AOF remains above physical encoding. It records commands and never serializes quicklist nodes, Dict buckets or SkipList levels. A restart may rebuild a physically different representation with the same logical state.

v0.5 will add an RDB-like state snapshot and AOF rewrite, which will force the project to define a stable logical serialization boundary for every type.

## 11. Demos

```bash
make rehash-demo
make encodings-demo
```

The first exposes v0.3 dictionary migration; the second shows each v0.4 encoding transition.
