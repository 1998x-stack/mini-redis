# mini-redis v0.5 — Persistence Depth

## 1. Identity

- Integrated branch after validation: `main`
- Historical learning snapshot: `v0.5`
- Version label: `v0.5.0`
- Previous snapshot: `v0.4`
- Status: implemented and validated

## 2. Primary question

**How does an in-memory database persist logical state, compact write history and replace persistence files without exposing a partially written database after a crash?**

v0.4 separated logical type from physical encoding. v0.5 adds the next boundary:

```text
Object{Type, Encoding, payload}
              ↓
      stable logical state
              ↓
       SnapshotEntry[]
          ┌───┴────┐
          ▼        ▼
         RDB      AOF rewrite
```

Persistence is deliberately defined above quicklist/Dict/intset/SkipList layout.

## 3. Stable logical snapshot boundary

`store.SnapshotEntry` contains only:

```text
key
logical type
logical values
absolute expiry deadline (optional)
```

It does **not** contain:

```text
Object.Encoding
quicklist nodes
Dict buckets / rehash cursor
intset memory layout
SkipList levels / spans
Go-private payload pointers
```

`Memory.Snapshot()` removes already-expired keys, sorts keys deterministically and extracts logical values. `RestoreSnapshot()` reconstructs objects through current constructors/mutators, so the restored process may choose a different physical encoding while preserving the same logical database.

## 4. RDB-like snapshot format

v0.5 introduces an educational, project-specific format rather than Redis binary compatibility:

```text
+----------+---------+----------------+-------------------+---------+
| "MRDB"   | version | payload length | logical payload   | CRC32   |
| 4 bytes  | u16     | u64            | canonical JSON    | u32     |
+----------+---------+----------------+-------------------+---------+
```

Properties:

- format magic: `MRDB`;
- format version: `1`;
- all five current logical types supported;
- absolute TTL milliseconds stored, never relative TTL durations;
- deterministic ordering for keys and unordered logical collections;
- checksum detects corruption;
- unsupported versions fail explicitly;
- ZSet scores use canonical strings on the wire, allowing `+Inf` and `-Inf` to round-trip even though JSON numeric literals cannot represent them.

This is intentionally an educational format, not Redis RDB compatibility.

## 5. Crash-safe file publication

Both snapshot publication and AOF rewrite use the same durability shape:

```text
valid target file
       │
       ├──────── remains untouched ───────────────┐
       │                                          │
create same-directory temp                       │
       ↓                                          │
write complete new contents                       │
       ↓                                          │
fsync(temp file)                                  │
       ↓                                          │
atomic rename(temp → target)  ◄───────────────────┘
       ↓
fsync(parent directory)
```

If work fails before rename, the previous valid target remains visible. Tests inject a failure in the pre-rename window and verify the old file bytes are unchanged.

`write()` alone is not treated as durability: file data and the directory entry are synchronized separately.

## 6. AOF rewrite

An append-only log preserves history, so repeated updates make it grow without bound:

```text
SET x 1
SET x 2
SET x 3
SET x 4
```

The current state needs only:

```text
SET x 4
```

v0.5 converts a logical snapshot to a minimal reconstruction stream:

```text
String -> SET
List   -> RPUSH
Hash   -> HSET
Set    -> SADD
ZSet   -> ZADD
TTL    -> PEXPIREAT absolute_ms
```

The rewritten AOF therefore records sufficient logical state, not previous mutation history or physical encodings.

## 7. Mutations during rewrite

The hard case is a write that arrives after the rewrite snapshot was taken but before the new AOF is published.

v0.5 names two boundaries:

```text
Server.persistenceMu
  └── command Store mutation + live AOF append

AOF Log.mu
  └── live descriptor + rewrite buffer + final swap
```

Rewrite timeline:

```text
lock persistenceMu
    ↓
Snapshot S0
    ↓
BeginRewrite(buffering ON)
unlock persistenceMu
    ↓
write compact S0 to temp file

meanwhile command C1:
    ├── execute against Memory
    ├── append to old/live AOF
    └── append same RESP bytes to rewrite buffer

final rewrite commit:
    ├── append buffered C1...Cn to temp
    ├── fsync temp
    ├── rename temp over old AOF
    ├── swap live descriptor
    └── fsync directory
```

Because no command can occur between `Snapshot()` and `BeginRewrite()`, and every later mutation is copied into the rewrite buffer, the replacement represents `S0 + all later acknowledged mutations`.

## 8. Startup policy

The recovery policy is deliberately simple and explicit.

### `appendonly=true`

- AOF is authoritative;
- RDB is ignored even if present;
- missing/empty AOF means an empty database;
- a truncated final AOF command is ignored;
- an invalid complete AOF command fails startup.

### `appendonly=false`

- load `-dbfilename` if it exists;
- missing RDB means an empty database;
- bad magic/version/checksum/truncation fails startup;
- expired entries are skipped during restoration and cannot be resurrected.

No automatic "try one file, then silently fall back to the other" heuristic is used in v0.5.

## 9. RDB vs AOF mental model

```text
RDB-like snapshot
  + compact state image
  + bounded restart input
  - work is concentrated at snapshot time
  - state only, not mutation history

AOF
  + every acknowledged mutation is represented
  + straightforward append path
  - file grows with history
  - restart may replay many redundant commands

AOF rewrite
  = keep AOF semantics while periodically collapsing history to state
```

The point of v0.5 is not to declare one universally better; it exposes the trade-off explicitly.

## 10. Validation

The exact v0.5 release SHA is required to pass:

```bash
gofmt -d .
go vet ./...
go test -race ./...
go build ./cmd/mini-redis-server
make rehash-demo
make encodings-demo
make persistence-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

Persistence-specific tests cover:

- all logical types through snapshot/restore;
- absolute TTL preservation;
- already-expired state not resurrected;
- deterministic RDB bytes;
- checksum/version/corruption failures;
- `+Inf/-Inf` ZSet score round-trip;
- failure before rename preserving the previous file;
- AOF rewrite history compaction;
- post-snapshot mutation buffering;
- truncated final AOF command recovery;
- RDB restart and AOF rewrite restart correctness;
- explicit AOF-over-RDB startup precedence.

## 11. Important source files

```text
internal/store/store.go
internal/store/versionedmemory/snapshot.go
internal/persistence/atomicfile.go
internal/persistence/commands.go
internal/rdb/rdb.go
internal/aof/aof.go
internal/server/server.go
examples/persistence/main.go
```

Recommended read order:

```text
SnapshotEntry
    ↓
Memory Snapshot/Restore
    ↓
RDB encoding
    ↓
atomic file publication
    ↓
logical state → command stream
    ↓
AOF rewrite buffering
    ↓
server persistence boundary / startup policy
```

## 12. Non-goals

- no Redis-compatible production RDB binary format;
- no `fork()` / copy-on-write background snapshot yet;
- no `SAVE`/`BGSAVE`/`BGREWRITEAOF` command surface;
- no multi-part AOF manifest;
- no automatic RDB fallback when AOF is enabled;
- no maxmemory/eviction;
- no transactions, replication, Sentinel or Cluster.

## 13. Next version

v0.6 moves from persistence pressure to memory pressure:

```text
logical keyspace
      ↓
approximate memory accounting
      ↓
maxmemory reached?
      ↓
expiration is not enough
      ↓
eviction policy
  ├── random
  ├── sampled LRU
  └── sampled LFU
```

Primary question: **what is the difference between expiration and eviction, and how can Redis approximate useful victim selection without maintaining a perfectly ordered global structure?**
