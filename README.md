# mini-redis

An architecture-driven Redis learning implementation in Go. Each version replaces one hidden mechanism with an explicit, testable implementation.

## Current milestone: v0.5

```text
v0.1  request → command → state → TTL → AOF
  ↓
v0.2  nonblocking sockets + epoll event loop
  ↓
v0.3  Redis-like Dict + bounded incremental rehash
  ↓
v0.4  List / Hash / Set / ZSet + adaptive encodings
  ↓
v0.5  logical snapshots + RDB-like state + AOF rewrite   ← current
  ↓
v0.6  maxmemory + sampled eviction policies
```

## Architecture

```text
client
  ↓ TCP/RESP2
reactor (Linux epoll) or legacy transport
  ↓
command.Registry
  ↓
store.Store
  ↓
Memory
  ├── keyspace incremental Dict
  ├── expiry incremental Dict
  └── Object{Type, Encoding, payload}
             ↓
         logical state
             ↓
       SnapshotEntry[]
         ┌───┴────┐
         ▼        ▼
       RDB       AOF rewrite
```

Persistence never serializes quicklist nodes, Dict buckets, intset internals or SkipList levels.

## Data types and encodings

| Type | Small encoding | General encoding |
| --- | --- | --- |
| String | `int` | `raw` |
| List | `listpack` | `quicklist` |
| Hash | `listpack` | `hashtable` |
| Set | `intset` | `hashtable` |
| ZSet | `listpack` | `skiplist` + score Dict |

The compact→general threshold is intentionally **8 entries** for teaching. Use `OBJECT ENCODING <key>` to observe transitions.

## Supported command subset

```text
PING ECHO
SET GET DEL EXISTS
INCR INCRBY
EXPIRE PEXPIREAT TTL
KEYS DBSIZE FLUSHDB
OBJECT ENCODING

LPUSH RPUSH LPOP RPOP LLEN LRANGE
HSET HGET HDEL HLEN HGETALL
SADD SREM SISMEMBER SMEMBERS SCARD
ZADD ZSCORE ZRANK ZRANGE ZREM
```

This is an educational compatibility subset, not a production Redis replacement.

## Run

```bash
make all
./bin/mini-redis-server -addr 127.0.0.1:6379
redis-cli -p 6379
```

Linux `auto` selects the epoll reactor. The v0.1-style reference transport remains available with `-network-engine=legacy`.

### Startup persistence policy

Default configuration enables AOF:

```bash
./bin/mini-redis-server \
  -appendonly=true \
  -appendfilename appendonly.aof \
  -appendfsync everysec
```

When AOF is enabled, it is the authoritative startup source and an RDB file is ignored.

To start from the RDB-like snapshot path instead:

```bash
./bin/mini-redis-server \
  -appendonly=false \
  -dbfilename dump.mrdb
```

A missing RDB means an empty database; corrupt or unsupported RDB input fails startup rather than silently discarding state.

`Server.SaveRDB()` and `Server.RewriteAOF()` expose the v0.5 reference mechanisms in code. They are intentionally **not** yet exposed as Redis `SAVE/BGSAVE/BGREWRITEAOF` commands; use the persistence demo to study them.

## Persistence model

### RDB-like snapshot

```text
SnapshotEntry[]
     ↓
MRDB | version | payload length | logical payload | CRC32
     ↓
temp file → fsync(file) → atomic rename → fsync(directory)
```

### AOF rewrite

```text
old history
SET x 1
SET x 2
SET x 3
     ↓ logical snapshot
compact reconstruction
SET x 3
     +
mutations that happened after the snapshot boundary
```

Expiry is persisted as an absolute `PEXPIREAT` deadline so process downtime does not restart the TTL duration.

## Learning demos

```bash
make rehash-demo       # v0.3: one bounded Dict migration step at a time
make encodings-demo    # v0.4: compact→general object encodings
make persistence-demo  # v0.5: RDB round-trip + AOF rewrite/buffering
make benchmark         # v0.2: reactor vs legacy networking comparison
make benchmark-dict    # v0.3: educational Dict vs Go map microbenchmark
```

## Validation

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

## Read the code in this order

For v0.5:

```text
internal/store/store.go                       # SnapshotEntry boundary
  ↓
internal/store/versionedmemory/snapshot.go    # logical extraction / restore
  ↓
internal/rdb/rdb.go                           # state snapshot format
  ↓
internal/persistence/atomicfile.go            # crash-safe publication
  ↓
internal/persistence/commands.go              # state → AOF commands
  ↓
internal/aof/aof.go                           # rewrite buffer / fd swap
  ↓
internal/server/server.go                     # startup + coordination boundary
```

Compare historical branches `v0.1` through `v0.5` to see one architecture change at a time.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical forward roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
