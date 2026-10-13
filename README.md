# mini-redis

An architecture-driven Redis learning implementation in Go. Each version replaces one hidden mechanism with an explicit, testable implementation.

## Current milestone: v0.6

```text
v0.1  request → command → state → TTL → AOF
  ↓
v0.2  nonblocking sockets + epoll event loop
  ↓
v0.3  Redis-like Dict + bounded incremental rehash
  ↓
v0.4  List / Hash / Set / ZSet + adaptive encodings
  ↓
v0.5  logical snapshots + RDB-like state + AOF rewrite
  ↓
v0.6  maxmemory + expiration vs eviction + sampled LRU/LFU   ← current
  ↓
v0.7  transactions + WATCH + Pub/Sub
```

## Architecture

```text
client
  ↓ TCP/RESP2
reactor or legacy transport
  ↓
command.Registry
  ↓
Server persistence / memory-pressure boundary
  ↓
memorypolicy.Manager              runtime only
  ├── approximate logical bytes
  ├── access time
  ├── LFU heat
  └── sampled victim selection
  ↓
versionedmemory.Memory
  ├── keyspace Dict
  ├── expiry Dict
  └── Object{Type, Encoding, payload}
  ↓
SnapshotEntry[] → RDB / AOF rewrite
```

LRU/LFU metadata never enters `SnapshotEntry`, RDB or AOF.

## Memory-pressure configuration

```bash
./bin/mini-redis-server \
  -maxmemory 1048576 \
  -maxmemory-policy allkeys-lru \
  -maxmemory-samples 5
```

Policies:

```text
noeviction
allkeys-random
allkeys-lru
allkeys-lfu
```

`-maxmemory 0` disables memory-pressure enforcement. The memory figure is a deterministic educational estimate of logical key/value state, **not** Go heap or RSS.

LFU decay can be adjusted with `-lfu-decay`, and `-eviction-seed` makes sampling deterministic for learning/tests.

## Expiration is not eviction

```text
TTL deadline passes
    ↓
key is invalid by time
    ↓
expiration

memory estimate exceeds maxmemory
    ↓
policy selects a still-valid victim
    ↓
eviction
```

They can both delete keys, but they answer different questions and are counted separately.

## noeviction semantics

A memory-growing command that would leave the database above maxmemory is rolled back to the pre-command logical snapshot and is not appended to AOF. Removal-only commands remain usable above the limit so memory can be recovered.

This is an intentionally clear reference path, not an optimized production admission algorithm.

## Eviction persistence

When an eviction policy removes keys, the original successful mutation plus a logical `DEL` for the victims are appended to AOF in order. Restart reconstructs the same surviving keyspace, but access heat starts fresh.

## Persistence policy

AOF remains authoritative at startup when enabled. With `-appendonly=false`, the RDB-like `dump.mrdb` snapshot is used instead. TTL persists as absolute deadlines.

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

## Run

```bash
make all
./bin/mini-redis-server -addr 127.0.0.1:6379
redis-cli -p 6379
```

Linux `auto` selects the epoll reactor; use `-network-engine=legacy` for the v0.1-style comparison path.

## Learning demos

```bash
make rehash-demo       # v0.3 Dict migration
make encodings-demo    # v0.4 compact→general encodings
make persistence-demo  # v0.5 RDB + AOF rewrite
make eviction-demo     # v0.6 expiration/noeviction/sampled LRU/LFU
make benchmark         # v0.2 reactor vs legacy comparison
make benchmark-dict    # v0.3 custom Dict vs Go map microbenchmark
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
make eviction-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read v0.6 in this order

```text
internal/store/memory_policy.go
  ↓
internal/store/memorypolicy/manager.go
  ↓
internal/command/registry.go
  ↓
internal/server/server.go
  ↓
internal/config/config.go
  ↓
examples/eviction/main.go
```

Compare historical branches `v0.1` through `v0.6` to see one architecture change at a time.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical forward roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
