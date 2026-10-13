# mini-redis

An architecture-driven Redis learning implementation in Go. Each version replaces one hidden mechanism with an explicit, testable implementation.

## Current milestone: v0.7

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
v0.6  maxmemory + sampled LRU/LFU eviction
  ↓
v0.7  WATCH + MULTI/EXEC + connection-mode Pub/Sub   ← current
  ↓
v0.8  replication full sync + backlog + partial resync
```

## Architecture

```text
client connection
  ↓ TCP/RESP2
reactor (Linux epoll) or legacy transport
  ↓
clientSession
  ├── WATCH tokens
  ├── MULTI queue
  └── subscriptions
  ↓
server coordination
  ├── persistenceMu
  ├── key versions
  └── Pub/Sub hub
  ↓
command.Registry
  ↓
memorypolicy.Manager (optional maxmemory)
  ↓
versionedmemory.Memory
  ├── keyspace incremental Dict
  ├── expiry incremental Dict
  └── Object{Type, Encoding, payload}
             ↓
         SnapshotEntry[]
         ┌──────┴──────┐
         ▼             ▼
        RDB           AOF
```

Session/WATCH/LRU/LFU metadata is runtime state. Persistence stores logical database state and mutation history, never these runtime control structures.

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

WATCH UNWATCH MULTI EXEC DISCARD
SUBSCRIBE UNSUBSCRIBE PUBLISH
```

This is an educational compatibility subset, not a production Redis replacement.

## Run

```bash
make all
./bin/mini-redis-server -addr 127.0.0.1:6379
redis-cli -p 6379
```

Linux `auto` selects the epoll reactor. The v0.1-style reference transport remains available with `-network-engine=legacy`.

## Transactions: the key mental model

```text
WATCH balance
MULTI
GET balance       # QUEUED
INCR balance      # QUEUED
EXEC              # ordered result array, or null array on WATCH conflict
```

`MULTI/EXEC` is **not** SQL rollback. A runtime error in one queued command does not undo successful commands that already executed. A command/arity error detected while queueing dirties the transaction and causes `EXECABORT`.

WATCH is optimistic: another successful write to the watched key invalidates EXEC even if that client later restores the original value. TTL disappearance also invalidates WATCH.

## Transaction persistence

Successful transaction mutations are appended to AOF inside an explicit frame:

```text
MULTI
SET a 1
INCR b
DEL evicted-key
EXEC
```

Replay buffers the frame and applies it only when the closing EXEC is present. A crash-truncated final transaction is discarded rather than replayed partially.

RDB snapshot capture and expiration maintenance share the EXEC coordination boundary, so they do not intentionally observe/interleave a half-transaction state.

## Pub/Sub

```text
# client A
SUBSCRIBE news

# client B
PUBLISH news hello
```

The subscriber receives a live RESP2 push-style array:

```text
["message", "news", "hello"]
```

Subscriptions belong to the connection, not the keyspace. They disappear on disconnect and are not persisted. In subscribed mode this teaching subset permits `SUBSCRIBE`, `UNSUBSCRIBE` and `PING` until subscription count returns to zero.

## Data types and encodings

| Type | Small encoding | General encoding |
| --- | --- | --- |
| String | `int` | `raw` |
| List | `listpack` | `quicklist` |
| Hash | `listpack` | `hashtable` |
| Set | `intset` | `hashtable` |
| ZSet | `listpack` | `skiplist` + score Dict |

The compact→general threshold is intentionally **8 entries** for teaching. Use `OBJECT ENCODING <key>` to observe transitions.

## Persistence startup policy

AOF is enabled by default and is authoritative at startup. Use `-appendonly=false -dbfilename dump.mrdb` to load the RDB-like snapshot path instead. Corrupt/incompatible RDB input fails startup rather than silently becoming an empty database.

Relative TTL crosses persistence as an absolute `PEXPIREAT` deadline, so downtime does not restart its duration.

## Maxmemory policies

```text
noeviction
allkeys-random
allkeys-lru
allkeys-lfu
```

Example:

```bash
./bin/mini-redis-server \
  -maxmemory 1048576 \
  -maxmemory-policy allkeys-lru \
  -maxmemory-samples 5
```

Memory accounting is deterministic educational logical-byte accounting, not a claim about Go heap/RSS.

## Learning demos

```bash
make rehash-demo       # v0.3: bounded Dict migration
make encodings-demo    # v0.4: compact→general encodings
make persistence-demo  # v0.5: RDB + AOF rewrite
make eviction-demo     # v0.6: expiration vs sampled eviction
make session-demo      # v0.7: WATCH, MULTI/EXEC and Pub/Sub
make benchmark         # v0.2: reactor vs legacy transport
make benchmark-dict    # v0.3: educational Dict vs Go map
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
make session-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read v0.7 in this order

```text
internal/server/session.go                    # per-client state + WATCH + Pub/Sub hub
  ↓
internal/server/server.go                     # EXEC/persistence coordination
  ↓
internal/command/registry.go                  # queue-time validation
  ↓
internal/aof/aof.go                           # transaction framing/replay
  ↓
internal/server/legacy.go                     # blocking connection session integration
internal/server/reactor_linux.go              # epoll client/session push integration
  ↓
internal/server/session_test.go
internal/server/network_session_test.go
internal/server/transaction_persistence_test.go
```

Compare historical branches `v0.1` through `v0.7` to see one architecture change at a time.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical forward roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
