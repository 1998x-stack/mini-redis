# mini-redis

An architecture-driven Redis learning implementation in Go. Each version replaces one hidden mechanism with an explicit, testable implementation.

## Current milestone: v0.8

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
v0.7  WATCH + MULTI/EXEC + connection-mode Pub/Sub
  ↓
v0.8  full sync + repl-id/offset + backlog + partial resync   ← current
  ↓
v0.9  Sentinel-inspired health/failover
```

## Architecture

```text
client TCP/RESP2
      ↓
reactor or legacy transport
      ↓
clientSession
      ↓
server coordination / persistenceMu
      ├── command semantics
      ├── AOF / RDB
      └── canonical replication mutation feed
                    ↓
             replication.Primary
             ├── repl-id
             ├── byte offset
             ├── frame-aware backlog
             └── live subscribers
                    ↓
             replication TCP
                    ↓
                 replica
             snapshot + suffix
                    ↓
                same Store
```

The important v0.8 boundary is **logical mutation order**. Replication does not serialize private Dict/Object memory layout; it transfers a logical `SnapshotEntry[]` base and canonical RESP reconstruction frames after that base.

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

## Run standalone

```bash
make all
./bin/mini-redis-server -addr 127.0.0.1:6379
redis-cli -p 6379
```

Linux `auto` selects the epoll reactor. The v0.1-style reference transport remains available with `-network-engine=legacy`.

## Run a primary and replica

Primary:

```bash
./bin/mini-redis-server \
  -addr 127.0.0.1:6379 \
  -replication-role=primary \
  -replication-addr 127.0.0.1:6380
```

Replica in the intentionally simple v0.8 teaching mode:

```bash
./bin/mini-redis-server \
  -addr 127.0.0.1:6381 \
  -appendonly=false \
  -dbfilename '' \
  -maxmemory 0 \
  -replication-role=replica \
  -replicaof 127.0.0.1:6380
```

Replica client writes are rejected with `READONLY`. v0.8 deliberately keeps replica persistence and independent eviction disabled so the primary remains the only mutation-policy authority.

## Replication mental model

### 1. Full synchronization

```text
primary lock persistenceMu
        ↓
SnapshotEntry[] base snapshot
        ↓
capture replication offset
        ↓
attach live subscriber at same boundary
        ↓
unlock
        ↓
transfer snapshot
        ↓
drain post-snapshot frames
        ↓
live follow
```

The subscriber is attached before the mutation-ordering lock is released, so a write cannot fall into a gap between “snapshot taken” and “replication started”.

### 2. Canonical byte offset

Every accepted logical mutation becomes canonical RESP reconstruction bytes. The primary increments a byte offset over exactly that stream.

```text
offset 0
  ↓ SET a 1 frame
42
  ↓ INCR a frame
65
  ↓ MULTI ... EXEC frame
143
```

Relative expiration is already rewritten to absolute `PEXPIREAT`; eviction-generated `DEL` is also part of the mutation stream.

### 3. Frame-aware backlog

The backlog is bounded by bytes but evicts **whole frames**:

```text
frame A      frame B      frame C
1..40        41..90       91..140
```

A replica at offset `90` can resume with frame C. If old history has wrapped out, correctness does not change—the primary simply falls back to full sync.

Transaction frames stay intact:

```text
MULTI
SET a 1
INCR a
EXEC
```

They are one replication frame, so catch-up cannot cut a transaction in half.

### 4. Reconnect / partial resync

Replica reconnects with:

```text
PSYNC <repl-id> <last-applied-offset>
```

Primary answers either:

```text
FULLRESYNC <new/current-id> <base-offset>
```

or, if the identity matches and the missing suffix is still retained:

```text
CONTINUE <id> <current-offset>
```

The replica advances its offset only after a complete frame applies successfully.

## Transactions

`MULTI/EXEC` remains different from SQL rollback. Queue-time command/arity errors cause `EXECABORT`; execution-time errors appear as individual EXEC elements without undoing successful siblings. WATCH is optimistic conflict detection using key versions/existence, not a lock.

Successful transaction mutations are AOF-framed and replication-framed with `MULTI ... EXEC` so neither crash recovery nor partial resync intentionally observes a prefix-only transaction.

## Pub/Sub

Subscriptions belong to the connection, not the keyspace. `SUBSCRIBE`/`PUBLISH` messages are ephemeral and unrelated to the v0.8 replication backlog. Disconnect removes channel membership; Pub/Sub is not durable message history.

## Data types and encodings

| Type | Small encoding | General encoding |
| --- | --- | --- |
| String | `int` | `raw` |
| List | `listpack` | `quicklist` |
| Hash | `listpack` | `hashtable` |
| Set | `intset` | `hashtable` |
| ZSet | `listpack` | `skiplist` + score Dict |

The compact→general threshold is intentionally **8 entries** for teaching. `OBJECT ENCODING <key>` exposes transitions.

## Persistence / memory policy

AOF remains authoritative at startup when enabled. Logical snapshots use `SnapshotEntry`; AOF rewrite and RDB never serialize private in-memory encodings. Relative TTL persists/replicates as an absolute deadline.

Maxmemory policies remain:

```text
noeviction
allkeys-random
allkeys-lru
allkeys-lfu
```

Memory accounting is deterministic educational logical-byte accounting, not Go heap/RSS accounting.

## Learning demos

```bash
make rehash-demo       # v0.3 Dict migration
make encodings-demo    # v0.4 adaptive encodings
make persistence-demo  # v0.5 RDB + AOF rewrite
make eviction-demo     # v0.6 sampled eviction
make session-demo      # v0.7 WATCH / MULTI / PubSub
make replication-demo  # v0.8 FULLRESYNC → live → CONTINUE → READONLY
make benchmark         # reactor vs legacy transport
make benchmark-dict    # educational Dict vs Go map
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
make replication-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read v0.8 in this order

```text
internal/replication/backlog.go
  ↓ frame boundaries / retained suffix
internal/replication/primary.go
  ↓ repl-id / offset / live subscribers
internal/server/replication.go
  ↓ FULLRESYNC / CONTINUE / replica apply
internal/server/server.go
  ↓ ordinary mutation → replication feed
internal/server/session.go
  ↓ EXEC → one transaction replication frame
internal/server/replication_test.go
examples/replication/main.go
```

Compare historical branches `v0.1` through `v0.8` to see one dominant architecture change at a time.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical dependency-ordered roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
