# mini-redis

An architecture-driven Redis learning implementation in Go. Each version replaces one hidden mechanism with an explicit, testable implementation.

## Current milestone: v0.4

```text
v0.1  request → command → state → TTL → AOF
  ↓
v0.2  nonblocking sockets + epoll event loop
  ↓
v0.3  Redis-like Dict + bounded incremental rehash
  ↓
v0.4  List / Hash / Set / ZSet + adaptive encodings   ← current
  ↓
v0.5  RDB-like snapshot + AOF rewrite
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
  └── Object
       ├── Type
       ├── Encoding
       └── payload
  ↓
AOF logical command log
```

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

Strings/keys:

```text
PING ECHO
SET GET DEL EXISTS
INCR INCRBY
EXPIRE PEXPIREAT TTL
KEYS DBSIZE FLUSHDB
OBJECT ENCODING
```

Collections:

```text
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

Linux `auto` selects the epoll reactor. The v0.1-style reference transport remains available:

```bash
./bin/mini-redis-server -network-engine=legacy
```

## Observe an encoding transition

```text
127.0.0.1:6379> RPUSH jobs a b c d e f g h
(integer) 8
127.0.0.1:6379> OBJECT ENCODING jobs
"listpack"
127.0.0.1:6379> RPUSH jobs i
(integer) 9
127.0.0.1:6379> OBJECT ENCODING jobs
"quicklist"
```

For Set:

```text
SADD ids 1 2 3
OBJECT ENCODING ids       # intset
SADD ids 01
OBJECT ENCODING ids       # hashtable
```

For ZSet:

```text
ZADD rank 1 a 2 b 3 c 4 d 5 e 6 f 7 g 8 h 9 i
OBJECT ENCODING rank      # skiplist
ZRANK rank e              # 4
```

## Learning demos

```bash
make rehash-demo       # v0.3: one bounded Dict migration step at a time
make encodings-demo    # v0.4: compact→general object encodings
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
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read the code in this order

For v0.4:

```text
internal/store/store.go
  ↓
internal/store/collections.go
  ↓
internal/skiplist/skiplist.go
  ↓
internal/store/versionedmemory/memory.go
  ↓
internal/command/core.go
  ↓
internal/server/
```

Then compare historical branches `v0.1`, `v0.2`, `v0.3`, and `v0.4` to see one architecture change at a time.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical forward roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
