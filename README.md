# mini-redis

A small, readable Redis learning implementation in Go.

**Current version: v0.1.0**

The goal is not feature parity. The goal is to make Redis' most important ideas visible in a codebase small enough to understand end-to-end:

```text
TCP -> RESP -> Command Registry -> Store -> TTL
                         |
                         +-------> AOF
```

## What v0.1 teaches

- how `redis-cli` speaks RESP2
- why command dispatch should be table-driven
- how a key/value engine can expose stable interfaces for future data types
- lazy expiration vs active expiration
- why `INCR` needs atomic storage semantics
- how AOF converts state recovery into command replay
- the durability trade-off among `fsync always/everysec/no`
- where v0.1's goroutine networking differs from Redis' event-loop architecture

## Supported commands

| Command | Example | Notes |
|---|---|---|
| PING | `PING` | optional one-message echo |
| ECHO | `ECHO hello` | bulk-string response |
| SET | `SET k v`, `SET k v EX 10`, `SET k v PX 500` | strings only |
| GET | `GET k` | null bulk when missing |
| DEL | `DEL a b` | multi-key |
| EXISTS | `EXISTS a b` | counts each existing argument |
| INCR | `INCR n` | int64 integer strings |
| INCRBY | `INCRBY n 5` | preserves TTL |
| EXPIRE | `EXPIRE k 10` | seconds |
| PEXPIREAT | `PEXPIREAT k 1770000000000` | absolute unix ms; also used by AOF |
| TTL | `TTL k` | `-2` missing, `-1` no expiry |
| KEYS | `KEYS user:*` | full scan; learning only |
| DBSIZE | `DBSIZE` | live keys |
| FLUSHDB | `FLUSHDB` | one DB only |

## Quick start

Requirements: Go 1.23+.

```bash
make all
./bin/mini-redis-server
```

In another shell:

```bash
redis-cli -p 6379
127.0.0.1:6379> SET name redis
OK
127.0.0.1:6379> GET name
"redis"
127.0.0.1:6379> INCR visits
(integer) 1
127.0.0.1:6379> SET session abc EX 5
OK
127.0.0.1:6379> TTL session
(integer) 5
```

To avoid an existing Redis instance:

```bash
./bin/mini-redis-server -addr 127.0.0.1:16379 -appendfilename /tmp/mini-redis.aof
redis-cli -p 16379
```

## Server flags

```text
-addr 127.0.0.1:6379
-appendonly true
-appendfilename appendonly.aof
-appendfsync everysec      # always | everysec | no
-active-expire-every 100ms
-active-expire-sample 64
-idle-timeout 0s
-version false
```

Disable persistence for experiments:

```bash
./bin/mini-redis-server -appendonly=false
```

## Repository map

```text
cmd/mini-redis-server/     process entrypoint
internal/resp/             RESP2 parser + encoder
internal/command/          command specs, registry, handlers
internal/store/            stable storage contracts
internal/store/versionedmemory/
                           v0.1 memory engine
internal/aof/              append-only log and replay
internal/server/           TCP lifecycle, request loop, expiration cycle
internal/config/           runtime flags
Agents.md                  mandatory engineering contract
Plan.md                    canonical version/architecture roadmap
version.md                 current milestone and delta
docs/ARCHITECTURE.md       design reasoning
docs/ROADMAP.md            short roadmap navigator
scripts/smoke.sh           redis-cli compatibility smoke test
```

## Development model

Before changing architecture or version scope, read `Agents.md` and `Plan.md`. `main` is always the newest validated integrated state; `v0.x` branches are frozen learning snapshots.

## Read the code in this order

For fastest learning:

```text
1. internal/resp/reader.go
2. internal/resp/writer.go
3. internal/command/registry.go
4. internal/command/core.go
5. internal/store/store.go
6. internal/store/versionedmemory/memory.go
7. internal/aof/aof.go
8. internal/server/server.go
9. cmd/mini-redis-server/main.go
```

### Mental trace: SET name redis EX 10

```text
redis-cli
   |
   | *5 $3 SET $4 name $5 redis $2 EX $2 10
   v
RESP Reader
   |
   | ["SET","name","redis","EX","10"]
   v
Command Registry
   |
   | validate arity + find SET handler
   v
SET handler
   |
   | store.Set("name", String("redis"), 10s)
   v
Memory Store
   |---- data["name"] = "redis"
   `---- expires["name"] = now + 10s
   |
   v
AOF rewrite + append
   |---- SET name redis
   `---- PEXPIREAT name <absolute-ms>
   |
   v
+OK\r\n
```

## Why this is intentionally extensible

### New command

Add a `Spec` and handler; networking and persistence require no change.

### New data type

Add another `ObjectType`, extend or evolve the store interface at a version boundary, then implement commands. RESP and transport remain unchanged.

### New storage implementation

Implement `store.Store`. Command handlers do not know whether the implementation is one map, sharded maps, snapshot-aware memory, etc.

### v0.2 event loop

Replace the transport/execution part in `internal/server` while preserving command and storage semantics. This creates a clean before/after experiment for learning epoll.

## Tests

```bash
make test
make vet
make smoke  # uses redis-cli if installed
```

## Docker

```bash
docker build -t mini-redis:v0.1.0 .
docker run --rm -p 6379:6379 mini-redis:v0.1.0
```

The scratch image has an ephemeral filesystem unless you add a volume for the AOF file.

## Important differences from production Redis

This code is educational. v0.1 deliberately does **not** implement Redis' production-grade event loop, object encodings, memory allocator behavior, fork/COW snapshots, eviction, ACL, transactions, replication, Sentinel, Cluster, Lua, Streams, or full protocol/command compatibility.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`docs/ROADMAP.md`](docs/ROADMAP.md).
