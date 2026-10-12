# mini-redis

A compact, architecture-driven Redis learning implementation in Go.

**Current version: v0.2.0 — nonblocking I/O + epoll event loop**

The project is not trying to clone every Redis command. Each version removes one important layer of “magic” while preserving the layers already understood.

```text
v0.1
TCP -> RESP -> Command Registry -> Store -> TTL -> AOF

v0.2
                    epoll
                      │
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
    client A       client B       client C
    in / out       in / out       in / out
    buffers        buffers        buffers
       └──────────────┼──────────────┘
                      ▼
                  Event Loop
            read → parse → execute
                 → queue reply
                 → partial write
                      │
                      ▼
              same command/store/AOF
```

## What v0.2 teaches

- why socket readiness is different from blocking I/O;
- how epoll separates “which fd is ready?” from “what should the server do?”;
- why TCP requires per-client input buffers;
- how a partial RESP frame can wait without blocking other clients;
- how Redis-style pipelining can parse multiple commands from one read;
- why nonblocking writes require an output queue;
- why `EPOLLOUT` should only be watched when output is pending;
- how slow readers create server-side memory pressure;
- how output-buffer limits implement simple backpressure;
- how time events such as expiration fit into the same event loop;
- how v0.1 goroutine-per-client differs from a single reactor.

The v0.1 request/state/persistence path remains intact underneath the new networking layer.

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
| PEXPIREAT | `PEXPIREAT k 1770000000000` | absolute unix ms; used by AOF |
| TTL | `TTL k` | `-2` missing, `-1` persistent |
| KEYS | `KEYS user:*` | full scan; educational only |
| DBSIZE | `DBSIZE` | live keys |
| FLUSHDB | `FLUSHDB` | one logical DB |

## Quick start

Requirements: Go 1.23+.

```bash
make all
./bin/mini-redis-server
```

On Linux, the default `auto` network engine chooses the v0.2 reactor.

```bash
./bin/mini-redis-server \
  -network-engine=reactor \
  -addr 127.0.0.1:6379
```

Then:

```bash
redis-cli -p 6379
127.0.0.1:6379> SET name redis
OK
127.0.0.1:6379> GET name
"redis"
127.0.0.1:6379> INCR visits
(integer) 1
```

### Compare v0.1 and v0.2 networking directly

The old mechanism is intentionally retained as a reference engine:

```bash
./bin/mini-redis-server -network-engine=legacy
./bin/mini-redis-server -network-engine=reactor
```

Both use the same RESP, command, Store, TTL and AOF layers. The independent variable is the networking/execution model.

## Pipelining

v0.2 can receive multiple RESP commands in one TCP read and queues responses in command order.

Conceptually:

```text
SET n 1 | INCR n | GET n
    ↓        ↓        ↓
  +OK       :2      $1 2
    └────────┴────────┘
       one output queue
```

## Server flags

```text
-addr 127.0.0.1:6379
-network-engine auto              # auto | reactor | legacy
-client-input-buffer-limit 67108864
-client-output-buffer-limit 8388608
-appendonly true
-appendfilename appendonly.aof
-appendfsync everysec             # always | everysec | no
-active-expire-every 100ms
-active-expire-sample 64
-idle-timeout 0s
-version false
```

On non-Linux systems `auto` selects the portable legacy engine. The reactor implementation is deliberately Linux/epoll-specific in v0.2. Docker is an easy way to run the reactor from macOS or Windows.

## Repository map

```text
cmd/mini-redis-server/       process entrypoint
internal/netpoll/            Poller contract + Linux epoll implementation
internal/resp/               blocking + incremental RESP2 parsing/encoding
internal/server/             reactor, legacy baseline, lifecycle
internal/command/            command specs, registry, handlers
internal/store/              stable storage contract
internal/store/versionedmemory/
                             current in-memory engine
internal/aof/                append-only log and replay
internal/config/             runtime policy/flags
Agents.md                    mandatory engineering contract
Plan.md                      canonical architecture roadmap
version.md                   current version delta/learning boundary
docs/ARCHITECTURE.md         v0.2 mechanism explanation
scripts/smoke.sh             end-to-end RESP smoke test
scripts/compare-engines.sh   legacy vs reactor benchmark harness
```

## Read the code in this order

For the v0.2 learning path:

```text
1. internal/netpoll/poller.go
2. internal/netpoll/poller_linux.go
3. internal/resp/incremental.go
4. internal/server/reactor_linux.go
5. internal/server/server.go
6. internal/server/legacy.go
7. internal/config/config.go
```

Then compare `internal/server` against the frozen `v0.1` branch.

For the underlying v0.1 Redis semantics, read:

```text
resp -> command -> store -> aof
```

## Tests

```bash
make test
make vet
make smoke
```

Concurrency validation:

```bash
go test -race ./...
```

Important reactor tests prove:

- partial-frame client isolation;
- pipeline reply ordering;
- output-buffer backpressure;
- protocol-error close behavior;
- epoll registration/readiness lifecycle.

## Benchmark both networking models

```bash
make benchmark
```

The harness compares the two engines from the same build so command/storage behavior is held constant. Numbers depend heavily on OS/kernel/CPU and should be interpreted as a learning experiment, not a Redis performance claim.

## Docker

```bash
docker build -t mini-redis:v0.2.0 .
docker run --rm -p 6379:6379 mini-redis:v0.2.0
```

Docker runs Linux, so `auto` selects the reactor.

## Development model

Before changing architecture or version scope, read `Agents.md` and `Plan.md`.

```text
main = newest validated integrated state
v0.x = frozen educational snapshots
```

The next planned version is **v0.3: custom Redis-like Dict + incremental rehash + object encoding metadata**.

## Important differences from production Redis

v0.2 is educational. It does not claim production Redis compatibility or performance. Notable simplifications include:

- Linux epoll only for the reactor;
- level-triggered readiness;
- simple fixed client buffer limits;
- Go map storage;
- mutex-protected Store retained from v0.1;
- synchronous AOF append on the command path;
- strings only;
- no RDB, eviction, transactions, replication, Sentinel or Cluster.

See [`version.md`](version.md), [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`Plan.md`](Plan.md).
