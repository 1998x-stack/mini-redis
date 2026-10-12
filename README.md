# mini-redis

A compact, architecture-driven Redis learning implementation in Go.

**Current version: v0.3.0 — Redis-like Dict + bounded incremental rehash + Object encoding**

The project does not try to clone every Redis command. Each version removes one important layer of “magic” while preserving the layers already understood.

```text
v0.1
TCP -> RESP -> Command Registry -> Store -> TTL -> AOF

v0.2
many sockets -> epoll -> one nonblocking event loop -> client buffers

v0.3
Store
  ├── data Dict[Object]
  │      ├── ht[0]
  │      ├── ht[1]
  │      └── rehashIndex
  └── expires Dict[time.Time]

Object = logical type + physical encoding + payload
```

## What v0.3 teaches

- what a hash table contains after `map` stops being a black box;
- how separate chaining resolves collisions;
- why load factor triggers growth/shrink policy;
- why Redis-like resizing can keep two hash tables alive at once;
- how lookup/update/delete remain correct while keys are split across `ht[0]` and `ht[1]`;
- how bounded rehash work avoids one large stop-the-world resize;
- how normal operations and event-loop timer work both advance rehash;
- why iteration during migration needs an explicit mutation policy;
- why Redis separates logical Object type from physical encoding;
- why persistence records logical state rather than transient bucket layout.

The v0.2 reactor and v0.1 RESP/command/TTL/AOF stack remain intact underneath the new memory engine.

## Core dictionary mental model

```text
stable:
ht[0] = current
ht[1] = empty
rehashIndex = -1

resize:
              source                         target
               ht[0]                          ht[1]
        ┌────────────────┐              ┌────────────────────┐
        │ bucket chains  │  incremental │ bucket chains      │
        │                │ ────────────► │                    │
        └────────────────┘              └────────────────────┘
               ▲
          rehashIndex
```

During migration: lookup searches both tables, update modifies the owner, new keys enter `ht[1]`, and delete searches both. One `RehashStep(1)` moves **at most one entry**, or advances over one empty bucket, so a long collision chain is not moved wholesale.

## Object type vs encoding

```text
SET a 42
OBJECT ENCODING a
"int"
GET a
"42"

SET b 001
OBJECT ENCODING b
"raw"
GET b
"001"
```

The logical value is stable even when physical representation differs.

## Supported commands

| Command | Example | Notes |
|---|---|---|
| PING | `PING` | optional one-message echo |
| ECHO | `ECHO hello` | bulk-string response |
| SET | `SET k v`, `SET k v EX 10`, `SET k v PX 500` | String only |
| GET | `GET k` | null bulk when missing |
| DEL | `DEL a b` | multi-key |
| EXISTS | `EXISTS a b` | counts existing arguments |
| INCR / INCRBY | `INCR n`, `INCRBY n 5` | int64 semantics, TTL preserved |
| EXPIRE / PEXPIREAT | `EXPIRE k 10` | relative/absolute expiry |
| TTL | `TTL k` | `-2` missing, `-1` persistent |
| KEYS | `KEYS user:*` | full scan; educational only |
| DBSIZE | `DBSIZE` | live keys |
| FLUSHDB | `FLUSHDB` | one logical DB |
| OBJECT | `OBJECT ENCODING k` | inspect `raw` / `int` |

## Quick start

Requirements: Go 1.23+.

```bash
make all
./bin/mini-redis-server
```

On Linux, `auto` selects the epoll reactor introduced in v0.2.

```bash
redis-cli -p 6379
127.0.0.1:6379> SET answer 42
OK
127.0.0.1:6379> OBJECT ENCODING answer
"int"
```

## Watch rehash happen

```bash
make rehash-demo
```

The demo deliberately hashes every key into the same bucket and prints stats after each `RehashStep(1)`:

```text
start: old=4 new=1
step1: old=3 new=2
step2: old=2 new=3
step3: old=1 new=4
step4: old=0 -> target becomes current
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
-rehash-work-per-cycle 64         # 0 disables timer-driven work
-idle-timeout 0s
-version false
```

Dictionary commands also make opportunistic rehash progress, so disabling timer work does not freeze a resize under active traffic.

## Repository map

```text
cmd/mini-redis-server/       process entrypoint
examples/rehash/             forced-collision demo
internal/netpoll/            Poller + Linux epoll
internal/resp/               RESP2 parsing/encoding
internal/server/             reactor, legacy baseline, timers/lifecycle
internal/command/            command specs/handlers
internal/dict/               buckets, chains, ht[0]/ht[1], incremental rehash
internal/store/              Store contract + Object type/encoding
internal/store/versionedmemory/
                             Dict-backed keyspace + expiry engine
internal/aof/                logical command log + replay
Agents.md                    engineering contract
Plan.md                      canonical architecture roadmap
version.md                   current version boundary
```

## Read v0.3 in this order

```text
1. internal/dict/dict.go
2. internal/dict/dict_test.go
3. internal/store/store.go
4. internal/store/versionedmemory/memory.go
5. internal/command/core.go
6. internal/server/server.go
7. internal/server/reactor_linux.go
```

Then compare snapshots:

```bash
git diff v0.2..v0.3 -- internal/dict internal/store
```

## Validation / exploration

```bash
go vet ./...
go test ./...
go test -race ./...
NETWORK_ENGINE=reactor make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
make rehash-demo
make benchmark-dict       # optional, machine-dependent
```

AOF still stores logical RESP commands, not physical Dict layout. Relative TTL is still rewritten to an absolute `PEXPIREAT` deadline, so restarts do not extend expiry.

## Version sequence

```text
v0.1  RESP / command / Store / TTL / AOF
  ↓
v0.2  epoll / reactor / partial I/O / pipeline / backpressure
  ↓
v0.3  Dict / collisions / incremental rehash / Object encoding   <- current
  ↓
v0.4  List / Hash / Set / ZSet / encoding conversion / skip list
  ↓
v0.5  RDB / AOF rewrite
  ↓
v0.6  maxmemory / eviction / approximate LRU/LFU
  ↓
v0.7+ transactions / PubSub / replication / Sentinel / Cluster
```

mini-redis remains educational: v0.3 intentionally does not copy Redis' exact hash function/resize heuristics, production iterator semantics, object header/refcount/LRU metadata, compact collection encodings, allocator behavior, RDB/AOF rewrite, eviction or distributed features.
