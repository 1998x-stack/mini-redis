# mini-redis

An architecture-driven Redis learning implementation in Go. Each version makes one hidden Redis mechanism explicit, small, inspectable and testable.

## Current milestone: v0.13

```text
v0.1  request → command → state → TTL → AOF
  ↓
v0.2  epoll event loop
  ↓
v0.3  Redis-like Dict + incremental rehash
  ↓
v0.4  List / Hash / Set / ZSet + adaptive encodings
  ↓
v0.5  RDB-like snapshots + AOF rewrite
  ↓
v0.6  maxmemory + sampled LRU/LFU
  ↓
v0.7  WATCH + MULTI/EXEC + Pub/Sub
  ↓
v0.8  replication / PSYNC / backlog
  ↓
v0.9  Sentinel-inspired failover
  ↓
v0.10 Cluster / 16384 slots / MOVED / ASK
  ↓
v0.11 HELLO 2/3 + richer RESP3 values + Push
  ↓
v0.12 observability / profiling / performance evidence
  ↓
v0.13 replica ACK / applied offset / authoritative byte lag  ← current
```

v0.12 ended the first architecture-learning arc by making the system measurable. It also exposed one thing the primary could **not** truthfully measure: remote replica applied progress.

v0.13 starts a second evidence-driven arc by adding the missing protocol mechanism.

## v0.13 mental model

```text
                data direction
primary ─────────────────────────────► replica
 offset=2000                            applied=2000
    │                                      │
    ◄──── REPLCONF ACK replica-1 2000 ─────┘

primary appends another 120 bytes

primary offset = 2120
last ACK       = 2000
lag_bytes      = 120

replica applies complete frame
    ↓
ACK 2120
    ↓
lag_bytes = 0
```

The central lesson is:

> **The sender knowing what it sent is not the same as knowing what the receiver applied.**

## Replication handshake

The replica still starts with:

```text
PSYNC <repl-id|?> <offset|-1>
```

The primary now includes one **connection-local replica ID** in the response:

```text
FULLRESYNC <repl-id> <base-offset> <replica-id>
CONTINUE   <repl-id> <current-offset> <replica-id>
```

Example:

```text
FULLRESYNC 8bf2... 1400 replica-3
```

That replica ID belongs only to the current TCP connection.

```text
connection A → replica-3
close
connection B → replica-4
```

CONTINUE continuity is still based on the primary repl-id and canonical offset, not the connection-local ID.

## Replica ACK

After successful application through offset `N`, the replica sends:

```text
REPLCONF ACK <replica-id> <N>
```

The primary accepts ACK progress only when:

```text
0 <= previous_ack <= new_ack <= primary_offset
```

It rejects:

- the wrong connection replica ID;
- negative offsets;
- regressing offsets;
- offsets ahead of the primary;
- malformed ACK frames.

Equal ACKs are allowed to refresh ACK age without inventing progress.

## Apply before ACK

A real mini-redis replica does not ACK bytes merely read from the network.

### FULLRESYNC

```text
read snapshot
  ↓
decode SnapshotEntry[]
  ↓
RestoreSnapshot succeeds
  ↓
local replica offset = base offset
  ↓
ACK base offset
```

### Live/backlog frame

```text
read complete frame
  ↓
decode commands
  ↓
apply complete frame
  ↓
advance local offset
  ↓
ACK new offset
```

This makes ACK represent **memory-applied progress**.

It does not represent fsync/disk durability; v0.13 teaching replicas remain memory-only.

## ACK is not part of the replication stream

There are now two directions, with different meanings:

```text
primary → replica
canonical mutation frames
counted by primary replication offset
retained in backlog

replica → primary
REPLCONF ACK control frames
not retained in backlog
not counted in canonical offset
```

Therefore ACK traffic cannot alter PSYNC boundaries or make the primary replication offset move.

## Authoritative lag

For an acknowledged live replica:

```text
lag_bytes = max(primary_offset - acknowledged_offset, 0)
```

`INFO replication` / `MetricsSnapshot` expose:

```text
replica_id
remote_addr
sync_mode
acknowledged
acknowledged_offset
lag_bytes
ack_age_ms
```

Before the first ACK, unknown progress is explicit:

```text
acknowledged_offset = -1
lag_bytes           = -1
ack_age_ms          = -1
```

The word “authoritative” means authoritative **according to receiver-reported protocol progress**. The primary cannot independently prove that a deliberately dishonest peer actually applied data before ACKing.

## ACK age is not liveness

`ack_age_ms` means:

```text
now - time of last accepted applied-progress ACK
```

It is not a heartbeat timer.

An idle replica may legitimately show:

```text
lag_bytes = 0
ack_age_ms = 300000
```

because nothing new needed to be applied for five minutes.

For that reason, v0.13 does **not** feed ACK age or byte lag into Sentinel SDOWN/ODOWN or candidate selection.

## One-lock primary replication snapshot

`replication.Primary.mu` owns together:

```text
primary repl-id
backlog end/start
live subscribers
connection-local replica IDs
accepted ACK offsets/timestamps
```

`PrimarySnapshot(now)` copies all of those values under one lock.

That allows one `INFO replication` snapshot to use a coherent primary offset and coherent lag values even while writes are happening concurrently.

## INFO

v0.12's read-only INFO surface remains:

```text
INFO
INFO server
INFO clients
INFO commands
INFO protocol
INFO persistence
INFO replication
INFO memory
INFO reactor
```

The replication section now adds live ACK progress plus:

```text
acks_received
acks_rejected
```

Snapshots expose copies/atomic values only. Internal subscriber channels and mutable maps are never returned.

## Deterministic ACK demo

Run:

```bash
make replication-ack-demo
```

It exercises real TCP connections through this sequence:

```text
FULLRESYNC
  ↓
initial ACK
  ↓
lag = 0
  ↓
primary SET while manual replica withholds ACK
  ↓
lag > 0
  ↓
replica reads frame + ACKs applied boundary
  ↓
lag = 0
  ↓
disconnect
  ↓
live progress removed
  ↓
PSYNC old repl-id + retained offset
  ↓
CONTINUE with fresh connection replica-id
  ↓
ACK resume boundary
  ↓
lag = 0
```

The manual peer is useful specifically because it can intentionally delay ACK to make lag observable. Real replica integration tests separately prove the normal apply-before-ACK implementation.

## Existing architecture remains intact

v0.13 does not replace earlier layers:

```text
v0.1   request / Store / TTL / AOF
v0.2   reactor / epoll / buffers / backpressure
v0.3   Dict / incremental rehash
v0.4   Object / adaptive encodings
v0.5   RDB / AOF rewrite
v0.6   memory pressure / sampled eviction
v0.7   transactions / WATCH / PubSub
v0.8   canonical replication / PSYNC / backlog
v0.9   Sentinel-style failover
v0.10  Cluster routing / migration
v0.11  RESP3 representation
v0.12  observability / profiling
v0.13  receiver ACK / authoritative byte lag
```

Sentinel, Cluster, client RESP2/RESP3, AOF/RDB and Store representations keep their prior semantics.

## Supported command subset

```text
PING ECHO
SET GET DEL EXISTS
INCR INCRBY
EXPIRE PEXPIREAT TTL
KEYS DBSIZE FLUSHDB
OBJECT ENCODING
INFO

LPUSH RPUSH LPOP RPOP LLEN LRANGE
HSET HGET HDEL HLEN HGETALL
SADD SREM SISMEMBER SMEMBERS SCARD
ZADD ZSCORE ZRANK ZRANGE ZREM

WATCH UNWATCH MULTI EXEC DISCARD
SUBSCRIBE UNSUBSCRIBE PUBLISH
HELLO [2|3]
```

The Cluster teaching facade additionally exposes `ASKING` and `CLUSTER KEYSLOT`.

`PSYNC` and `REPLCONF ACK` belong to the dedicated replication plane, not the normal client command registry.

## Build and run

```bash
go build ./cmd/mini-redis-server

go run ./cmd/mini-redis-server \
  -addr 127.0.0.1:6379
```

Useful teaching configuration includes:

```text
-network-engine auto|reactor|legacy
-appendonly
-appendfilename
-appendfsync always|everysec|no
-dbfilename
-maxmemory
-maxmemory-policy
-replication-role standalone|primary|replica
-replication-addr
-replicaof
-repl-backlog-bytes
```

## Learning demos

```bash
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make session-demo
make replication-demo
make replication-ack-demo   # v0.13 applied ACK + lag lifecycle
make sentinel-demo
make cluster-demo
make resp3-demo
make observability-demo
```

## Performance / profiling

v0.12 measurement tools remain available:

```bash
make benchmark
make benchmark-dict
make benchmark-observability
```

See `docs/PERFORMANCE.md` before making performance claims. Workload, engine, persistence policy, concurrency, payload and commit SHA must be stated.

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
make replication-ack-demo
make sentinel-demo
make cluster-demo
make resp3-demo
make observability-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read v0.13 in this order

```text
version.md
  ↓
internal/replication/primary.go
  ↓ subscriber / ACK ownership + PrimarySnapshot
internal/server/replication.go
  ↓ full-duplex ACK wire protocol
internal/server/metrics.go
  ↓ lag / INFO representation
internal/replication/progress_test.go
internal/server/replication_ack_test.go
  ↓ invariants + real integration
examples/replication-ack/main.go
```

For the full architectural history, compare branches `v0.1` through `v0.13`.

## Important limits

v0.13 intentionally does not implement:

- Redis WAIT / WAITAOF;
- client writes waiting for replica quorum;
- periodic idle ACK heartbeat;
- replica fsync/disk acknowledgement;
- persistent replica node IDs;
- ACK-driven Sentinel policy;
- production Redis replication compatibility.

Those are separate systems questions rather than implicit promises of this version.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — dependency/evidence-driven roadmap.
- `version.md` — exact current learning boundary.
- `docs/ARCHITECTURE.md` — current mechanism reasoning.
- `docs/PERFORMANCE.md` — measurement discipline.
