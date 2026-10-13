# mini-redis

An architecture-driven Redis learning implementation in Go. Each version replaces one hidden mechanism with an explicit, testable implementation.

## Current milestone: v0.9

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
v0.8  full sync + repl-id/offset + backlog + partial resync
  ↓
v0.9  SDOWN/ODOWN + failover epoch + promotion + topology repair  ← current
  ↓
v0.10 Cluster / hash slots
```

## Architecture

```text
clients
  ↓ RESP2
mini-redis data nodes
  ├── primary
  └── replicas
        ↑
        │ v0.8 replication
        │ SnapshotEntry base + canonical RESP mutation frames
        │
Sentinel-inspired control plane
  ├── monitor s1 ─┐
  ├── monitor s2 ─┼─ health observations
  └── monitor s3 ─┘
          ↓
      SDOWN votes
          ↓ quorum
        ODOWN
          ↓
    failover epoch
          ↓
   candidate ranking
          ↓
      promotion
          ↓
  topology reconfiguration
```

The central v0.9 lesson is that **data replication and failover coordination are different layers**. Sentinel never copies database state itself; it decides which v0.8 replication topology should be active.

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

## v0.9 failover mental model

### 1. Subjective failure: SDOWN

Every monitor owns its own health history:

```text
s1 ──X──> primary      → s1 says SDOWN
s2 ─────> primary      → healthy
s3 ─────> primary      → healthy
```

One observer timing out is **not** enough to promote a replica.

### 2. Objective failure: ODOWN

With three monitors and quorum `2`:

```text
s1: SDOWN
s2: SDOWN
s3: healthy
     ↓
2 / 3 votes
     ↓
ODOWN
```

Only then may failover start.

### 3. Failover epoch and deterministic leader

One failover attempt belongs to one monotonically increasing epoch. In this teaching implementation monitor IDs are sorted and the first SDOWN voter becomes the deterministic coordinator for that attempt.

```text
epoch 1
  leader = s1
```

This is deliberately simpler than production Redis Sentinel election and is **not** a distributed-consensus claim.

### 4. Candidate selection

A replica is eligible only when a quorum can reach it and observes it as a replica. Candidates are ranked by:

```text
reachable monitor votes  ↓ descending
replication offset       ↓ descending
stable node ID           ↑ ascending tie-break
```

So an unreachable replica cannot win merely because its last known offset was high.

### 5. Promotion

Promotion changes a running replica into a primary at a `Server.persistenceMu` boundary:

```text
old replica
   ↓ cancel upstream-follow role
fresh repl-id + fresh backlog
   ↓
new replication listener
   ↓
role = primary
   ↓
client writes allowed
```

The promoted node already owns the replicated data. Promotion does not introduce a second copy protocol.

### 6. Surviving replicas follow the promoted primary

```text
old primary P   X unreachable
replica R1      → promoted primary
replica R2      → FollowPrimary(R1)
```

Explicit topology changes intentionally reset remembered repl-id/offset, so following a different primary begins from a safe v0.8 `FULLRESYNC` boundary rather than accidentally reusing an offset from another replication identity.

### 7. Old-primary return

When the previous primary becomes observable again:

```text
old P returns as primary
        ↓
Sentinel sees chosen primary = R1
        ↓
P.FollowPrimary(R1)
        ↓
P becomes READONLY replica
        ↓
FULLRESYNC / later replication convergence
```

The teaching failover configuration requires dynamically demotable nodes to run with:

```text
appendonly = false
maxmemory  = 0
```

v0.9 explicitly rejects dynamic demotion otherwise, rather than leaving a stale local AOF or independent eviction policy after the node becomes a replica.

## Important limitation: no fencing

Sentinel-style observation cannot magically stop an unreachable old primary.

During a partition this can exist temporarily:

```text
clients A → old primary P      (still alive, monitors cannot reach it)
clients B → promoted primary R1
```

That is a split-brain risk. v0.9 does **not** implement fencing, leases, a consensus log, or production-grade Redis Sentinel protocol semantics. When the old primary becomes reachable again, the control plane reconfigures it as a replica of the chosen primary.

This limitation is intentional and important to understand.

## v0.8 replication remains the data plane

Full synchronization still uses:

```text
lock persistenceMu
  ↓
SnapshotEntry[] base
  ↓
capture repl-id / offset boundary
  ↓
attach live subscriber
  ↓
unlock
  ↓
base + ordered suffix
```

Reconnect still uses `PSYNC`, `CONTINUE`, frame-aware backlog and `FULLRESYNC` fallback. Successful EXEC mutations remain one replication frame.

## Transactions and Pub/Sub

`MULTI/EXEC` is ordered, non-interleaved execution rather than SQL rollback. WATCH is optimistic conflict detection. Pub/Sub remains ephemeral connection state and is unrelated to replication backlog or Sentinel health observations.

## Learning demos

```bash
make rehash-demo       # v0.3 Dict migration
make encodings-demo    # v0.4 adaptive encodings
make persistence-demo  # v0.5 RDB + AOF rewrite
make eviction-demo     # v0.6 sampled eviction
make session-demo      # v0.7 WATCH / MULTI / PubSub
make replication-demo  # v0.8 FULLRESYNC → live → CONTINUE → READONLY
make sentinel-demo     # v0.9 SDOWN → ODOWN → election → promotion → repair
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
make sentinel-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read v0.9 in this order

```text
internal/sentinel/sentinel.go
  ↓ monitor-local SDOWN / quorum ODOWN / epoch / ranking
internal/sentinel/server_node.go
  ↓ real TCP PING probe + topology adapter
internal/server/topology.go
  ↓ runtime PromoteToPrimary / FollowPrimary
internal/server/replication.go
  ↓ role-scoped replication transport
internal/sentinel/sentinel_test.go
internal/sentinel/integration_test.go
internal/server/topology_test.go
examples/sentinel/main.go
```

Compare historical branches `v0.1` through `v0.9` to see one dominant architecture change at a time.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical dependency-ordered roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
