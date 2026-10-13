# mini-redis

An architecture-driven Redis learning implementation in Go. Each version replaces one hidden mechanism with an explicit, testable implementation.

## Current milestone: v0.10

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
v0.9  Sentinel-inspired SDOWN/ODOWN/failover
  ↓
v0.10 16384 hash slots + MOVED/ASK + migration + bounded gossip  ← current
```

## v0.10 mental model

```text
key
 ↓ CRC16/XMODEM
hash-tag {...} when non-empty
 ↓
slot ∈ [0, 16383]
 ↓
slot ownership table
 ├─ local stable owner  → execute
 ├─ remote stable owner → MOVED <slot> <addr>
 └─ migrating slot
       ├─ source still has key → execute on source
       ├─ source missing key   → ASK <slot> <target>
       └─ importing target + ASKING → execute once
```

The central lesson is that **key/value state and slot/topology state are different layers**. Slot ownership never lives inside Store objects or persistence payloads.

## Hash tags

Redis-style non-empty hash tags force related keys into the same slot:

```text
user:{42}:name
order:{42}:1
```

Both hash only `42`, so both map to slot `8000` in this implementation.

`CLUSTER KEYSLOT <key>` is supported by the cluster-aware facade.

## Stable routing: MOVED

If node `a` owns a slot and a client asks node `b`:

```text
client → b
          ↓
-MOVED <slot> a-address
```

`MOVED` means the stable ownership table says another node owns the slot.

## Migration routing: ASK / ASKING

During an explicit migration `source → target`, stable ownership remains the source until commit.

```text
source owns slot
migrationTarget = target
```

Existing source keys continue to execute on the source. If the source no longer has a key in that slot, it returns:

```text
-ASK <slot> <target-address>
```

The client connects to the target and sends:

```text
ASKING
<one routed command>
```

`ASKING` is a one-request connection grant. The following routed command may execute on the importing target; the grant is then consumed. Without ASKING, the target still redirects to the stable owner until migration commit.

## Multi-key safety

Commands such as `DEL` and `EXISTS` may reference multiple keys, but all routed keys must hash to one slot.

```text
DEL a:{same} b:{same}   → allowed
DEL foo bar             → CROSSSLOT
```

The cluster facade intentionally does not support MULTI/EXEC in v0.10. Transparent cross-command transaction routing is a non-goal rather than a partially correct promise.

## Slot migration

`cluster.MigrateSlot` exposes the migration boundary explicitly:

```text
BeginMigration(slot, source, target)
        ↓
source.KeysInSlot(slot)
        ↓
for each remaining key:
    source lock persistenceMu
        ↓
    SnapshotEntry export
        ↓
    target ImportKey
        ↓ success only
    source DEL
        ↓
CommitMigration(slot)
        ↓
stable owner = target
```

The source does not delete a key before target import succeeds. A partial migration keeps the migration marker visible and can be retried from the source's remaining keys.

Target import reconstructs logical `SnapshotEntry` state using the existing command/persistence path, including absolute TTL deadlines. Physical Object encodings do not cross nodes.

## Cluster-aware RESP endpoint

`Server.ServeCluster` exposes the routing layer over RESP2/TCP on a caller-supplied listener. Each TCP connection owns one cluster session, so ASKING has true one-request connection semantics.

This endpoint is deliberately separate from the historical standalone listener. v0.10 is an educational Cluster layer, not a drop-in production Redis Cluster server.

## Bounded topology exchange

`Topology.GossipPage(start, limit)` returns at most `limit` slot records at a time. Repeated pages can rebuild another node's topology view.

Conflict resolution is explicit last-writer-wins by topology epoch with deterministic tie-breaking. This is a teaching mechanism for bounded dissemination, **not a consensus protocol**.

## Important limitations

v0.10 does not claim:

- production Redis Cluster wire/protocol completeness;
- consensus or linearizable slot ownership changes;
- automatic cluster failover coupling Sentinel to slot ownership;
- fencing against conflicting topology writers;
- transparent MULTI/EXEC across cluster routing;
- production anti-entropy or gossip failure detection.

v0.8 replication and v0.9 Sentinel remain independent lower layers. v0.10 adds the orthogonal question: **which primary owns this key's slot?**

## Learning demos

```bash
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make session-demo
make replication-demo
make sentinel-demo
make cluster-demo      # v0.10 MOVED → ASK/ASKING → slot transfer → MOVED
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
make cluster-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read v0.10 in this order

```text
internal/cluster/slot.go
  ↓ CRC16 + hash tags
internal/cluster/topology.go
  ↓ owners / migration state / MOVED / ASK / bounded gossip
internal/cluster/commands.go
  ↓ command key extraction / CROSSSLOT / ASKING session
internal/cluster/migration.go
  ↓ resumable slot transfer contract
internal/server/cluster.go
  ↓ routing gate + SnapshotEntry transfer under persistenceMu
internal/server/cluster_wire.go
  ↓ RESP/TCP cluster endpoint
internal/cluster/cluster_test.go
internal/server/cluster_test.go
internal/server/cluster_wire_test.go
examples/cluster/main.go
```

Compare historical branches `v0.1` through `v0.10` to see one dominant architecture change at a time.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical dependency-ordered roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
