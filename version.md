# mini-redis v0.10 — Cluster / Hash Slots

## 1. Identity

- Historical learning branch: `v0.10`
- Previous snapshot: `v0.9`
- Version label: `v0.10.0`
- Theme: 16384 hash slots, deterministic routing, MOVED/ASK/ASKING, resumable logical key migration and bounded topology dissemination

## 2. Primary question

**How can multiple writable primaries partition one logical keyspace so every routed key has one deterministic slot/owner, clients receive explicit stable or migration-time redirects, and a slot can move without embedding topology state into Store?**

```text
key
 ↓ CRC16/XMODEM + optional non-empty {...} tag
slot ∈ [0, 16383]
 ↓
slot topology
 ├─ local stable owner  → execute
 ├─ remote stable owner → MOVED
 └─ source→target migration
       ├─ source has key   → execute source
       ├─ source lacks key → ASK target
       └─ target + ASKING  → execute once
```

## 3. Delta from v0.9

v0.9 decides **which replica is primary for one replicated dataset**. v0.10 adds an orthogonal topology dimension: **which primary owns this key's slot**.

Added:

- `internal/cluster` package;
- 16384 logical slots;
- Redis-style CRC-16/XMODEM hashing;
- first non-empty `{...}` hash-tag semantics;
- explicit node metadata and slot ownership table;
- stable MOVED redirects;
- CROSSSLOT protection for multi-key commands;
- explicit source→target migration state;
- temporary ASK redirects;
- one-request ASKING grants;
- resumable logical key migration;
- `SnapshotEntry` value transfer with TTL preservation;
- bounded topology pages and epoch-based deterministic merge;
- `Server.ClusterExecute` routing facade;
- `Server.ServeCluster` RESP2/TCP teaching endpoint;
- real server and wire-level tests;
- `make cluster-demo`.

Unchanged:

- Store physical encodings;
- standalone command semantics;
- v0.8 replication stream/full/partial sync;
- v0.9 Sentinel failure detection and promotion;
- AOF/RDB logical serialization boundary.

## 4. Hash-slot model

Every routed key maps to exactly one slot:

```text
slot = CRC16(hashInput(key)) mod 16384
```

If the key contains a first non-empty hash tag, only the tagged bytes are hashed:

```text
user:{42}:name
order:{42}:1
       ↓
     "42"
       ↓
   slot 8000
```

Empty or malformed tags fall back to hashing the whole key.

## 5. Routing before Store

`Server.ClusterExecute` validates the command and extracts its key arguments before Store execution.

For commands with multiple keys:

```text
all key slots equal?  yes → continue
                     no  → CROSSSLOT
```

For migration-sensitive requests, `Server.persistenceMu` covers:

```text
local existence observation
        ↓
slot route decision
        ↓
authorized command execution
```

So the fact used to authorize source-side execution cannot change between routing and mutation.

## 6. MOVED means stable ownership

If slot S is stably owned by node A:

```text
request to A → execute
request to B → MOVED S A-address
```

An unassigned slot returns `CLUSTERDOWN Hash slot not served`.

MOVED is deliberately different from migration-time ASK.

## 7. ASK / ASKING migration semantics

Beginning migration leaves the stable owner unchanged:

```text
owner = source
migrationTarget = target
```

During this state:

- source still serves a key it physically owns;
- source returns ASK once that key is absent locally;
- target without ASKING still redirects to the stable source;
- ASKING grants exactly one following routed command on that connection;
- the grant is consumed even though stable ownership has not changed yet.

`Server.ServeCluster` owns one `cluster.Session` per TCP connection, so ASKING cannot leak across clients.

## 8. Logical key migration

`cluster.MigrateSlot` transfers values through `store.SnapshotEntry`, not private Object payloads.

For one key:

```text
source persistenceMu
      ↓
capture SnapshotEntry
      ↓
target ImportKey
      ↓ target acknowledges success
source DEL
      ↓
release source persistenceMu
```

This gives the central transfer invariant:

> the source never deletes the key before the target has successfully imported the complete logical value.

Target import reconstructs through existing command machinery, preserving absolute TTL deadlines and publishing through existing AOF/replication paths when enabled.

## 9. Partial failure and resume

A partially completed slot move may already have some keys on the target. Therefore failure does not erase migration state.

```text
owner = source
migrationTarget = target
```

remains visible. A retry recognizes the same migration and transfers the source's remaining keys.

Stable ownership changes only after all remaining source keys transfer successfully.

## 10. Commit boundary

After migration completes:

```text
owner = target
migrationTarget = ""
new topology epoch
```

Then:

- old source returns MOVED target;
- target serves normally without ASKING.

## 11. Bounded gossip teaching mechanism

`GossipPage(start, limit)` exposes at most `limit` slot records. A peer reconstructs topology through repeated bounded pages.

Merge policy:

```text
higher epoch wins
same slot epoch:
  lexicographically larger (ownerID, migrationTarget) tuple wins
same node epoch:
  deterministic address tie-break
```

Tests apply equal-epoch conflicting pages in opposite orders and require the same result.

This is deterministic LWW dissemination, **not consensus**.

## 12. Cluster-aware wire endpoint

`Server.ServeCluster` is a separate RESP2/TCP endpoint that routes commands through the v0.10 gate.

Wire tests prove real protocol behavior:

```text
-MOVED <slot> <address>
+OK        # ASKING
+OK        # one importing command
-MOVED ... # next command: grant already consumed
```

Keeping this endpoint separate makes the teaching boundary explicit. The historical standalone listener bypasses cluster routing and must not be used as authoritative cluster traffic.

## 13. Transaction scope

The v0.10 cluster facade intentionally rejects MULTI/EXEC/WATCH routing.

Reason: cross-command cluster transaction ownership is a separate problem. Returning an explicit error is safer than pretending that queue-time slot validation alone gives distributed transaction semantics.

## 14. Critical invariants

- exactly 16384 logical slots;
- every routed key has one deterministic slot;
- equal non-empty hash tags force equal slots;
- cross-slot multi-key commands fail before mutation;
- stable wrong-owner routing is MOVED;
- temporary migration routing is ASK;
- ASKING is one request on one connection;
- migration metadata never becomes Store payload data;
- physical Object encodings never cross node boundaries;
- source delete occurs only after target import success;
- partial migration state stays explicit and retryable;
- stable owner changes only after migration completion;
- migration preserves absolute TTL;
- routing existence check and execution share `persistenceMu`;
- equal-epoch gossip conflicts resolve independent of arrival order;
- bounded gossip does not imply consensus.

## 15. Tests that prove the mechanism

Pure cluster tests prove:

- CRC16 known vectors;
- hash-tag semantics;
- CROSSSLOT;
- MOVED/ASK routing;
- migration commit;
- bounded gossip reconstruction;
- equal-epoch order-independent merge;
- partial migration failure visibility and retry convergence.

Real server tests prove:

- owner execution and wrong-owner MOVED;
- CLUSTER KEYSLOT;
- ASKING one-shot behavior;
- logical two-key slot migration;
- TTL preservation;
- source deletion and post-commit routing.

Wire test proves:

- RESP/TCP MOVED;
- ASKING + one command on the same connection;
- the next request is redirected again while stable ownership remains source.

Everything runs under `go test -race ./...`.

## 16. Demo

```bash
make cluster-demo
```

shows:

```text
hash slot / hash tag
      ↓
MOVED
      ↓
CROSSSLOT
      ↓
BeginMigration
      ↓
ASK / ASKING one-shot
      ↓
resumable SnapshotEntry transfer
      ↓
ownership commit
      ↓
old owner MOVED / new owner execute
      ↓
bounded gossip reconstruction
```

## 17. Explicit limitations

v0.10 does not claim:

- production Redis Cluster protocol/bus compatibility;
- distributed consensus for topology changes;
- fencing against conflicting slot-owner writers;
- production gossip failure detection/anti-entropy;
- automatic Sentinel-to-Cluster failover integration;
- cluster MULTI/EXEC semantics;
- transparent routing through the historical standalone endpoint;
- atomic multi-slot migration.

## 18. Read this version in order

```text
internal/cluster/slot.go
  ↓
internal/cluster/topology.go
  ↓
internal/cluster/commands.go
  ↓
internal/cluster/migration.go
  ↓
internal/server/cluster.go
  ↓
internal/server/cluster_wire.go
  ↓
internal/cluster/cluster_test.go
internal/server/cluster_test.go
internal/server/cluster_wire_test.go
  ↓
examples/cluster/main.go
```

## 19. Next version

v0.11 returns to the client protocol layer: **RESP3**.

The main question becomes:

> How can one connection negotiate richer response types and push messages while preserving all existing storage, replication, failover and cluster-routing boundaries?
