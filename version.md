# mini-redis v0.8 — Replication

## 1. Identity

- Branch: `v0.8`
- Previous snapshot: `v0.7`
- Version label: `v0.8.0`
- Theme: full sync, canonical mutation propagation, replication identity/offset, bounded backlog and partial resynchronization

## 2. Delta from v0.7

v0.7 made one client connection stateful. v0.8 makes one Redis process part of a **multi-process state topology**.

Before:

```text
client → one server → one logical database
```

Now:

```text
clients → primary
            │
            ├── logical snapshot base
            └── ordered mutation frames
                       ↓
                    replica
```

The new problem is not networking alone. It is defining exactly where the base snapshot ends, where the suffix starts, how that suffix is named by offsets, and when a reconnect may safely avoid another full snapshot.

## 3. Implemented mechanism

### Roles

- `standalone`
- `primary`
- `replica`

Primary has a replication listener. Replica follows one configured upstream and rejects direct mutating client commands.

### Canonical mutation feed

The existing reconstruction form is reused:

- relative TTL → absolute `PEXPIREAT`;
- eviction victims → `DEL`;
- failed/read-only commands → no feed frame;
- normal accepted mutation → one frame;
- successful EXEC mutation set → one `MULTI ... EXEC` frame.

### Identity and offset

- primary creates a fresh random repl-id on process creation;
- byte offset counts canonical RESP mutation payload bytes only;
- replica stores current repl-id and last completely applied byte offset in memory.

### Frame-aware backlog

The backlog has a byte budget but drops complete oldest frames. Partial resync therefore starts only at valid logical frame boundaries.

An evicted frame does **not** automatically make its end offset unusable: a replica that already applied that frame may resume at its end if the next frame remains retained.

### Full sync

Under `Server.persistenceMu` the primary atomically:

1. captures logical `SnapshotEntry[]`;
2. records current replication offset;
3. attaches a live subscriber at that offset.

Only then does snapshot transfer begin. Post-boundary writes accumulate in/live through the attached subscriber, preventing a snapshot→stream gap.

### Partial sync

Replica sends:

```text
PSYNC <repl-id-or-?> <offset>
```

Primary chooses:

```text
FULLRESYNC <id> <base-offset>
```

when identity/history cannot resume, or:

```text
CONTINUE <id> <current-offset>
```

when the requested suffix is retained.

Retained catch-up frames remain separate transport frames; they are not flattened into one byte blob, so transaction boundaries and offset commits remain explicit.

### Replica apply

Replica:

1. installs full base snapshot before suffix frames;
2. decodes canonical RESP payloads;
3. applies each complete frame under `persistenceMu`;
4. strips and validates `MULTI ... EXEC` transaction markers;
5. updates key-version runtime metadata;
6. advances offset only after complete frame success.

Replication apply bypasses client READONLY, but replica client mutation requests do not.

## 4. Critical invariants

```text
snapshot(baseOffset) + frames(baseOffset+1...) = primary logical order
```

And:

- no primary mutation can occur between snapshot capture and subscriber attachment;
- one canonical stream order drives both live replication and backlog history;
- byte offsets never refer to transport envelope bytes;
- resume happens only at complete-frame boundaries;
- backlog wrap may force full sync but cannot create partial/corrupt apply;
- stale repl-id forces full sync;
- a replica never applies suffix before base state;
- offset commits after frame success, not before;
- EXEC is one replication frame;
- private physical encodings never cross the replication boundary;
- replicas do not independently evict keys in v0.8.

## 5. Intentional simplifications

- replica repl-id/offset is not durable across replica restart;
- new primary process means new repl-id and therefore full sync;
- replica local AOF is disabled;
- replica maxmemory policy is disabled;
- replication uses a separate teaching TCP listener instead of sharing reactor internals;
- slow live subscribers are disconnected/recovered through reconnect rather than production-grade replication output-buffer management;
- no ACK quorum / WAIT semantics;
- no chained-replica or multi-primary model.

## 6. Files that define the version

Read in this order:

```text
internal/replication/backlog.go
  ↓
internal/replication/primary.go
  ↓
internal/server/replication.go
  ↓
internal/server/server.go
  ↓
internal/server/session.go
  ↓
internal/replication/*_test.go
internal/server/replication_test.go
examples/replication/main.go
```

## 7. Mechanism tests

The version includes focused tests for:

- frame offsets and complete-frame eviction;
- earliest valid resume boundary;
- oversized/non-retained frame behavior;
- stale identity rejection;
- atomic catch-up + live subscriber handoff;
- full-sync convergence;
- live propagation;
- primary/replica repl-id and offset convergence;
- replica READONLY behavior;
- forced disconnect followed by CONTINUE catch-up;
- backlog loss followed by FULLRESYNC fallback;
- MULTI/EXEC replication framing.

All server replication tests participate in `go test -race ./...`.

## 8. Observable demo

```bash
make replication-demo
```

The demo starts an in-process primary and replica and visibly walks:

```text
initial FULLRESYNC
       ↓
live SET propagation
       ↓
forced replication-link disconnect
       ↓
primary write during gap
       ↓
CONTINUE from retained backlog
       ↓
matching repl-id/offset
       ↓
replica client write → READONLY
```

## 9. Validation gate

Required exact-SHA validation:

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

Before documentation freeze, the implemented replication code already passed this mechanism/race/demo stack. The final documentation/version SHA must pass the same complete CI again before `main` advances.

## 10. Non-goals

- no Sentinel health/quorum/election;
- no automatic primary promotion;
- no Cluster/hash slots/MOVED/ASK;
- no production protocol-compatibility claim;
- no persistent replication metadata;
- no diskless production tuning claim.

## 11. Learning goal

After v0.8 the reader should be able to explain why replication correctness requires all of these concepts together:

```text
consistent base snapshot
+ ordered mutation stream
+ explicit stream identity
+ committed offset
+ retained reconnect history
+ full-sync fallback
```

Removing any one creates a different failure mode.

## 12. Next version

v0.9 builds Sentinel-inspired failover on top of this replication topology:

```text
health probes
  ↓
subjective down
  ↓
quorum-backed objective down
  ↓
election epoch
  ↓
replica selection / promotion
  ↓
replica reconfiguration
```

Sentinel must coordinate the v0.8 replication model, not replace it.
