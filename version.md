# mini-redis v0.8 — Replication

## 1. Identity

- Active development branch: `v0.8`
- Previous snapshot: `v0.7`
- Target version label: `v0.8.0`
- Theme: full sync, ordered mutation propagation, replication offsets/backlog, partial resynchronization

## 2. Primary question

**How can a replica establish one consistent base state, follow exactly the primary's accepted logical mutation order, and reconnect using only the missing suffix when that suffix is still retained?**

v0.8 adds a replication plane beside the existing client plane:

```text
client RESP connections
        ↓
server command/session semantics
        ↓
Server.persistenceMu
        ├── AOF ordering
        └── canonical replication feed
                 ↓
          replication.Primary
          ├── repl-id
          ├── byte offset
          ├── bounded frame backlog
          └── live subscribers
                 ↓
         replication transport
                 ↓
               replica
```

The replication stream contains logical RESP reconstruction commands, not private Dict/Object memory layout.

## 3. Roles

v0.8 exposes three explicit roles:

- `standalone`: no replication listener or upstream;
- `primary`: accepts replica synchronization connections;
- `replica`: connects to one configured primary and rejects client-side mutating commands.

Replica persistence and local eviction policy are deliberately constrained in the first teaching version so the primary remains the source of mutation policy.

## 4. Canonical replication stream

The stream reuses the same logical mutation commands already produced for AOF reconstruction:

- relative TTL becomes absolute `PEXPIREAT`;
- eviction-generated `DEL` is included;
- read-only / failed commands emit no replication bytes;
- one normal accepted logical mutation becomes one replication frame;
- one successful `EXEC` becomes one frame containing `MULTI ... EXEC`.

Offsets count canonical RESP payload bytes. They do not count TCP framing bytes.

## 5. Backlog invariant

The backlog is bounded in bytes but evicts **whole replication frames** rather than arbitrary byte prefixes. Therefore a resumable offset always begins at a valid frame boundary.

```text
frame A     frame B      frame C
[1..40]     [41..90]     [91..140]

replica offset=90
        ↓
resume with frame C
```

If capacity pressure removes frame B, an offset inside/behind the removed history cannot partial-resync and must full-sync.

## 6. Full synchronization boundary

Primary full sync must establish:

```text
lock persistenceMu
    ↓
logical SnapshotEntry snapshot
    ↓
capture replication base offset
    ↓
attach live subscriber at that exact offset
    ↓
unlock
    ↓
transfer snapshot
    ↓
drain buffered post-snapshot frames
    ↓
continue live
```

The subscriber is attached while the mutation-ordering lock is still held, so writes after the snapshot boundary cannot fall into a gap.

The snapshot wire representation is logical `SnapshotEntry` data, never physical encodings.

## 7. Partial resynchronization

Replica reconnect request carries its known replication identity and last completely applied stream offset.

Primary returns partial continuation only when:

1. replication identity still matches;
2. requested offset is a retained frame boundary;
3. backlog contains every byte after that offset.

Otherwise it falls back to full sync.

A new primary process generates a new replication identity, intentionally forcing full sync after primary restart in v0.8.

## 8. Replica apply rules

- full snapshot is restored before any suffix frame is applied;
- a frame is applied while holding `persistenceMu`;
- `MULTI ... EXEC` feed frames apply as one non-interleaved unit;
- replica offset advances only after a complete frame applies successfully;
- replicated commands bypass the client READONLY gate but do not execute local eviction decisions;
- absolute expiry deadlines preserve the primary's logical lifetime semantics;
- client mutating commands on replicas return READONLY.

## 9. Transport scope

Client traffic keeps the v0.2 reactor/legacy engines. Replication uses a separate educational TCP connection so client event-loop mechanics do not obscure synchronization semantics.

Handshake is Redis-inspired:

```text
PSYNC <repl-id-or-?> <offset>

FULLRESYNC <new-id> <base-offset>
  + logical snapshot payload
  + live mutation frames

or

CONTINUE <id> <current-offset>
  + retained backlog suffix
  + live mutation frames
```

Transport frames carry canonical RESP mutation payloads; replication offsets count the payload bytes only.

## 10. Required validation

- backlog append/whole-frame eviction/boundary lookup;
- matching identity + retained suffix → partial resync;
- stale identity or wrapped backlog → full sync;
- writes during snapshot transfer are delivered after the base snapshot;
- replica converges after full sync;
- disconnect/reconnect converges through partial resync;
- transaction order/framing survives replication;
- replica rejects direct mutating client commands;
- primary/replica tests run under `go test -race`;
- deterministic replication demo distinguishes FULLRESYNC from CONTINUE.

## 11. Non-goals

- no Sentinel health/quorum/election;
- no automatic replica promotion;
- no multi-primary;
- no Cluster slots/MOVED/ASK;
- no diskless production tuning claim;
- no cross-process persistence of replication offset in v0.8.

## 12. Definition of Done

The exact v0.8 SHA must pass repository baseline validation plus replication-specific unit/integration/demo tests. Only then may `main` fast-forward to the same SHA.

## 13. Next version

v0.9 will build Sentinel-inspired failover on top of the replication topology established here: health observation, subjective/objective failure, quorum/election, promotion, and reconfiguration.