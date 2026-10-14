# mini-redis v0.13 — Replication ACK / Authoritative Lag

## 1. Identity

- Historical learning branch: `v0.13`
- Previous snapshot: `v0.12`
- Version label: `v0.13.0`
- Theme: replica acknowledgements, primary-visible applied offsets and truthful replication lag

## 2. Primary question

**How can a primary know how far each replica has actually applied the canonical replication stream, rather than merely knowing what it has sent?**

```text
primary canonical offset
        ↓
replication frame
        ↓
replica fully decodes + applies frame
        ↓
ACK <replica-id> <applied-offset>
        ↓
primary validates monotonic ACK
        ↓
per-replica acknowledged offset
        ↓
lag_bytes = primary_offset - acknowledged_offset
```

The dominant lesson is:

> **Replication lag becomes authoritative only when the receiver explicitly reports durable/applied progress back to the sender.**

## 3. Delta from v0.12

Planned additions:

- connection-local replica identity assigned during PSYNC handshake;
- replica → primary ACK control frames after successful full-sync install and each successfully applied replication frame;
- primary-side per-replica acknowledgement registry;
- monotonic ACK validation;
- rejection of ACK offsets ahead of the primary canonical offset;
- connection teardown removes that connection's live ACK state;
- immutable replica progress snapshots;
- authoritative `lag_bytes` and `ack_age_ms` in replication metrics / INFO;
- metrics for ACK received/rejected and last ACK age;
- deterministic lag demo covering caught-up, delayed, disconnected and resumed states;
- tests proving ACK is emitted only after successful apply;
- tests proving malformed/regressing/ahead ACKs cannot corrupt progress state;
- preservation of existing FULLRESYNC / CONTINUE / backlog semantics.

Unchanged:

- canonical mutation byte stream;
- primary repl-id semantics;
- frame-aware backlog retention;
- snapshot boundary under `Server.persistenceMu`;
- replica applied offset advances only after full frame application;
- AOF/RDB formats;
- Sentinel candidate ranking policy;
- Cluster routing/migration;
- RESP2/RESP3 client protocol semantics.

## 4. Protocol shape

### Handshake

Replica still starts with:

```text
PSYNC <repl-id|?> <offset|-1>
```

Primary replies with a mode line that additionally carries one connection-local replica ID:

```text
FULLRESYNC <repl-id> <base-offset> <replica-id>
CONTINUE   <repl-id> <current-offset> <replica-id>
```

`replica-id` identifies one live replication connection, not durable replica identity.

### ACK

After the replica has actually installed/applied state through offset `N`, it writes:

```text
REPLCONF ACK <replica-id> <N>
```

ACK traffic is control-plane metadata. It is not part of the canonical mutation stream and therefore does not advance replication offsets or enter the backlog.

## 5. ACK invariants

For one live replica connection:

```text
last_ack <= new_ack <= primary_offset
```

The primary must reject:

- malformed ACK frames;
- wrong replica IDs;
- negative offsets;
- regressing offsets;
- offsets beyond the primary's current canonical offset.

A rejected ACK never changes accepted progress.

The replica sends ACK only after:

- a FULLRESYNC snapshot has been installed successfully; or
- an entire live/backlog frame has been decoded and applied successfully.

Therefore `acknowledged_offset` means **applied progress**, not bytes merely read from TCP.

## 6. Connection ownership

ACK state belongs to the live primary ↔ replica connection.

Primary registry entry:

```text
replica-id
remote address
acknowledged offset
last ACK time
sync mode
connected=true
```

When the connection closes, the live entry is removed. Historical counters may remain in observability, but stale connection progress must not be presented as live replica lag.

## 7. Lag semantics

For a connected replica:

```text
lag_bytes = max(primary_offset - acknowledged_offset, 0)
ack_age_ms = now - last_ack_time
```

This is authoritative with respect to the mini-redis definition of applied offset.

It is **not** a wall-clock replication-delay estimate and does not imply fsync/durable disk acknowledgement, because replicas remain memory-only in this teaching implementation.

## 8. Full sync and partial resync

### FULLRESYNC

```text
snapshot base offset B
        ↓
replica RestoreSnapshot succeeds
        ↓
replica local offset = B
        ↓
ACK B
```

### CONTINUE

```text
replica asks from offset R
        ↓
primary sends retained frames
        ↓
replica applies frame ending at N
        ↓
ACK N
```

The ACK mechanism must not alter backlog frame boundaries or PSYNC eligibility.

## 9. Observability

`Server.MetricsSnapshot().Replication` should expose live replica progress entries on a primary:

```text
replica_id
remote_addr
acknowledged_offset
lag_bytes
ack_age_ms
sync_mode
```

Runtime metrics add:

```text
replication_acks_received
replication_acks_rejected
```

Replica-local metrics from v0.12 remain useful and unchanged.

## 10. Tests that must prove the mechanism

- primary ACK registry accepts monotonic in-range progress;
- regressing/ahead/wrong-ID ACKs are rejected without changing state;
- snapshot ACK occurs only after successful RestoreSnapshot;
- frame ACK occurs only after successful complete frame application;
- ACK control frames do not enter backlog or advance primary offset;
- caught-up replica reports `lag_bytes == 0`;
- paused/delayed ACK produces positive lag while primary advances;
- reconnect receives a new connection-local replica ID;
- CONTINUE preserves offset continuity and catches lag back to zero;
- disconnect removes live replica progress;
- existing replication/Sentinel/Cluster/RESP3/observability regressions remain green under `-race`.

## 11. Non-goals

v0.13 does not implement:

- Redis WAIT / WAITAOF semantics;
- synchronous write acknowledgement to clients;
- disk/fsync acknowledgement from replicas;
- persistent replica IDs across reconnects;
- quorum write durability;
- automatic failover policy based on ACK age;
- TCP heartbeat replacement for Sentinel probes;
- production Redis REPLCONF compatibility beyond the teaching ACK shape.

## 12. Definition of Done

- ACK protocol implemented end-to-end;
- primary live progress registry implemented;
- authoritative lag exposed through immutable snapshot / INFO;
- malformed/regressing/ahead ACK tests exist;
- integration demo proves lag rises and returns to zero;
- ACK observation remains separate from failover policy;
- all historical CI gates remain green;
- VERSION/docs synchronized;
- exact `v0.13` SHA passes CI;
- `main` fast-forwarded to the same SHA;
- same SHA passes CI on `main`;
- `main` and `v0.13` compare identical.

## 13. Read this version in order

```text
internal/replication/primary.go
  ↓ live replica progress registry
internal/server/replication.go
  ↓ handshake + ACK reader/writer
internal/server/metrics.go
  ↓ authoritative lag snapshot
internal/replication/*_test.go
internal/server/replication*_test.go
  ↓
examples/replication-ack/main.go
```

## 14. Next step

v0.13 starts a second systems-learning arc by converting a v0.12 measurement limitation into a real protocol mechanism. Any v0.14 should again be justified by an explicit systems question revealed by the now-observable replication path.
