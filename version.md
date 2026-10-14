# mini-redis v0.13 — Replication ACK / Authoritative Lag

## 1. Identity

- Historical learning branch: `v0.13`
- Previous snapshot: `v0.12`
- Version label: `v0.13.0`
- Theme: receiver acknowledgements, primary-visible applied offsets and truthful replication lag

## 2. Primary question

**How can a primary distinguish replication bytes it has produced from work a replica has actually applied?**

```text
primary canonical offset
        ↓
replication frame
        ↓
replica fully decodes + applies frame
        ↓
REPLCONF ACK <connection-replica-id> <applied-offset>
        ↓
primary validates progress under Primary.mu
        ↓
acknowledged offset
        ↓
lag_bytes = primary_offset - acknowledged_offset
```

The dominant lesson is:

> **Sender progress and receiver progress are different facts. Authoritative lag requires an explicit receiver→sender acknowledgement.**

## 3. Delta from v0.12

Implemented:

- one connection-local replica identity for every accepted replication connection;
- replica ID included in FULLRESYNC and CONTINUE handshake lines;
- bidirectional ACK control traffic on the existing replication TCP connection;
- FULLRESYNC ACK only after successful snapshot installation;
- frame ACK only after complete decode/apply and replica-offset advancement;
- immediate ACK of the retained local applied offset after CONTINUE negotiation;
- primary-side live ACK registry colocated with subscribers under `replication.Primary.mu`;
- monotonic/in-range ACK validation;
- rejection of wrong-ID, negative, regressing and ahead-of-primary ACKs;
- live progress removal on disconnect or subscriber backpressure eviction;
- one-lock `PrimarySnapshot` containing canonical offset, backlog state, sync counters and replica progress;
- immutable server replication snapshots exposing acknowledged offset, byte lag and ACK age;
- `INFO replication` live replica entries plus ACK received/rejected counters;
- real-replica integration tests;
- delayed/manual ACK tests that make lag become positive and return to zero;
- reconnect tests proving CONTINUE gets a fresh connection-local replica ID;
- `make replication-ack-demo` for the full lag lifecycle.

Unchanged:

- canonical mutation byte stream;
- primary repl-id semantics;
- frame-aware backlog retention;
- PSYNC eligibility rules;
- snapshot/base-offset/live-subscriber boundary under `Server.persistenceMu`;
- replica applied offset advances only after complete frame application;
- AOF/RDB formats;
- Sentinel failure detection and candidate ranking policy;
- Cluster routing/migration;
- RESP2/RESP3 client protocol semantics.

## 4. Protocol shape

Replica still begins with:

```text
PSYNC <repl-id|?> <offset|-1>
```

Primary replies with:

```text
FULLRESYNC <repl-id> <base-offset> <replica-id>
CONTINUE   <repl-id> <current-offset> <replica-id>
```

`replica-id` belongs to one live replication connection. It is not persistent node identity.

After the replica has actually installed/applied state through offset `N`, it writes:

```text
REPLCONF ACK <replica-id> <N>
```

ACK traffic is control metadata. It never enters AOF, RDB, the canonical replication backlog, or canonical offset accounting.

## 5. ACK invariants

For one live replication connection:

```text
0 <= previous_ack <= new_ack <= primary_offset
```

The primary rejects:

- malformed ACK frames;
- wrong connection-local replica IDs;
- negative offsets;
- regressing offsets;
- offsets beyond the primary canonical offset.

A rejected ACK never changes the last accepted progress.

Equal ACKs are valid. They can refresh ACK age without inventing new progress.

## 6. Apply-before-ACK invariant

### FULLRESYNC

```text
snapshot payload
    ↓
RestoreSnapshot succeeds
    ↓
replica local offset = base offset
    ↓
ACK base offset
```

### live / catch-up frame

```text
complete canonical frame
    ↓
decode all commands
    ↓
apply successfully under replica mutation boundary
    ↓
advance replica local offset by complete frame length
    ↓
ACK new applied offset
```

The replica never ACKs bytes merely read from TCP.

## 7. Primary ownership and snapshot boundary

`replication.Primary.mu` owns together:

```text
primary repl-id
backlog end/start offsets
live subscribers
connection-local replica IDs
accepted ACK offsets/timestamps
```

`PrimarySnapshot(now)` captures all of those values under one lock so INFO cannot combine an old primary offset with newer lag calculations, or vice versa.

Subscriber removal also removes the corresponding live ACK record under the same lock.

## 8. Lag semantics

For an acknowledged connected replica:

```text
lag_bytes = max(primary_offset - acknowledged_offset, 0)
ack_age_ms = now - last_accepted_ack_time
```

Before the first ACK:

```text
acknowledged_offset = -1
lag_bytes           = -1
ack_age_ms          = -1
```

This avoids silently treating unknown progress as zero lag.

`lag_bytes` is authoritative **according to the mini-redis protocol's receiver-reported applied offset**. A primary cannot independently prove that a malicious/manual peer truly applied what it ACKed.

`ack_age_ms` is not heartbeat age. An idle, caught-up replica may have `lag_bytes == 0` while ACK age keeps increasing.

ACK also does not mean fsync/disk durability because teaching replicas remain memory-only.

## 9. Reconnect semantics

Disconnect removes live progress.

A reconnect using the same primary repl-id and retained applied offset may receive CONTINUE, but the new TCP connection always receives a fresh connection-local replica ID:

```text
old connection: replica-1  → close → progress removed
new connection: replica-2  → CONTINUE → ACK retained offset
```

The durable continuity is repl-id + canonical offset, not connection-local replica ID.

## 10. Observability

Primary `Server.MetricsSnapshot().Replication.Replicas` exposes copies of:

```text
replica_id
remote_addr
sync_mode
acknowledged
acknowledged_offset
lag_bytes
ack_age_ms
```

Runtime metrics add:

```text
replication_acks_received
replication_acks_rejected
```

`INFO replication` is assembled from the immutable snapshot and does not expose subscriber pointers or mutable ACK maps.

## 11. Validation

The version must prove:

- monotonic in-range ACK acceptance;
- wrong-ID/negative/regressing/ahead ACK rejection without progress corruption;
- ACK control traffic does not alter primary canonical offset;
- real FULLRESYNC replica reaches acknowledged lag zero;
- live frame apply is followed by ACK and lag zero;
- deliberately withheld ACK while primary advances produces positive lag;
- later ACK returns lag to zero;
- disconnect removes live progress;
- CONTINUE reconnect receives a new connection-local replica ID;
- primary snapshot keeps offset/backlog/progress under one lock;
- historical replication/Sentinel/Cluster/RESP3/observability behavior remains green under race detector;
- `make replication-ack-demo` exercises caught-up → lagging → caught-up → disconnected → CONTINUE.

## 12. Non-goals

v0.13 does not implement:

- Redis WAIT / WAITAOF semantics;
- synchronous client writes waiting for replicas;
- replica fsync/disk acknowledgements;
- persistent replica IDs across reconnects;
- periodic ACK heartbeat timers;
- quorum write durability;
- automatic failover policy based on ACK age or byte lag;
- ACK-based replacement for Sentinel probes;
- production Redis REPLCONF compatibility beyond the teaching ACK shape.

## 13. Read this version in order

```text
internal/replication/primary.go
  ↓ live subscriber + ACK registry + PrimarySnapshot
internal/server/replication.go
  ↓ FULLRESYNC/CONTINUE + ACK reader/writer
internal/server/metrics.go
  ↓ authoritative lag snapshot / INFO
internal/replication/progress_test.go
internal/server/replication_ack_test.go
  ↓ invariants and real wire behavior
examples/replication-ack/main.go
  ↓ deterministic lag lifecycle
```

## 14. Definition of Done

- [x] ACK protocol implemented end-to-end;
- [x] primary live progress registry implemented;
- [x] authoritative byte lag exposed through immutable snapshot / INFO;
- [x] invalid ACK tests exist;
- [x] real delayed-ACK integration test proves lag rises and returns to zero;
- [x] reconnect/fresh connection ID behavior tested;
- [x] ACK observation remains separate from failover policy;
- [x] VERSION is `v0.13.0`;
- [ ] exact final `v0.13` SHA passes complete CI;
- [ ] `main` fast-forwarded to the same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] `main` and `v0.13` compare identical.

## 15. Next step

v0.13 starts the second evidence-driven systems arc by converting a v0.12 observability limitation into a real protocol mechanism.

A future v0.14 must again choose one explicit systems question. ACK-based client write waiting, idle heartbeat/progress reporting, disk durability acknowledgement and failover freshness are intentionally separate possible directions rather than bundled follow-ons.
