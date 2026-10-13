# Architecture — v0.8

## 1. Current stack

```text
client TCP/RESP2
      ↓
reactor or legacy transport
      ↓
clientSession
      ↓
Server.persistenceMu  ← canonical logical order
      ├── command execution
      ├── memory-pressure decision
      ├── AOF publication
      ├── RDB/full-sync snapshot capture
      └── replication publication
              ↓
       replication.Primary
       ├── repl-id
       ├── canonical byte offset
       ├── frame-aware backlog
       └── live subscribers
              ↓
       replication TCP
              ↓
            replica
              ↓
   SnapshotEntry restore + RESP frame apply
```

v0.8's central lesson is that replication is not “copy the Go heap.” It is **base state + one ordered logical mutation stream**.

## 2. Why replication has a separate transport

Client I/O mechanics were already the v0.2 lesson. Reusing the client reactor for every replication concern would mix epoll mechanics with synchronization semantics.

v0.8 therefore keeps:

```text
client plane      → normal RESP command server
replication plane → dedicated TCP synchronization connection
```

The replication transport is separate, but the mutation payload is deliberately **not** a private protocol object graph. Frames contain canonical RESP reconstruction commands already understood by the command layer.

## 3. The canonical stream

An accepted mutation already produces reconstruction commands for AOF. v0.8 reuses that logical form:

```text
SET session abc EX 10
        ↓
SET session abc
PEXPIREAT session <absolute-ms>
```

Likewise, memory-pressure eviction contributes reconstruction `DEL` commands.

One normal accepted logical mutation becomes one replication frame. Successful EXEC mutations become one frame:

```text
MULTI
<mutation reconstruction commands>
EXEC
```

Reads, failed commands and rejected replica writes emit no replication bytes.

## 4. Replication identity and byte offset

A primary process owns a random replication identity. v0.8 intentionally does not persist it across primary restart.

The replication offset is the cumulative length of canonical RESP **payload bytes**:

```text
frame A: bytes 1..40
frame B: bytes 41..90
frame C: bytes 91..140
```

TCP bulk-string envelope bytes do not count. This keeps stream position independent of transport packetization.

Replica offset advances only after the whole corresponding frame applies successfully.

## 5. Frame-aware backlog

The primary keeps a bounded runtime backlog.

A naive byte ring can overwrite the first half of a RESP command while retaining its tail. v0.8 instead evicts complete frames:

```text
capacity pressure
       ↓
drop complete oldest frame
       ↓
remaining resume points are still logical boundaries
```

Important subtlety: if frame A has been evicted, a replica whose offset equals **A's end** may still resume when frame B is retained. The replica already has A; it needs only B onward.

An offset inside a frame is never accepted as a partial-resync point.

## 6. Full synchronization boundary

The hard problem is not serializing a snapshot; it is preventing a gap between snapshot and subsequent live writes.

The primary performs:

```text
lock persistenceMu
    ↓
SnapshotEntry[] = current logical state
    ↓
baseOffset = replication current end
    ↓
attach live subscriber at baseOffset
    ↓
unlock persistenceMu
    ↓
serialize / transfer snapshot
    ↓
consume buffered frames created after baseOffset
    ↓
continue live
```

Because ordinary mutations also need `persistenceMu`, there cannot be a write between snapshot capture and subscriber attachment.

This gives the invariant:

```text
replica state = snapshot(baseOffset) + every frame after baseOffset
```

If a subscriber cannot keep up and its bounded channel is lost, correctness is recovered by reconnecting rather than silently dropping frames.

## 7. Handshake

Replica sends:

```text
PSYNC <known-repl-id-or-?> <last-applied-offset>
```

### Full path

Used when identity is unknown/stale or the needed suffix is no longer retained:

```text
+FULLRESYNC <id> <base-offset>
$<snapshot-json>
<live bulk frames...>
```

The snapshot JSON is an educational wire representation of logical `SnapshotEntry[]`, not private object encodings.

### Partial path

Used when identity matches and offset is a retained frame boundary:

```text
+CONTINUE <id> <current-offset>
<retained missing frame 1>
<retained missing frame 2>
...
<live frames...>
```

Catch-up frames are sent separately. Concatenating them into one payload would destroy transaction/frame boundaries and make offset advancement ambiguous.

## 8. Replica apply path

Replica application intentionally bypasses the normal client write gate:

```text
replication frame
      ↓
decode canonical RESP commands
      ↓
lock persistenceMu
      ↓
apply commands directly through command.Registry
      ↓
update WATCH/key-version runtime metadata
      ↓
advance replica offset by frame payload length
```

If a frame starts with MULTI it must close with EXEC. Markers are stripped and all enclosed reconstruction commands apply under one `persistenceMu` hold.

Client-originated mutating commands on the same replica return:

```text
READONLY You can't write against a read only replica.
```

Thus “replication may mutate replica state” and “clients may not mutate replica state” are two distinct authorization paths.

## 9. Why v0.8 disables replica AOF and maxmemory

A teaching replica with its own eviction policy could remove a different victim than the primary and diverge. A replica re-appending replicated frames to an independent AOF would also introduce a second durability story before replication semantics are established.

So v0.8 constrains replicas to:

```text
appendonly = false
maxmemory  = 0
```

This is an explicit simplification, not a statement about production Redis.

## 10. Expiration semantics

The canonical stream carries absolute `PEXPIREAT` deadlines. Primary and replica therefore agree on one logical expiration instant instead of restarting a relative TTL at replication time.

Active/lazy expiration can physically remove an expired key on either node after the deadline, but it cannot extend the lifetime beyond the propagated absolute deadline.

## 11. Transactions across replication

v0.7 established one non-interleaved EXEC execution/persistence boundary. v0.8 extends it:

```text
lock persistenceMu
    ↓
WATCH validation
    ↓
execute queued commands
    ↓
collect successful mutation reconstruction commands
    ↓
AOF MULTI...EXEC frame
    ↓
replication MULTI...EXEC frame
    ↓
unlock
```

This avoids publishing transaction children as unrelated independently resumable frames.

## 12. Full versus partial resync

```text
reconnect
   ↓
repl-id matches?
 ├── no → FULLRESYNC
 └── yes
       ↓
 offset is retained complete-frame boundary?
 ├── no → FULLRESYNC
 └── yes → CONTINUE + missing frames
```

Backlog wrap therefore affects **efficiency**, not correctness.

## 13. Observability used by tests/demo

`replication.Primary` exposes teaching stats:

```text
FullSyncs
PartialSyncs
Subscribers
```

The replica exposes its current repl-id and last applied offset. `DisconnectReplicationLink()` is a deliberate diagnostic hook used to force reconnect without destroying in-memory identity/offset.

`make replication-demo` visibly demonstrates:

```text
FULLRESYNC
  ↓
live SET propagation
  ↓
forced disconnect
  ↓
write while disconnected
  ↓
CONTINUE from backlog
  ↓
READONLY replica client write
```

## 14. Validation model

Focused mechanism tests:

- frame offsets and whole-frame eviction;
- earliest resumable boundary;
- stale identity rejection;
- live subscription handoff.

Server integration tests:

- initial full-sync convergence;
- live write propagation;
- repl-id/offset equality after convergence;
- replica READONLY;
- disconnect + partial catch-up;
- backlog loss + full-sync fallback;
- transaction frame replication.

Everything runs under `go test -race ./...` plus the normal demos/smoke tests.

## 15. Runtime state versus database state

Database/logical state:

```text
keys
logical values
absolute expiry deadlines
```

Runtime replication state:

```text
primary repl-id
primary byte offset
backlog
subscriber channels
replica last-applied offset
replication TCP connection
sync counters
```

v0.8 deliberately does not persist the latter.

## 16. Non-goals

- no persistent replication identity/offset;
- no ACK quorum or WAIT;
- no replica promotion;
- no Sentinel failure detection/election;
- no Cluster slots;
- no claim of production-compatible Redis replication wire protocol;
- no production diskless-sync optimization.

## 17. Next architectural layer

v0.9 asks a different question: once copies exist, **who decides the primary is actually dead and which replica should become the next primary?**

```text
replication topology
       ↓
health observations
       ↓
SDOWN vs quorum-backed ODOWN
       ↓
election epoch
       ↓
replica selection / promotion
       ↓
topology reconfiguration
```

Sentinel work must reuse this v0.8 replication topology rather than bypassing it.
