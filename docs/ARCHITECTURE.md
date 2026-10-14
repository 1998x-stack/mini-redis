# Architecture — v0.13

## 1. What v0.13 changes

v0.8 already made replication data flow explicit:

```text
primary logical mutation
        ↓
canonical RESP command bytes
        ↓
primary repl-id + monotonic byte offset
        ↓
frame-aware backlog
        ↓
FULLRESYNC / CONTINUE / live stream
        ↓
replica applies complete frames
```

v0.12 then exposed an observability gap: the primary knew its own canonical offset, but it had no protocol evidence for how far a remote replica had actually applied.

v0.13 adds the missing reverse direction:

```text
                 canonical data direction
primary  ─────────────────────────────────────►  replica
  │                                                  │
  │                                                  │ complete apply boundary
  │                                                  ↓
  ◄──────────────── REPLCONF ACK ────────────────────┘
                 applied offset direction
```

The central lesson is:

> **Sent work and applied work are different states. Receiver progress needs an explicit acknowledgement protocol.**

## 2. Data stream and ACK stream are different things

The primary canonical offset still counts only canonical mutation bytes.

```text
SET k v
   ↓
RESP command frame
   ↓
Primary.Append(frame)
   ↓
backlog / offset advance
```

ACK is control metadata:

```text
REPLCONF ACK replica-7 12345
```

It is intentionally excluded from:

- AOF;
- RDB;
- SnapshotEntry;
- replication backlog;
- canonical replication offset;
- Cluster state;
- Sentinel state.

Therefore sending one ACK or one million ACKs cannot advance `Primary.Offset()`.

## 3. Handshake now assigns connection-local identity

Replica still requests synchronization with:

```text
PSYNC <repl-id|?> <offset|-1>
```

The primary returns one extra field:

```text
FULLRESYNC <primary-repl-id> <base-offset> <replica-id>
CONTINUE   <primary-repl-id> <current-offset> <replica-id>
```

Example:

```text
FULLRESYNC abcd... 1200 replica-3
```

`replica-3` is a live-connection identifier. It is deliberately not durable replica identity.

Why connection-local identity?

```text
TCP connection A → replica-3
close
TCP connection B → replica-4
```

The primary must be able to reject an ACK that belongs to an obsolete connection even when the new connection resumes the same primary repl-id and canonical offset.

Durable stream continuity remains:

```text
primary repl-id + applied offset
```

not:

```text
connection replica-id
```

## 4. ACK ownership lives in replication.Primary

`replication.Primary` already owns:

```text
primary repl-id
backlog
canonical end offset
live subscribers
```

v0.13 adds to the same ownership domain:

```text
connection-local replica id
remote address
sync mode
has ACKed?
last accepted ACK offset
last accepted ACK time
```

All of these are protected by:

```text
replication.Primary.mu
```

This is important because ACK validation needs one coherent view of:

```text
is this replica connection still live?
what was its previous ACK?
what is the current primary offset?
```

The acceptance rule is evaluated under that one lock:

```text
0 <= previous_ack <= new_ack <= primary_offset
```

## 5. Why ACK validation is monotonic

Accepted progress cannot move backward.

```text
ACK 100  ✓
ACK 140  ✓
ACK 140  ✓  equal ACK may refresh age
ACK 120  ✗  regression
ACK 999  ✗  if primary_offset < 999
```

The primary also rejects:

- wrong connection replica ID;
- negative offset;
- malformed REPLCONF/ACK frame.

A rejected ACK does not modify the last accepted progress.

For the teaching protocol, invalid ACK traffic terminates that replication connection rather than trying to recover inside the same stream.

## 6. Apply-before-ACK is the key replica invariant

The useful meaning of ACK depends entirely on ordering.

### FULLRESYNC

```text
read FULLRESYNC header
      ↓
read snapshot payload
      ↓
decode SnapshotEntry[]
      ↓
RestoreSnapshot succeeds
      ↓
replica repl-id = primary repl-id
replica offset  = base offset
      ↓
REPLCONF ACK <replica-id> <base-offset>
```

If snapshot restore fails, the ACK is never sent.

### Live/backlog frame

```text
read complete bulk frame
      ↓
decode all canonical commands
      ↓
apply every command successfully
      ↓
advance replicaOffset by exact frame bytes
      ↓
ACK replicaOffset
```

The replica therefore does not ACK bytes merely received from TCP.

This is still memory-applied progress, not disk-durable progress.

## 7. FULLRESYNC and CONTINUE remain the same data mechanisms

ACK does not change PSYNC eligibility.

### FULLRESYNC path

```text
identity mismatch / backlog gap
        ↓
logical snapshot
        ↓
base offset + live subscriber attachment
        ↓
FULLRESYNC ... <replica-id>
        ↓
snapshot payload
        ↓
replica installs snapshot
        ↓
ACK base offset
```

### CONTINUE path

```text
same primary repl-id
+ retained exact frame boundary
        ↓
CONTINUE ... <new-replica-id>
        ↓
replica ACKs its already-applied resume offset
        ↓
retained catch-up frames
        ↓
ACK after every complete apply
        ↓
live stream
```

The old v0.8 frame-aware backlog and offset continuity rules remain unchanged.

## 8. One replication TCP connection is now full duplex

Previously the normal teaching flow was conceptually mostly:

```text
replica → PSYNC
primary → handshake + snapshot/frames
```

v0.13 keeps the same connection and uses TCP full duplex:

```text
primary writer goroutine
    ↓
handshake / bulk replication frames

same TCP connection

primary ACK-reader goroutine
    ↑
REPLCONF ACK frames
```

Reading ACKs does not serialize frame writing. The two directions are independent except where both touch `Primary.mu` state.

If the ACK reader sees invalid protocol or EOF, the connection is closed and the live subscriber/progress entry is removed.

## 9. Subscriber lifecycle and ACK lifecycle are one lifecycle

A live progress record must never survive its stream connection.

Therefore subscriber removal is centralized:

```text
removeSubscriberLocked(id)
   ├── close subscriber channel
   ├── delete replica-id index
   └── delete subscriber / ACK progress
```

This same path is used for ordinary disconnect and output-buffer overflow eviction.

That prevents this incorrect state:

```text
replica disconnected
but INFO still shows old lag=0
```

After disconnect, live progress is absent.

Historical counters such as total ACKs received may remain, because counters are observation history rather than live connection state.

## 10. Authoritative lag definition

For one connected replica after its first accepted ACK:

```text
lag_bytes = max(primary_offset - acknowledged_offset, 0)
```

Example:

```text
primary offset = 2000
replica ACK    = 2000
lag            = 0

primary appends 120 bytes
primary offset = 2120
replica ACK    = 2000
lag            = 120

replica applies frame and ACKs 2120
lag            = 0
```

Before any ACK, progress is unknown:

```text
acknowledged_offset = -1
lag_bytes           = -1
ack_age_ms          = -1
```

Unknown is not silently converted to zero.

## 11. What “authoritative” means here

It means:

> according to the mini-redis replication protocol, the primary has explicit receiver-reported evidence of the last applied canonical offset.

It does **not** mean the primary can cryptographically or independently prove that an arbitrary peer really applied the data before ACKing.

A well-behaved mini-redis replica follows apply-before-ACK. A deliberately dishonest manual peer could lie.

This distinction matters when reasoning about real distributed protocols: acknowledgements create protocol evidence, not magical independent verification.

## 12. ACK age is not heartbeat age

The snapshot also exposes:

```text
ack_age_ms = now - last accepted ACK time
```

In v0.13 replicas send ACKs when applied progress changes or when CONTINUE establishes the resume boundary. They do not send periodic idle heartbeats.

Therefore this is valid:

```text
lag_bytes = 0
ack_age_ms = 300000
```

The replica may simply have been idle and fully caught up for five minutes.

Consequences:

- ACK age is not a liveness detector;
- Sentinel must not use ACK age as SDOWN/ODOWN evidence;
- candidate ranking is unchanged;
- periodic progress/heartbeat reporting is a separate possible future mechanism.

## 13. PrimarySnapshot makes INFO internally coherent

v0.12 originally assembled replication metrics using separate getters:

```text
Offset()
BacklogStartOffset()
Stats()
ReplicaProgressSnapshot()
```

A concurrent mutation could theoretically occur between those calls.

v0.13 adds:

```text
PrimarySnapshot(now)
```

which captures under one `Primary.mu`:

```text
repl-id
primary offset
backlog start
full/partial sync counters
subscriber count
all ACK progress / lag calculations
```

`Server.MetricsSnapshot()` then copies that snapshot into the INFO representation.

This gives one internally consistent replication observation without holding Store or server-global locks while formatting INFO.

## 14. INFO replication

The primary view now includes live replica entries conceptually shaped as:

```text
replicas: [
  {
    replica_id:          "replica-7",
    remote_addr:         "127.0.0.1:54321",
    sync_mode:           "full",
    acknowledged:        true,
    acknowledged_offset: 2120,
    lag_bytes:           0,
    ack_age_ms:          4
  }
]

acks_received:  12
acks_rejected:  0
```

The values come from copied structs and atomic metric snapshots. No subscriber channel, mutex or mutable map is exposed.

RESP3 can represent nested maps directly. RESP2 uses the v0.11 downgrade path.

## 15. Relationship to Sentinel

Replication ACK and Sentinel answer different questions.

```text
ACK / lag:
  how far has this live replication receiver reported applying?

Sentinel:
  is this node reachable according to monitor probes?
  is there SDOWN quorum?
  which reachable replica is eligible/freshest for failover?
```

v0.13 intentionally does not feed `lag_bytes` or `ack_age_ms` into Sentinel decisions.

That separation prevents an observability/progress mechanism from silently becoming a new HA policy.

## 16. Relationship to durability

Current replicas are memory-only in the v0.8 teaching contract.

Therefore ACK means:

```text
applied to replica logical in-memory state
```

not:

```text
fsynced to replica disk
```

Client-visible synchronous replication, WAIT/WAITAOF, quorum durability and replica disk acknowledgements require additional protocol semantics and are outside v0.13.

## 17. Validation model

### Primary unit invariants

- monotonic ACK acceptance;
- equal ACK refresh;
- negative/wrong/regressing/ahead rejection;
- rejected ACK cannot overwrite accepted state;
- ACK cannot advance canonical offset;
- disconnect removes progress.

### Real replica integration

- FULLRESYNC snapshot install → ACK;
- live SET apply → ACK;
- primary and acknowledged offsets converge;
- historical replica READONLY semantics remain unchanged.

### Delayed manual ACK

A raw teaching replica intentionally withholds ACK:

```text
initial ACK → lag 0
primary SET → lag > 0
read frame + ACK → lag 0
```

This directly proves that lag is based on reported applied progress rather than bytes sent.

### Reconnect

```text
replica-1 closes
  ↓
live progress removed
  ↓
PSYNC same repl-id + retained offset
  ↓
CONTINUE ... replica-2
  ↓
ACK resume offset
  ↓
lag 0
```

### Demo

```bash
make replication-ack-demo
```

prints the same lifecycle through real TCP connections.

## 18. Explicit limits

v0.13 intentionally omits:

- persistent replica node IDs;
- periodic heartbeat ACKs;
- client WAIT / synchronous replication;
- replica fsync/disk ACK;
- quorum durability;
- ACK-based Sentinel health policy;
- production Redis REPLCONF compatibility guarantees.

These are separate systems questions, not missing lines in the current implementation.

## 19. Next architectural question

v0.13 establishes a second-arc development rule:

```text
measure a real limitation
  ↓
add one explicit mechanism
  ↓
measure the new boundary
```

A future version should choose one next question from evidence—such as client-visible synchronous replication, idle progress heartbeat, replica disk durability, or another measured subsystem bottleneck—rather than bundling them together.
