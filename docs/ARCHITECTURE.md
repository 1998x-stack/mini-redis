# Architecture — v0.7

## 1. Current stack

```text
redis-cli / client
      ↓
RESP2
      ↓
reactor client or legacy connection
      ↓
clientSession
  ├── transaction queue
  ├── WATCH tokens
  └── channel subscriptions
      ↓
Server coordination
  ├── persistenceMu
  ├── keyVersionTable
  └── pubSubHub
      ↓
command.Registry
      ↓
memorypolicy.Manager (optional)
      ↓
versionedmemory.Memory
  ├── keyspace Dict
  ├── expiry Dict
  └── Object encodings
      ↓
SnapshotEntry logical state
      ↓
RDB / AOF
```

v0.7 adds connection semantics without moving them into the Store. This distinction is the main architecture lesson.

## 2. Why transactions require client state

Before v0.7 every parsed command could be executed immediately:

```text
RESP argv → executeCommand(argv) → reply
```

MULTI breaks that assumption. After one connection sends MULTI, the same `SET` request means “validate and queue,” while another connection's identical request still means “execute now.” Therefore transaction state belongs to the connection:

```text
clientSession
  inMulti
  dirty
  queue[][]string
  watched map[key]token
```

This state is destroyed with the connection. It is not database content.

## 3. Queue-time versus execution-time errors

The registry now exposes validation without executing handlers.

Queue-time failure:

```text
MULTI
NO_SUCH_COMMAND  → error now, mark dirty
SET k v          → QUEUED
EXEC             → EXECABORT, execute nothing
```

Execution-time failure:

```text
SET n text
MULTI
INCR n           → QUEUED
SET survivor yes → QUEUED
EXEC             → [integer-error, OK]
```

The second SET survives. Redis transactions give ordered, non-interleaved execution; they do not provide SQL-style rollback.

## 4. WATCH as optimistic concurrency control

WATCH does not lock a key. It records a token and lets other clients continue:

```text
WATCH k records T0
        ↓
other clients run normally
        ↓
EXEC compares current token with T0
```

The token contains global epoch, per-key write version and whether the key existed.

Why version instead of only value comparison?

```text
WATCH k   (value=A)
other: SET k B
other: SET k A
EXEC
```

Final value equals the original, but an intervening mutation occurred. The version changed, so EXEC aborts.

Existence is also rechecked. This makes an already-watched key that expires before EXEC conflict even though expiration is not modeled as a client write-version event.

`FLUSHDB` advances a global epoch instead of enumerating every watched key version.

## 5. The EXEC serialization boundary

The sequence is:

```text
lock persistenceMu
    ↓
validate every WATCH token
    ↓
execute queued command 1
execute queued command 2
...
execute queued command N
    ↓
append one transaction AOF frame
unlock persistenceMu
```

Normal commands already use this lock from earlier persistence work, so another client cannot insert a logical mutation between queued EXEC commands.

The legacy engine also has a timer goroutine. v0.7 moves active-expiration maintenance through the same boundary so TTL cleanup cannot slip between WATCH validation and the transaction body. RDB snapshot capture also takes the boundary while extracting logical state, preventing a snapshot of only part of EXEC.

The Store mutex remains the lower-level data-structure safety boundary; these locks solve different problems.

## 6. Why AOF needs a transaction frame

Suppose EXEC runs:

```text
SET a 1
SET b 2
```

If AOF writes them as unrelated records and the process crashes after the first durable write, restart exposes a state that never existed after a completed EXEC response.

v0.7 writes:

```text
MULTI
SET a 1
SET b 2
EXEC
```

AOF replay is a small state machine:

```text
outside frame
  MULTI → start buffer
  normal command → apply

inside frame
  normal command → buffer only
  EXEC → apply buffered commands in order
  EOF/truncated tail → drop buffer
```

The framing markers are persistence protocol, not normal Store commands. Server replay strips the markers before sending buffered commands through the existing command handlers.

AOF rewrite still compacts database state. If an EXEC happens after rewrite buffering begins, the exact transaction frame is captured in the rewrite buffer.

## 7. Memory pressure inside EXEC

Each queued command still uses v0.6 memory-policy semantics. A command rejected by `noeviction` returns an error element and its own attempted mutation is rolled back. EXEC itself does not roll back successful sibling commands.

If an eviction policy removes keys during a queued mutation, reconstruction `DEL` commands are collected into the same transaction AOF frame. WATCH versions for those victims are advanced as runtime coordination metadata.

## 8. Pub/Sub is not a data type

A tempting design is to store channels in the database Store. That would incorrectly imply persistence, TTL, snapshots and replication semantics for subscriptions.

Instead:

```text
pubSubHub
  channel → set(clientSession)
```

`SUBSCRIBE` adds the session to the hub and to its own subscription set. `PUBLISH` copies the current targets while holding the hub lock, releases the lock, then pushes the message to each session.

The message is a live RESP2 array:

```text
[message, channel, payload]
```

There is no stored message history.

## 9. Connection role transition

A connection with at least one subscription is in subscribed mode.

```text
normal client
    ↓ SUBSCRIBE
subscribed client
    ↓ UNSUBSCRIBE last channel
normal client
```

In this version subscribed mode permits only SUBSCRIBE, UNSUBSCRIBE and PING. This makes the role transition explicit rather than hiding it in command handlers.

Disconnect cleanup calls `unsubscribeAll`, preventing dead sessions from remaining channel members.

## 10. Legacy versus reactor delivery

### Legacy

A connection goroutine normally writes request replies, while another publisher goroutine may push to it. A per-connection write mutex prevents the two RESP frames from interleaving bytes.

### Reactor

The event loop owns client buffers. A Pub/Sub push encodes the RESP value, appends it to the subscriber's output buffer and enables writable epoll interest. The same output/backpressure machinery as normal replies is reused.

The session layer therefore stays transport-independent while each transport preserves its own I/O ownership rule.

## 11. Persistence and runtime-state separation

Persisted:

```text
keys
logical values
absolute expiry deadlines
successful logical mutation history
transaction frame boundaries needed for crash replay
```

Not persisted:

```text
MULTI queue before EXEC
WATCH tokens/versions
subscriptions
Pub/Sub messages
LRU/LFU access heat
socket state
```

The distinction is essential before replication: v0.8 must replicate canonical database mutation order, not client-local control state.

## 12. Testing model

Direct session tests prove transaction semantics without sockets. Network tests then run transaction pipelines and Pub/Sub through both legacy and Linux reactor engines. Persistence tests prove complete transaction replay and truncated-frame discard. `make session-demo` gives an observable walkthrough.

```bash
make session-demo
```

Expected concepts:

```text
WATCH + intervening write → EXEC nil-array
MULTI commands           → QUEUED
EXEC                     → ordered result array
SUBSCRIBE                 → connection role change
PUBLISH                   → live message push
```

## 13. Non-goals

- no SQL-style rollback;
- no PUBLISH queueing inside MULTI in this teaching version;
- no durable Pub/Sub history;
- no Streams;
- no replication yet;
- no Sentinel/election;
- no Cluster slots.

## 14. Next architectural layer

v0.8 uses the ordering boundaries built so far to teach replication:

```text
logical base snapshot S0
       +
ordered mutations after S0
       =
replica convergence
```

The next difficult boundary is reconnect: a replica presents identity + offset; the primary either serves the missing suffix from a bounded backlog or falls back to a new full synchronization.
