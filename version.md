# mini-redis v0.7 — Transactions, WATCH and Pub/Sub

## 1. Identity

- Integrated branch after validation: `main`
- Historical learning snapshot: `v0.7`
- Version label: `v0.7.0`
- Previous snapshot: `v0.6`
- Status: implemented; freeze requires exact-SHA CI on `v0.7` and then the same SHA on `main`

## 2. Primary question

**How can Redis queue commands, detect optimistic conflicts, execute a transaction as one ordered unit, and let a connection change role into a live Pub/Sub subscriber without turning transactions into database-style rollback?**

v0.7 introduces explicit per-connection state above the existing command/store stack:

```text
TCP connection
    ↓
clientSession
  ├── WATCH tokens
  ├── MULTI queue / dirty flag
  └── subscriptions
    ↓
server coordination
  ├── persistenceMu
  ├── key version table
  └── Pub/Sub hub
    ↓
command.Registry → memory policy → Store
```

Session state is not keyspace state and is never serialized into RDB/AOF.

## 3. MULTI/EXEC is queueing, not rollback

`MULTI` switches one client into queue mode. Normal registry commands are validated for command existence/arity and return `QUEUED` instead of executing. `EXEC` then runs queued commands in order while holding `Server.persistenceMu`.

Execution-time errors are returned as elements of the EXEC response and do **not** roll back earlier successful commands. This intentionally models Redis' transaction mental model rather than SQL-style rollback.

Queue-time validation errors mark the transaction dirty. A later `EXEC` returns `EXECABORT` and executes none of the queued commands.

`DISCARD` clears the queue and WATCH state.

## 4. WATCH is optimistic conflict detection

A WATCH token records:

```text
(global epoch, per-key write version, existence)
```

Successful writes to a key advance its version. `FLUSHDB` advances the global epoch. Memory-pressure evictions advance affected key versions. At EXEC, the server also rechecks key existence, so a watched key that expires between WATCH and EXEC aborts the transaction even though TTL cleanup is not represented as a normal client write.

Changing a value and later changing it back still conflicts because the write version changed. WATCH therefore detects intervening writes, not only final-value inequality.

Conflict result is RESP2 null array (`*-1`).

## 5. EXEC coordination boundary

`EXEC` holds `persistenceMu` across:

```text
WATCH validation
      ↓
command 1
command 2
...
command N
      ↓
transaction AOF append
```

Normal mutations also use this lock. v0.7 extends the same boundary to active expiration maintenance and RDB snapshot capture so the legacy maintenance goroutine and persistence observers cannot see or create a half-EXEC state.

The Store's own mutex still protects its internal structures; `persistenceMu` is the higher-level logical sequencing boundary.

## 6. Crash-safe AOF transaction framing

Persisting each EXEC mutation independently would create a restart anomaly: a crash could leave only a durable prefix of the transaction.

v0.7 therefore writes successful EXEC mutations as:

```text
MULTI
  <mutation reconstruction command 1>
  <mutation reconstruction command 2>
  ...
EXEC
```

AOF replay buffers commands inside this frame and applies them only after a complete `EXEC` marker is present. A truncated final transaction is discarded as a unit.

Read-only commands are not written to AOF. Execution-time failed commands produce no mutation command. Eviction-generated `DEL` commands remain part of the same transaction frame.

AOF rewrite continues to emit compact logical reconstruction state; post-snapshot transaction frames enter the rewrite buffer exactly as written.

## 7. Pub/Sub is connection-role state

`SUBSCRIBE channel` registers a `clientSession` in an in-memory hub. `PUBLISH channel payload` copies the current subscriber set and pushes RESP2 arrays:

```text
["message", channel, payload]
```

A subscribed connection changes role: in this teaching subset it accepts `SUBSCRIBE`, `UNSUBSCRIBE`, and `PING`; ordinary key commands are rejected until subscription count returns to zero.

Pub/Sub data is deliberately not stored in the keyspace, RDB, or AOF. There is no history or replay: subscribers receive live messages only.

The hub counts subscribed targets, not confirmed socket delivery. A disconnected session is removed from the hub during connection cleanup.

## 8. Transport integration

Both networking models share the same session semantics:

- legacy engine: each connection goroutine owns a session; a write mutex prevents push/reply byte interleaving;
- reactor engine: each `reactorClient` owns a session; Pub/Sub push queues bytes into that client's output buffer and enables writable interest.

Network tests exercise transaction pipelines and Pub/Sub fan-out on legacy and, on Linux, epoll reactor transport.

## 9. Commands introduced at the session layer

```text
WATCH key [key ...]
UNWATCH
MULTI
EXEC
DISCARD
SUBSCRIBE channel [channel ...]
UNSUBSCRIBE [channel ...]
PUBLISH channel message
```

These commands are intentionally handled above the ordinary command registry because their primary state belongs to the client connection/server coordination layer rather than the keyspace Store.

## 10. Deliberate v0.7 simplifications

- `PUBLISH` is not queued inside MULTI; attempting it marks the transaction dirty;
- `SUBSCRIBE` / `UNSUBSCRIBE` are not allowed inside MULTI;
- `UNWATCH` inside MULTI is rejected in this teaching subset;
- key write-version tracking is conservative: a successful write-shaped command may invalidate WATCH even when its final logical effect is a no-op;
- no Lua/scripts;
- no replication yet;
- no durable Pub/Sub backlog or Streams.

These restrictions keep the version focused on session state, optimistic conflict detection and transaction ordering.

## 11. Validation

The exact v0.7 candidate must pass:

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
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

Mechanism tests cover:

- queue order and `QUEUED` responses;
- queue-time error → `EXECABORT`;
- execution-time error without rollback;
- unrelated writes not aborting WATCH;
- write-then-write-back still aborting WATCH;
- TTL expiration invalidating WATCH;
- RESP2 null-array conflict reply;
- complete EXEC AOF framing and restart;
- crash-truncated transaction frames discarded as a unit;
- invalid `EXEC` AOF framing rejected;
- two-subscriber Pub/Sub fan-out;
- subscribed-mode command restriction;
- transaction and Pub/Sub protocol behavior on both network engines.

## 12. Important source files

```text
internal/server/session.go
internal/server/server.go
internal/server/legacy.go
internal/server/reactor_linux.go
internal/aof/aof.go
internal/resp/value.go
internal/command/registry.go
examples/sessions/main.go
```

## 13. Next version

v0.8 introduces replication:

```text
primary logical mutations
       ↓
replication stream + offset
       ↓
replica

reconnect:
replica offset ── backlog contains gap? ── yes → partial resync
                                 └── no  → full sync snapshot
```

Primary question: **how does a replica establish a consistent base state, continue from an ordered mutation stream, and reconnect without always copying the full database?**
