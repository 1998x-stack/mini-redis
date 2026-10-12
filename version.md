# mini-redis v0.1 — Core Request/State/Persistence MVP

## 1. Identity

- Integrated branch: `main`
- Historical learning snapshot: `v0.1`
- Version label: `v0.1.0`
- Previous version: none — repository bootstrap
- Status: implemented and validated

This is the first coherent mini-redis learning milestone. The `v0.1` branch is intended to remain a historical snapshot once created from this validated state.

---

## 2. Version theme

**Make one Redis command visible from network bytes to memory mutation to persistent replay.**

The core trace is:

```text
redis-cli
   ↓ TCP
RESP2 bytes
   ↓
RESP Reader
   ↓
argv []string
   ↓
Command Registry
   ↓
Command Handler
   ↓
Store
   ├── value
   └── expiry deadline
   ↓
AOF command log
   ↓
RESP reply
```

---

## 3. What changed relative to the previous version, and why?

There is no previous version. v0.1 establishes the stable conceptual boundaries that later versions will evolve independently:

1. protocol parsing is separate from command semantics;
2. command dispatch is table-driven;
3. handlers depend on a `store.Store` contract rather than a concrete map;
4. expiration metadata is separate from values;
5. persistence reuses RESP command representation;
6. server transport is isolated so v0.2 can replace goroutine-per-client networking with an event loop without rewriting command/storage semantics.

The goal is not feature quantity. The goal is a complete and readable vertical slice.

---

## 4. Mechanisms added

### RESP2

- request arrays of bulk strings;
- response simple strings;
- errors;
- integers;
- bulk strings;
- null bulk response;
- command encoding reused by AOF.

### Command registry

Each command owns metadata:

- canonical name;
- arity rule;
- mutating/non-mutating flag;
- handler;
- optional AOF rewrite behavior;
- summary.

### Supported commands

- `PING`
- `ECHO`
- `SET` with optional `EX` / `PX`
- `GET`
- `DEL`
- `EXISTS`
- `INCR`
- `INCRBY`
- `EXPIRE`
- `PEXPIREAT`
- `TTL`
- `KEYS`
- `DBSIZE`
- `FLUSHDB`

### Storage

- `store.Store` interface;
- `ObjectType` / `Object` boundary;
- string objects;
- in-memory implementation;
- mutex-protected operations;
- integer increment with overflow/error handling.

### Expiration

- separate expiry metadata;
- lazy expiration on access;
- periodic active expiration;
- TTL tri-state: missing / persistent / expiring.

### AOF

- RESP-formatted mutation log;
- replay using normal command execution;
- append batches;
- `always`, `everysec`, `no` fsync modes;
- truncated-tail tolerance;
- relative TTL persistence rewritten to absolute deadline commands.

### Server

- TCP listener;
- one goroutine per client;
- request loop;
- optional idle timeout;
- active-expiry goroutine;
- graceful shutdown.

---

## 5. Important persistence invariant

Persisting this literally is wrong across downtime:

```text
SET session abc EX 10
```

If replayed five seconds later with another relative 10-second TTL, the key lives too long.

v0.1 therefore rewrites relative expiry into an absolute deadline for AOF, conceptually:

```text
SET session abc
PEXPIREAT session <absolute-unix-ms>
```

The learning point is broader than Redis: persistence must record enough information to preserve **semantics across time**, not merely re-run superficially similar API calls.

---

## 6. Mechanisms intentionally unchanged / hidden

v0.1 deliberately uses:

- Go `map` instead of a Redis-like custom dictionary;
- one goroutine per TCP connection instead of epoll/kqueue;
- a mutex-protected store instead of classic single-thread command execution;
- strings only;
- simple expiry sampling;
- direct AOF growth without rewrite;
- one logical database.

These are baselines, not forgotten work.

---

## 7. Important source files

Read in this order:

```text
internal/resp/reader.go
internal/resp/writer.go
        ↓
internal/command/registry.go
internal/command/core.go
        ↓
internal/store/store.go
internal/store/versionedmemory/memory.go
        ↓
internal/aof/aof.go
        ↓
internal/server/server.go
        ↓
cmd/mini-redis-server/main.go
```

Supporting docs:

- `Agents.md` — engineering contract;
- `Plan.md` — canonical future roadmap;
- `docs/ARCHITECTURE.md` — current implementation reasoning.

---

## 8. Tests and validation

Subsystem tests cover:

- RESP parsing/encoding;
- command execution;
- store and expiration behavior;
- AOF append/replay;
- server request path.

Validated baseline commands:

```bash
gofmt -d .
go vet ./...
go test ./...
go test -race ./...
go build ./cmd/mini-redis-server
```

When `redis-cli` is available:

```bash
make smoke
```

---

## 9. Learning goals

After reading and running v0.1, a learner should be able to explain:

1. how RESP2 converts bytes into a command vector;
2. why command dispatch benefits from metadata and registration;
3. why storage should be behind an interface before richer object types exist;
4. why expiration needs both lazy and active cleanup;
5. what atomic `INCR` means in this implementation;
6. why an in-memory server still needs durability policy;
7. the difference between `write` and `fsync`;
8. why AOF can reconstruct state by replaying commands;
9. why relative TTL persistence needs special handling;
10. exactly which part v0.2 will replace to teach Redis' event loop.

---

## 10. Non-goals

- production deployment;
- full Redis compatibility;
- event-loop networking;
- custom hash table / incremental rehash;
- List/Hash/Set/ZSet;
- compact encodings;
- eviction;
- RDB;
- AOF rewrite;
- transactions;
- Pub/Sub;
- replication;
- Sentinel;
- Cluster;
- RESP3.

---

## 11. Remaining work in this version

No planned feature work. Only fixes that preserve v0.1's learning scope should land on the frozen `v0.1` branch.

`main` may evolve into v0.2 according to `Plan.md`.

---

## 12. Next-version direction

v0.2 replaces the networking model while preserving command/storage semantics:

```text
v0.1
blocking connection goroutines
        ↓
        ↓ replace transport/runtime only
        ↓
v0.2
nonblocking sockets
+ epoll Poller
+ per-client buffers
+ partial reads/writes
+ pipelining
+ backpressure
+ event-loop timers
```

The point of v0.2 is not “more throughput” by itself. The point is to make the Redis reactor mental model explicit and measurable.
