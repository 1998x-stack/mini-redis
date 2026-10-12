# Architecture

## 1. Goal

mini-redis is a teaching implementation, not a production Redis replacement. The architecture deliberately separates *protocol*, *command semantics*, *storage*, *persistence*, and *transport* so each concept can be replaced independently in later versions.

```text
redis-cli / client
       |
       | TCP + RESP2
       v
+--------------------+
| server.Server      |  transport/lifecycle
+---------+----------+
          |
          v
+--------------------+
| command.Registry   |  parse command name, arity, metadata
+---------+----------+
          |
          v
+--------------------+
| store.Store        |  stable data-engine interface
+----+----------+----+
     |          |
     |          +--------> expiration metadata
     v
  data map

Mutating successful commands
          |
          v
+--------------------+
| AOF                |  RESP command log
+--------------------+
```

## 2. Why `store.Store` exists in v0.1

A tiny demo could let command handlers directly access `map[string]string`. We intentionally do not. Future versions need richer object types, snapshots, or sharding. Command handlers depend only on the behavior they need.

`ObjectType` also exists from day one. v0.1 has only `ObjectString`; v0.3 can add List/Hash/Set/ZSet without changing the transport or AOF grammar.

## 3. Expiration model

Two mechanisms work together:

1. **Lazy expiration** — every key access checks its deadline and removes stale state.
2. **Active expiration** — a periodic cycle samples TTL entries and removes expired keys that no client reads again.

The current active cycle is intentionally simple. A later version can implement Redis-like adaptive sampling without changing command APIs.

## 4. Atomicity model

The storage implementation uses a mutex, so each store operation is atomic within the Go process. This differs from classic Redis' mostly single-threaded command execution model, and that difference is intentional in v0.1: it lets the first version focus on semantics.

v0.2 is where the transport/execution model should become a real event loop. At that point, the learning exercise is to compare:

- goroutine-per-connection + mutex-protected store
- one command execution loop + nonblocking I/O

## 5. AOF design

AOF stores commands in RESP2, not a custom text format. This yields a useful invariant:

> The same parser that accepts network requests can replay persistence logs.

Successful mutating commands are appended before the success reply is sent. Relative `SET ... EX/PX` and `EXPIRE` operations are rewritten into an absolute `PEXPIREAT` deadline in AOF, so process downtime never restarts a relative TTL. `appendfsync` controls when dirty page-cache data is flushed to durable storage.

Policies:

- `always`: fsync after each command — strongest durability, slowest.
- `everysec`: write each command, fsync roughly once per second — default learning mode.
- `no`: rely on the OS — fastest, weakest explicit durability.

## 6. Known v0.1 simplifications

- one goroutine per TCP connection, not epoll/kqueue.
- one database only.
- strings only.
- no transactions, replication, RDB, cluster, ACL, eviction, pub/sub.
- `KEYS` uses a full scan.
- active expiration sampling is deterministic map iteration rather than Redis' adaptive algorithm.
- AOF tail recovery ignores a truncated final command but has no rewrite/compaction yet.

These are explicit roadmap boundaries, not accidental omissions.
