# Changelog

## v0.2.0 — 2026-10-12

### Added

- `Poller` abstraction and Linux epoll implementation;
- nonblocking listener and accepted sockets;
- single reactor command/event loop;
- per-client input and output buffers;
- incremental RESP command parsing for partial reads;
- command pipelining with ordered replies;
- partial-write handling and dynamic writable interest;
- input/output client buffer limits;
- event-loop active-expiration timer hook;
- idle-client cleanup in the reactor;
- explicit `legacy` networking engine for v0.1/v0.2 comparison;
- reactor invariant/integration tests;
- networking comparison benchmark harness.

### Preserved

- command behavior and registry;
- Store interface and current memory engine;
- TTL semantics;
- AOF format, relative-TTL rewrite and replay behavior.

## v0.1.0 — 2026-10-12

Initial educational Redis vertical slice:

- RESP2 command parsing and response encoding;
- command registry and metadata;
- string key/value store;
- lazy and active expiration;
- `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `INCRBY`, `EXPIRE`, `PEXPIREAT`, `TTL`, `KEYS`, `DBSIZE`, `FLUSHDB`;
- RESP-formatted AOF with `always`, `everysec`, `no` fsync policies;
- AOF replay and absolute-deadline TTL persistence;
- goroutine-per-client TCP server baseline.
