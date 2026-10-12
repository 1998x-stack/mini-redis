# Changelog

## v0.1.0 — Core Redis learning loop

- RESP2 request parser and response encoder.
- Extensible command registry with metadata.
- In-memory string store behind a stable `store.Store` interface.
- `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `INCRBY`, `EXPIRE`, `TTL`, `KEYS`, `DBSIZE`, `FLUSHDB`.
- Lazy expiration plus periodic active expiration.
- RESP-encoded AOF with `always`, `everysec`, and `no` fsync policies.
- AOF replay on startup.
- Relative TTL commands are persisted with absolute deadlines to avoid TTL extension across restart.
- TCP server compatible with `redis-cli` for the implemented command subset.
- Unit tests, smoke test, Makefile, Dockerfile, architecture and roadmap docs.
