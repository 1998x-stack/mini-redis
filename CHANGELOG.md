# Changelog

## v0.8.0

Architecture milestone: make primary/replica synchronization, canonical replication offsets, bounded backlog and partial resynchronization explicit.

- add explicit `standalone`, `primary` and `replica` roles and replication configuration;
- add a dedicated educational replication TCP plane beside normal client transport;
- add random primary replication identity and monotonically increasing canonical mutation byte offset;
- couple primary replication publication to the existing `Server.persistenceMu` logical-order boundary;
- add logical `SnapshotEntry[]` full synchronization without serializing private Dict/Object layouts;
- capture snapshot, base offset and live subscriber under one mutation-ordering boundary to prevent snapshot→stream gaps;
- add frame-aware bounded backlog with whole-frame eviction and exact resume boundaries;
- add `PSYNC`, `FULLRESYNC` and `CONTINUE` teaching handshake semantics;
- add partial resync from retained suffix and automatic full-sync fallback after identity mismatch/backlog wrap;
- preserve catch-up frame boundaries instead of flattening multiple logical frames;
- advance replica offset only after a complete frame applies successfully;
- propagate absolute `PEXPIREAT` reconstruction commands and eviction-generated `DEL` through the canonical feed;
- preserve successful EXEC mutations as one `MULTI ... EXEC` replication frame;
- add replica-side READONLY client gate while allowing the dedicated replication apply path;
- intentionally require v0.8 replicas to disable local AOF/maxmemory policy to avoid independent durability/eviction semantics;
- add replication full/live/partial/fallback/transaction integration tests under the race detector;
- add sync counters and a diagnostic disconnect hook for deterministic learning tests;
- add `make replication-demo` showing FULLRESYNC → live propagation → disconnect → CONTINUE → READONLY.

## v0.7.0

Architecture milestone: make per-connection transaction state, optimistic WATCH conflicts, EXEC persistence framing and live Pub/Sub explicit.

- add `clientSession` as the connection-local owner of MULTI queue, WATCH tokens and subscriptions;
- add `MULTI`, `EXEC`, `DISCARD`, `WATCH` and `UNWATCH` session commands;
- separate queue-time validation errors (`EXECABORT`) from execution-time errors (returned inside EXEC result without rollback);
- add key write versions, global FLUSH epoch and existence checks for optimistic WATCH conflict detection;
- detect write-then-write-back conflicts and watched-key expiration;
- add RESP2 null-array support for WATCH-aborted EXEC;
- hold `Server.persistenceMu` across WATCH validation and complete queued execution so other logical mutations cannot interleave;
- serialize legacy expiration maintenance and RDB snapshot capture with the same EXEC boundary;
- add AOF transaction framing (`MULTI ... EXEC`) and replay buffering so crash-truncated final transactions are discarded as a unit;
- preserve eviction-generated reconstruction deletes inside transaction persistence;
- add in-memory `pubSubHub` with `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` and connection cleanup;
- add subscribed connection mode and RESP2 live message arrays;
- integrate pushes with legacy writer locking and reactor output-buffer/writable-interest machinery;
- add direct session tests, transaction AOF restart/truncation tests and both-engine network tests;
- add `make session-demo`;
- add CI concurrency so obsolete pushes on one branch do not consume validation capacity.

## v0.6.0

Architecture milestone: make expiration versus memory-pressure eviction and approximate victim selection explicit.

- add deterministic logical memory accounting at the Store/SnapshotEntry boundary;
- add `maxmemory`, `noeviction`, `allkeys-random`, sampled LRU and sampled LFU;
- add runtime access metadata without persisting LRU/LFU heat;
- remove expired keys before eviction and count expiration separately;
- add noeviction logical rollback for rejected memory-growing commands;
- keep removal-only commands usable while already above the limit;
- persist eviction-generated `DEL` with the acknowledged mutation;
- add deterministic policy tests and `make eviction-demo`.

## v0.5.0

Architecture milestone: make logical state serialization, snapshot persistence, AOF compaction and crash-safe file replacement explicit.

- add `SnapshotEntry` / `Snapshotter` as a stable logical persistence boundary;
- add deterministic RDB-like `MRDB` v1 snapshots for all current logical types;
- persist TTLs as absolute millisecond deadlines;
- add framing/checksum validation and crash-safe temp → fsync → rename → directory-fsync publication;
- add compact AOF rewrite and post-snapshot mutation buffering;
- define AOF-vs-RDB startup precedence and recovery tests;
- add `make persistence-demo`.

## v0.4.0

- add List, Hash, Set and Sorted Set logical types;
- add listpack/quicklist/hashtable/intset/skiplist teaching encodings;
- add collection commands, WRONGTYPE checks, TTL preservation and compact→general transitions;
- add `make encodings-demo`.

## v0.3.0

- add educational incremental Dict with explicit buckets, collisions, `ht[0]/ht[1]` and bounded rehash;
- replace keyspace/expiry Go maps with Dict;
- split logical Object type from physical encoding;
- add `OBJECT ENCODING`, demos and reference-model tests.

## v0.2.0

- add Poller abstraction + Linux epoll reactor;
- add nonblocking client buffers, pipelines, partial writes and backpressure;
- add event-loop timers and wake-pipe shutdown;
- retain legacy transport for controlled comparison.

## v0.1.0

- RESP2 request/response path;
- table-driven commands and String Store;
- TTL lazy + active expiration;
- RESP-formatted AOF, replay and fsync policies;
- goroutine-per-client reference transport.
