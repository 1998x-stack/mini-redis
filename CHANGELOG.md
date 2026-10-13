# Changelog

## v0.6.0

Architecture milestone: separate time-based expiration from pressure-based eviction and make sampled cache policies explicit.

- add deterministic logical-byte memory estimation distinct from Go heap/RSS;
- add runtime `memorypolicy.Manager` above the durable Store;
- add `-maxmemory`, `-maxmemory-policy`, `-maxmemory-samples`, `-lfu-decay` and deterministic `-eviction-seed`;
- implement `noeviction`, `allkeys-random`, sampled `allkeys-lru` and sampled `allkeys-lfu`;
- add runtime last-access metadata and probabilistic LFU counters with time decay;
- clean expired keys before pressure eviction and count them separately;
- add logical snapshot rollback for rejected noeviction memory-growing commands;
- keep removal-only commands usable above a noeviction limit;
- persist selected evictions as logical AOF `DEL` commands;
- keep LRU/LFU heat outside RDB/AOF and reset it on restore;
- add deterministic policy, AOF restart and sampling-bound tests;
- add `make eviction-demo` and CI coverage.

## v0.5.0

Architecture milestone: make logical state serialization, snapshot persistence, AOF compaction and crash-safe file replacement explicit.

- add `SnapshotEntry` / `Snapshotter` as a stable logical persistence boundary independent of `Object.Encoding`;
- add deterministic RDB-like `MRDB` v1 snapshots for String/List/Hash/Set/ZSet;
- persist TTLs as absolute millisecond deadlines so downtime never extends expiration;
- add magic/version/payload-length framing and CRC32 corruption detection;
- encode ZSet scores canonically as strings so every currently valid score, including `+Inf` / `-Inf`, round-trips;
- publish RDB and rewritten AOF files through same-directory temp file, file `fsync`, atomic rename and directory `fsync`;
- add compact AOF rewrite from current logical state instead of replay history;
- buffer mutations after the rewrite snapshot boundary and append them before the replacement AOF is published;
- define startup precedence: AOF is authoritative when enabled; RDB is loaded only when AOF is disabled;
- add persistence recovery/crash-window tests and `make persistence-demo`.

## v0.4.0

- add List, Hash, Set and Sorted Set logical object types;
- add adaptive listpack/quicklist/hashtable/intset/skiplist teaching encodings;
- add span-aware SkipList, WRONGTYPE and atomic TTL-preserving collection mutation;
- add core collection commands and `make encodings-demo`.

## v0.3.0

- add explicit buckets/collision chains and bounded incremental rehash;
- replace top-level Go maps with custom Dict;
- add Object Type/Encoding and String raw/int encodings;
- add `OBJECT ENCODING` and rehash demo/tests.

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
