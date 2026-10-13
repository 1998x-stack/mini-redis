# Changelog

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
- add a named server `persistenceMu` boundary around command mutation + AOF append versus snapshot + rewrite start;
- define startup precedence: AOF is authoritative when enabled; RDB is loaded only when AOF is disabled;
- retain the explicit truncated-final-AOF-command recovery policy and fail startup on corrupt/incompatible RDB;
- add `Server.SaveRDB`, `Server.RewriteAOF`, recovery/crash-window tests and `make persistence-demo`;
- keep persistence independent of quicklist, Dict bucket, intset and SkipList memory layout.

## v0.4.0

Architecture milestone: make Redis logical data types and adaptive physical encodings explicit.

- add List, Hash, Set and Sorted Set logical object types;
- add `listpack`, `quicklist`, `hashtable`, `intset` and `skiplist` teaching encodings;
- add a span-aware probabilistic SkipList with deterministic `(score, member)` ordering;
- add List commands: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`;
- add Hash commands: `HSET`, `HGET`, `HDEL`, `HLEN`, `HGETALL`;
- add Set commands: `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`;
- add ZSet commands: `ZADD`, `ZSCORE`, `ZRANK`, `ZRANGE`, `ZREM`;
- add Redis-style `WRONGTYPE` checks;
- keep collection mutations atomic inside Store and preserve TTL metadata;
- delete key + expiry when a collection becomes empty;
- add one-way compact→general encoding conversions with an educational threshold of eight entries;
- add `make encodings-demo` and collection-aware smoke tests for both reactor and legacy engines;
- keep AOF representation command-oriented and independent of physical encodings.

## v0.3.0

Architecture milestone: make Redis' central dictionary and object encoding visible.

- add generic educational `internal/dict` with explicit buckets and separate chaining;
- add `ht[0]` / `ht[1]`, load-factor resize policy and bounded entry-level incremental rehash;
- replace keyspace and expiry Go maps with independent Dict instances;
- add timer-driven + opportunistic rehash;
- evolve Object into logical `Type` + physical `Encoding` + payload;
- add raw/canonical-int String encodings and `OBJECT ENCODING`;
- add forced-collision rehash demo and randomized reference-model tests.

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
