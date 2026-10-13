# Changelog

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
