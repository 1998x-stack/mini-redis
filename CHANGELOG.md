# Changelog

## v0.3.0

Architecture milestone: make Redis' central dictionary and object encoding visible.

- add generic educational `internal/dict` with explicit power-of-two buckets and separate chaining;
- add injectable hash-function boundary and deterministic FNV-1a reference hasher;
- add `ht[0]` / `ht[1]` resize state and explicit `rehashIndex`;
- add bounded entry-level incremental rehash: one work unit moves at most one entry or skips one empty bucket;
- add grow/shrink policy and load-factor tracking through dictionary stats;
- replace keyspace and expiry Go maps with independent Dict instances;
- add opportunistic rehash on dictionary operations and timer-driven rehash maintenance;
- add `-rehash-work-per-cycle` policy;
- evolve `store.Object` into logical `Type` + physical `Encoding` + payload;
- add raw and canonical-int String encodings;
- add `OBJECT ENCODING` teaching command;
- add forced-collision `make rehash-demo`;
- add dictionary benchmarks and randomized reference-model tests;
- verify AOF replay across multiple dictionary resizes;
- preserve v0.2 reactor/legacy networking and v0.1 TTL/AOF semantics.

## v0.2.0

Architecture milestone: replace one blocking execution context per client with a readiness-driven event loop.

- add Poller abstraction + Linux epoll implementation;
- add nonblocking listener/client fds;
- add per-client input/output buffers;
- add incremental RESP parsing, pipelining and partial writes;
- add output/input buffer ceilings and slow-client isolation;
- move active expiry into reactor time events;
- add idle cleanup and wake-pipe shutdown;
- retain `legacy` network engine for controlled comparison;
- add dual-engine smoke tests and comparison benchmark harness.

## v0.1.0

Initial educational Redis vertical slice.

- RESP2 request parsing and response encoding;
- table-driven command registry;
- String commands and TTL;
- lazy + active expiration;
- RESP-formatted AOF with replay and fsync policies;
- relative TTL persisted as absolute deadline;
- goroutine-per-client reference server.
