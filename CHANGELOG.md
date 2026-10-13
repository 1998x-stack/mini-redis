# Changelog

## v0.9.0

Architecture milestone: make monitor-local failure detection, quorum-backed failover, runtime replica promotion and replication-topology repair explicit.

- add `internal/sentinel` as a control plane separate from the v0.8 replication data plane;
- track per-monitor last success/failure, failure start and subjective-down state;
- add SDOWN after one monitor's configured `down-after` interval;
- require configured SDOWN quorum before declaring ODOWN and starting failover;
- add monotonically increasing in-memory failover epoch and deterministic teaching leader selection;
- require quorum reachability and quorum replica observation before a node is promotion-eligible;
- rank eligible candidates by reachable votes, replication offset and stable node-ID tie-break;
- add `ServerNode` adapter with real client-plane TCP PING health probes;
- refactor server replication role from startup-only configuration to runtime-managed topology state;
- add `PromoteToPrimary` with fresh replication identity, backlog and listener;
- add `FollowPrimary` for runtime demotion/reconfiguration to a new upstream;
- cancel obsolete role-specific replication goroutines/connections when topology changes;
- keep runtime role transitions inside `Server.persistenceMu` logical-order boundary;
- reset repl-id/offset when following a different primary so topology changes safely re-enter FULLRESYNC;
- reconfigure reachable surviving replicas after promotion;
- reconfigure a returning old primary as a read-only replica of the chosen primary;
- explicitly reject dynamic demotion with active local AOF or maxmemory policy in v0.9;
- add asymmetric monitor-fault tests distinguishing isolated SDOWN from quorum ODOWN;
- add candidate freshness/reachability tests and repeated-Tick epoch tests;
- add real three-data-node failover/rejoin integration under `go test -race`;
- add `make sentinel-demo` showing SDOWN → ODOWN → election → promotion → repair;
- explicitly document that v0.9 has no fencing/lease/consensus guarantee and can permit temporary split brain during network partition.

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
- separate queue-time validation errors (`EXECABORT`) from execution-time errors;
- add key write versions, global FLUSH epoch and existence checks for optimistic WATCH conflict detection;
- hold `Server.persistenceMu` across WATCH validation and complete queued execution;
- add AOF transaction framing and crash-tail replay protection;
- add `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` and connection cleanup;
- integrate pushes with legacy writer locking and reactor output buffers;
- add `make session-demo`.

## v0.6.0

Architecture milestone: make expiration versus memory-pressure eviction and approximate victim selection explicit.

- add deterministic logical memory accounting;
- add `noeviction`, `allkeys-random`, sampled LRU and sampled LFU;
- separate expiration from eviction accounting;
- add noeviction logical rollback;
- persist eviction-generated `DEL`;
- add `make eviction-demo`.

## v0.5.0

Architecture milestone: make logical state serialization, snapshot persistence, AOF compaction and crash-safe file replacement explicit.

- add `SnapshotEntry` / `Snapshotter` logical persistence boundary;
- add deterministic RDB-like `MRDB` snapshots;
- persist TTLs as absolute deadlines;
- add checksum/framing validation and crash-safe publication;
- add AOF rewrite + post-snapshot mutation buffering;
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
