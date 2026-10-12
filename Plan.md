# mini-redis — Architecture-Driven Development Plan

> Canonical roadmap for `main`. `main` must always be the newest integrated, validated state; `v0.x` branches are frozen educational snapshots.

## 1. Why this project exists

Redis looks simple from the outside: TCP requests mutate an in-memory key/value database. The interesting architecture is everything that makes that model fast, durable, bounded and eventually distributed.

mini-redis removes one layer of magic at a time so the code can answer:

- what bytes does `redis-cli` send?
- how are protocol and command semantics separated?
- why does a command table exist?
- how can many sockets share one event loop?
- why do partial TCP reads/writes require client buffers?
- how can a hash table grow without a long pause?
- why does Redis separate logical type from physical encoding?
- why are lazy and active expiration both needed?
- what exactly does AOF persist and what does fsync change?
- how do RDB and AOF make different durability/restart trade-offs?
- how do LRU/LFU approximations bound eviction cost?
- what state makes partial replication resynchronization possible?
- what does Sentinel add beyond replication?
- why does Cluster use hash slots and MOVED/ASK?

## 2. Architectural reference model

```text
                   Client / redis-cli
                          │
                          ▼
                  TCP / event loop
                          │
                          ▼
                     RESP protocol
                          │
                          ▼
                   command registry
                          │
                          ▼
                   command semantics
                          │
                          ▼
                      Store API
                 ┌────────┼────────┐
                 ▼        ▼        ▼
              Object     TTL      Dict
                 └────────┼────────┘
                          ▼
                      memory state
                          │
             ┌────────────┴────────────┐
             ▼                         ▼
            AOF                       RDB
        command log                snapshot

later:
replication offsets/backlog
          ↓
   replica convergence
       ┌──┴──┐
       ▼     ▼
  Sentinel  Cluster
```

The roadmap follows dependency order, not command popularity.

## 3. Repository/version model

```text
main ───────────────────────────────────────────────► newest validated
  │
  ├── v0.1  request → state → persistence
  ├── v0.2  nonblocking I/O + epoll reactor
  ├── v0.3  Dict + incremental rehash + object metadata
  ├── v0.4  collections + encodings + skip list
  └── ...
```

A version is complete only after:

```text
mechanism
   ↓
unit + invariant + error-path tests
   ↓
integration / smoke
   ↓
race/recovery validation where relevant
   ↓
documentation + version.md
   ↓
validated state synchronized to main
```

## 4. Completed versions

### v0.1 — Core request/state/persistence MVP

Primary question:

> What happens between `SET key value` bytes and durable observable state?

Implemented:

- RESP2 array-of-bulk request parser;
- RESP response encoder;
- table-driven command registry;
- string Store abstraction and memory engine;
- `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `INCRBY`, `EXPIRE`, `PEXPIREAT`, `TTL`, `KEYS`, `DBSIZE`, `FLUSHDB`;
- lazy + active expiration;
- RESP-formatted AOF;
- `always` / `everysec` / `no` fsync policies;
- restart replay;
- relative TTL rewritten to absolute deadline for AOF;
- goroutine-per-client networking baseline.

Deliberate baseline choices:

- Go map hides dictionary internals;
- one goroutine/client hides reactor mechanics;
- strings only;
- no RDB/rewrite/eviction/distribution.

### v0.2 — Nonblocking networking + event loop

Primary question:

> Why can one reactor serve many sockets without one blocking thread per connection?

Architecture:

```text
                nonblocking listener
                        │
                        ▼
                       epoll
                        │
       ┌────────────────┼────────────────┐
       ▼                ▼                ▼
    client A         client B         client C
    in / out         in / out         in / out
    buffers          buffers          buffers
       └────────────────┼────────────────┘
                        ▼
                    Event Loop
          accept → read → parse → execute
                         → queue reply
                         → partial write
```

Implemented:

- `Poller` abstraction;
- Linux epoll implementation;
- nonblocking listener/accepted fds;
- single reactor loop;
- per-client input/output buffers;
- incremental RESP parsing;
- pipelining and ordered replies;
- partial writes;
- writable readiness only while output exists;
- input/output buffer ceilings;
- slow-client isolation;
- event-loop TTL timer;
- idle-client timer;
- wake-pipe shutdown;
- legacy engine retained as a controlled comparison baseline;
- dual-engine smoke + benchmark harness.

Critical invariants:

- a partial frame from client A cannot block client B;
- pipelined replies preserve command order;
- slow writers do not block unrelated clients;
- output growth is bounded per client;
- command/Store/AOF semantics are unchanged across transports.

Deliberate simplifications:

- Linux epoll only;
- level-triggered readiness;
- synchronous AOF append remains on command path;
- v0.1 mutex-protected Store remains unchanged;
- no I/O thread pool/TLS/io_uring.

## 5. v0.3 — Redis-like Dict, incremental rehash and object metadata

Why next: after scheduling is explicit, Go's `map` is the largest hidden core mechanism.

Primary question:

> How can a hash table resize while the server keeps serving commands without one stop-the-world rehash?

Target model:

```text
Dict
 ├── ht[0]  old/current table
 ├── ht[1]  target table while rehashing
 └── rehashIndex

lookup during rehash:
  search ht[0]
      ↓ miss
  search ht[1]

per command/tick:
  migrate bounded number of buckets
```

Required:

- explicit buckets;
- documented collision strategy;
- hash-function boundary;
- insert/find/delete;
- load-factor tracking;
- grow/shrink triggers;
- two-table rehash state;
- bounded rehash steps;
- lookup/update/delete correctness while migrating;
- explicit iterator policy;
- event-loop timer/opportunistic rehash steps;
- Redis-like object metadata separating logical type and encoding;
- optional SDS-inspired string representation if it clarifies length/capacity/binary safety.

Invariant tests must prove:

- no key disappears during rehash;
- one logical key never exists as conflicting duplicates in two tables;
- updates find the correct owner;
- deletes work in either table;
- each rehash step has bounded work;
- existing string command behavior remains unchanged.

Non-goals:

- no collection commands yet;
- no compact collection encodings;
- no persistence redesign.

## 6. v0.4 — Core data types and internal encodings

Primary question:

> Why can one Redis logical type use multiple physical representations?

Implement:

- List: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`;
- Hash: `HSET`, `HGET`, `HDEL`, `HLEN`, `HGETALL`;
- Set: `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`;
- ZSet: `ZADD`, `ZSCORE`, `ZRANK`, `ZRANGE`, `ZREM`;
- `WRONGTYPE` semantics;
- explicit type/encoding conversion thresholds;
- same logical results before/after representation conversion;
- ZSet educational model: member→score dict + ordered skip list.

## 7. v0.5 — Persistence depth: RDB + AOF rewrite

Primary question:

> How does an in-memory database trade write cost, restart speed and durability?

Implement:

- deterministic educational RDB format;
- type + absolute-expiry metadata;
- checksum/corruption detection;
- blocking snapshot reference path;
- atomic temp-file replacement;
- AOF rewrite from current logical state;
- mutations captured during rewrite;
- atomic AOF replacement;
- startup precedence and corruption policy;
- fork/COW as an isolated experiment/documented production concept after correctness is clear.

## 8. v0.6 — Expiration, maxmemory and eviction

Primary questions:

- why not scan all TTL keys continuously?
- what differs between expiration and eviction?
- how can LRU/LFU be approximated cheaply?

Implement:

- time-budgeted adaptive active-expire cycle;
- approximate memory accounting;
- `maxmemory`;
- no-eviction;
- random policies;
- sampled LRU approximation;
- sampled LFU approximation;
- metrics showing approximation/cost.

## 9. v0.7 — Transactions + Pub/Sub

Transactions:

- `MULTI`, `EXEC`, `DISCARD`;
- `WATCH`, `UNWATCH`;
- queued execution;
- optimistic key versioning.

Learning target: Redis transactions execute an uninterrupted queued batch; they are not SQL rollback transactions.

Pub/Sub:

- `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`;
- subscribed-client state;
- output-buffer/backpressure interaction.

## 10. v0.8 — Replication + partial resynchronization

Primary question:

> What state lets a replica catch up without receiving the whole dataset again?

Implement:

- primary/replica roles;
- full initial synchronization;
- replication offset;
- backlog ring buffer;
- command propagation;
- reconnect + partial resync when backlog still covers the gap;
- fallback to full sync otherwise;
- convergence tests.

## 11. v0.9 — Sentinel-inspired high availability

Implement educational versions of:

- health monitoring;
- subjective vs objective failure;
- quorum;
- leader/election decision;
- replica promotion;
- topology reconfiguration;
- split-brain limitations documented explicitly.

## 12. v0.10 — Cluster / hash slots

Implement:

- 16384-slot model;
- deterministic key→slot mapping;
- slot ownership;
- `MOVED`;
- migration/importing state;
- `ASK` semantics;
- basic node gossip/topology exchange;
- resharding experiment.

## 13. v0.11 — RESP3 and richer client semantics

Add RESP3 only after server/runtime/data/distributed concepts are stable:

- richer scalar/container types;
- push-style messages where useful;
- protocol negotiation;
- compatibility tests.

## 14. v0.12 — Observability and performance engineering

Add:

- command latency stats;
- connection/event-loop counters;
- expiration/eviction metrics;
- AOF/RDB timing;
- replication lag/offset metrics;
- CPU/memory profiling examples;
- repeatable benchmark methodology;
- explicit comparison against prior reference implementations.

Performance is a learning result only when the mechanism causing the change is explained.

## 15. Version sequencing invariant

The intended dependency chain is:

```text
request semantics
   ↓
event-loop scheduling
   ↓
dictionary internals
   ↓
objects/collections/encodings
   ↓
persistence depth
   ↓
memory policy
   ↓
transactions/client fan-out
   ↓
replication
   ↓
failover
   ↓
sharding
   ↓
protocol/observability refinement
```

Do not skip ahead simply because a later feature is more visible. The learning value comes from being able to explain why each later subsystem needs the earlier one.

## 16. Current directive

Current integrated version: **v0.2**.

Next active version should be **v0.3** and must be created from latest validated `main`.

Do not begin v0.4 collection encodings until v0.3 can demonstrate correct bounded incremental rehash under the existing event loop.
