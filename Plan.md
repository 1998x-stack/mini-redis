# mini-redis — Architecture-Driven Development Plan

> This document is the canonical development plan for `main`.
>
> **Repository rule:** `main` must always represent the newest integrated, validated state. Historical version branches (`v0.x`) are educational snapshots. Active development may occur on a version branch, but validated desired behavior must be synchronized into `main` so the integration branch never intentionally lags.

---

# 1. Why this project exists

Redis can look deceptively simple: a TCP server backed by an in-memory key/value map. Its educational value comes from everything required to make that idea fast, durable, predictable and distributed.

mini-redis exists to remove one layer of that “magic” at a time.

The project should eventually make these questions answerable from code:

- what bytes does `redis-cli` actually send?
- how are protocol parsing and command semantics separated?
- why does a command table exist instead of a giant switch statement?
- how can expiry work without one timer per key?
- why are lazy and active expiration both needed?
- what does `INCR` atomicity really depend on?
- why can Redis handle many connections with an event loop?
- how does a hash table keep serving lookups while it grows?
- why does Redis separate logical type from internal encoding?
- why is a sorted set naturally represented by both a hash table and a skip list?
- what exactly does AOF persist, and what does `fsync` change?
- how can RDB snapshotting coexist with writes?
- how do approximate LRU/LFU eviction policies trade accuracy for cost?
- what state is needed for partial replication resynchronization?
- what problem does Sentinel solve that replication alone does not?
- why does Redis Cluster use hash slots and MOVED/ASK redirects?

The desired end state is a compact educational system whose subsystems can be mapped to Redis concepts without inheriting production Redis' full implementation complexity.

---

# 2. Architectural reference model

The roadmap follows dependency order rather than feature popularity.

```text
                  Client / redis-cli
                         │
                         ▼
                TCP connection lifecycle
                         │
                         ▼
                    RESP protocol
                         │
                         ▼
                  Command registry
              metadata / arity / flags
                         │
                         ▼
                  Command semantics
                         │
                         ▼
                     Store API
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
           Object       TTL         Dict
       type/encoding  deadlines   key lookup
             │                       │
             └───────────┬───────────┘
                         ▼
                    memory state
                         │
        successful mutation / snapshot point
                         │
             ┌───────────┴───────────┐
             ▼                       ▼
            AOF                     RDB
       command history          state snapshot

Later distributed layers:

   replication offsets/backlog
              │
              ▼
       replica convergence
              │
        ┌─────┴─────┐
        ▼           ▼
     Sentinel     Cluster
     failover    sharding/slots
```

The architecture deliberately makes boundaries visible before replacing them with faster implementations.

---

# 3. Repository and version model

## 3.1 `main`

`main` is the newest integrated, validated state.

Required invariants:

1. `main` never intentionally trails a validated active version branch;
2. `main/Plan.md` is the canonical future roadmap;
3. `main/Agents.md` is the canonical engineering contract;
4. version branches are learning snapshots, not independent long-lived products;
5. later mechanisms do not leak backward into frozen historical branches.

## 3.2 Version branches

```text
main  ─────────────────────────────────────────────────► newest integrated state
  │
  ├──── v0.1   request → state → persistence MVP
  │
  ├────────── v0.2   nonblocking I/O + event loop
  │
  ├──────────────── v0.3   Dict + incremental rehash + object metadata
  │
  ├────────────────────── v0.4   collections + encodings + skip list
  │
  └──────────────────────────── ...
```

Each version should introduce one dominant mental model and contain `version.md` explaining its delta.

## 3.3 Completion rule

A version is complete only after:

```text
mechanism implemented
       ↓
unit + invariant tests
       ↓
error-path tests
       ↓
integration / redis-cli demonstration
       ↓
race / recovery validation where relevant
       ↓
documentation
       ↓
version.md updated
       ↓
Agents.md constraints satisfied
       ↓
validated state represented on main
```

---

# 4. Current integrated version

# v0.1 — Core request, state, expiry and AOF loop

## Primary question

**What happens between `redis-cli SET key value` and durable observable state?**

## Mental model

```text
TCP bytes
   ↓
RESP2 parser
   ↓
[]string argv
   ↓
command.Registry
   ↓
handler
   ↓
store.Store
   ├── data
   └── expiry metadata
   ↓
successful mutation
   ↓
AOF append / fsync policy
   ↓
RESP reply
```

## Implemented mechanisms

### Protocol

- RESP2 array-of-bulk-string request parser
- simple string, error, integer, bulk string and null responses
- reusable RESP command encoder for AOF

### Command registry

- name -> command metadata
- arity validation
- mutating-command metadata
- handler dispatch
- AOF rewrite hook

### Commands

- `PING`, `ECHO`
- `SET`, `GET`
- `DEL`, `EXISTS`
- `INCR`, `INCRBY`
- `EXPIRE`, `PEXPIREAT`, `TTL`
- `KEYS`, `DBSIZE`, `FLUSHDB`

### Storage

- stable `store.Store` interface
- object type marker with v0.1 string support
- mutex-protected in-memory implementation
- atomic `IncrBy` at store-operation boundary

### Expiration

- expiration dictionary/metadata separate from values
- lazy expiration on access
- periodic active expiration
- configurable sampling limit

### Persistence

- RESP-formatted AOF
- replay through normal protocol/command execution path
- `always`, `everysec`, `no` fsync policies
- crash-tail tolerance for a truncated final command
- relative TTL rewritten to absolute `PEXPIREAT` semantics so restart does not extend expiry

### Server/runtime

- TCP server
- one goroutine per connection
- graceful close
- idle timeout option
- active-expiration background cycle

## Why v0.1 intentionally uses Go's map and goroutines

The first version exposes semantic boundaries before recreating Redis internals. If v0.1 simultaneously implemented epoll, custom hash tables, compact encodings and persistence, each mechanism would hide the others.

The simplifications are deliberate baselines for later architectural diffs.

## v0.1 explicit non-goals

- no epoll/kqueue reactor;
- no custom Redis dictionary;
- no List/Hash/Set/ZSet;
- no object encodings;
- no RDB;
- no AOF rewrite;
- no eviction;
- no transactions;
- no Pub/Sub;
- no replication/Sentinel/Cluster;
- no claim of production Redis compatibility.

---

# 5. Planned core versions

# v0.2 — Nonblocking networking and event loop

## Why v0.2 follows v0.1

v0.1 has correct enough request semantics, so the transport can now be replaced without changing the command/store mental model. This makes the event-loop architecture visible as a controlled before/after experiment.

## Primary question

**Why can Redis serve many concurrent sockets without one blocking thread per connection?**

## Target architecture

```text
                  listening socket
                        │
                        ▼
                   Poller (epoll)
                        │
       ┌────────────────┼─────────────────┐
       ▼                ▼                 ▼
   client A          client B          client C
 input/output       input/output       input/output
   buffers            buffers            buffers
       │                │                 │
       └────────────────┼─────────────────┘
                        ▼
                    Event loop
              read → parse → execute
                  → queue reply
                        │
                        ▼
              writable readiness
```

## Required implementation

### v0.2.1 Poller boundary

- `Poller` abstraction
- Linux epoll implementation
- readable/writable/error readiness
- fd/client registration lifecycle

### v0.2.2 Nonblocking clients

- nonblocking listener/client sockets
- per-client input buffer
- per-client output buffer
- partial request handling
- pipelined commands
- partial writes
- writable-event registration only when output exists

### v0.2.3 Event loop

- deterministic event-loop ownership
- accept/read/write phases
- timer hook for active expiration
- graceful shutdown
- client cleanup on protocol/network errors

### v0.2.4 Backpressure

- configurable output-buffer limit
- slow-client behavior
- no unbounded output accumulation

### v0.2.5 Comparative benchmark

Compare v0.1 goroutine-per-client vs v0.2 reactor for:

- idle connection count;
- memory footprint;
- simple GET/SET throughput;
- slow-client isolation.

## Critical invariants

- one partial client frame cannot corrupt another client's parser state;
- a slow writer cannot block unrelated clients;
- pipelined replies preserve command order;
- transport refactor does not change command semantics.

## Non-goals

- no I/O thread pool yet;
- no TLS;
- no custom allocator;
- no collection data types.

---

# v0.3 — Redis-like dictionary, incremental rehash and object metadata

## Why v0.3 follows v0.2

Once connection scheduling is explicit, the next hidden “magic” is the in-memory dictionary itself. Go's map currently hides hashing, collisions, resizing and rehash cost.

## Primary question

**How can a hash table resize without pausing the server for one giant rehash?**

## Target dictionary model

```text
Dictionary
   │
   ├── ht[0]  current table
   └── ht[1]  target table during resize

rehash_idx = -1         no rehash
rehash_idx >= 0         migrate a few buckets per operation/tick

lookup during rehash:
   search ht[0]
      ↓ miss
   search ht[1]
```

## Required implementation

### v0.3.1 Dictionary

- explicit buckets
- collision chains or another documented educational strategy
- hash function boundary
- insert/find/delete
- load factor tracking
- grow/shrink trigger

### v0.3.2 Incremental rehash

- two-table state
- bounded bucket migration per operation
- background/timer rehash step
- lookup/delete correctness while migrating
- iterator policy documented explicitly

### v0.3.3 String representation

Introduce an SDS-inspired educational dynamic string abstraction if it clarifies:

- length vs capacity;
- binary-safe storage;
- append growth;
- avoiding repeated `strlen`-style work.

Do not copy Redis SDS source.

### v0.3.4 Redis object metadata

Evolve `store.Object` into a clear logical-type / physical-encoding model:

```text
Object
  type      = string/list/hash/set/zset
  encoding  = raw/int/...
  payload   = encoding-specific data
```

Initially only string encodings need to work.

## Critical invariants

- no key disappears during rehash;
- updates do not create duplicate logical keys across both tables;
- delete works regardless of which table owns the key;
- rehash work per command remains bounded;
- external String command behavior remains stable.

---

# v0.4 — Core Redis data types and internal encodings

## Why v0.4 follows the dictionary/object model

Collections should be implemented after logical type and physical encoding are separate concepts. Otherwise command handlers become coupled to one representation and later compact encodings require rewrites.

## Primary question

**Why can one Redis logical type use multiple internal representations?**

## Required logical types

### List

Commands:

- `LPUSH`, `RPUSH`
- `LPOP`, `RPOP`
- `LLEN`
- `LRANGE`

Educational representation progression:

- simple reference list/deque;
- quicklist/listpack-inspired compact representation or a simplified equivalent.

### Hash

Commands:

- `HSET`, `HGET`
- `HDEL`
- `HLEN`
- `HGETALL`

Representations:

- compact small-hash encoding;
- dictionary encoding after threshold.

### Set

Commands:

- `SADD`, `SREM`
- `SISMEMBER`
- `SMEMBERS`
- `SCARD`

Representations:

- integer-set-inspired encoding for suitable small integer sets;
- dictionary encoding otherwise.

### Sorted Set

Commands:

- `ZADD`
- `ZSCORE`
- `ZRANK`
- `ZRANGE`
- `ZREM`

Core learning representation:

```text
member -> score dictionary
          +
score/member ordered skip list
```

## Required semantics

- `WRONGTYPE` errors;
- explicit encoding thresholds;
- representation conversion tests;
- same logical results before/after conversion.

## Critical learning target

A logical API is stable even when internal storage changes based on size/content.

---

# v0.5 — Persistence depth: RDB and AOF rewrite

## Why v0.5 follows richer in-memory state

Persistence becomes more interesting once multiple object types and encodings exist. The version should teach snapshot vs command-log trade-offs rather than only file serialization.

## Primary question

**How does an in-memory database trade restart speed, write cost and durability?**

## Required implementation

### RDB-like snapshot

- deterministic educational snapshot format
- type + expiry metadata
- checksum/corruption detection
- save/load
- atomic temp-file replacement

### Snapshot execution model

Stage 1 reference:

- blocking snapshot for correctness.

Stage 2 educational production concept:

- Linux fork/COW experiment or clearly isolated simulation/documentation;
- explain parent/child memory behavior and write amplification.

### AOF rewrite

- rebuild minimal command history from current logical state;
- capture mutations while rewrite is in progress;
- atomic replacement;
- restart correctness.

### Startup policy

- clearly defined AOF vs RDB source precedence;
- corrupted-file behavior;
- explicit failure rather than silent data loss.

## Critical invariants

- snapshot/rewrite never publishes a partially written final file;
- TTL remains an absolute deadline through restart;
- logical state after reload matches pre-save state;
- rewrite reduces redundant history without changing state.

---

# v0.6 — Expiration, memory limits and eviction policies

## Why this version exists

v0.1 demonstrates expiration semantics but intentionally hides production-grade memory lifecycle policy. Once memory structures and persistence are mature, the project can reason about what happens under memory pressure.

## Primary questions

- why not scan every TTL key continuously?
- how does Redis approximate LRU/LFU cheaply?
- what is the difference between expiration and eviction?

## Required implementation

### Active expiry refinement

- sampled expiry cycle
- time budget
- continue scanning when stale density is high
- metrics for expired vs sampled keys

### Memory accounting

- educational approximate object memory accounting
- configurable `maxmemory`

### Eviction policies

At minimum:

- no-eviction
- allkeys-random
- volatile-random
- allkeys-LRU approximation
- allkeys-LFU approximation

Optional volatile variants may follow.

### LRU/LFU metadata

- bounded metadata cost;
- sampled victim selection;
- tests showing approximation rather than pretending exactness.

## Critical distinction

```text
expiration: key becomes invalid because time passed

eviction:   valid key is removed because memory policy requires space
```

---

# v0.7 — Transactions, optimistic locking and Pub/Sub

## Why this follows the execution model

Transactions only make sense once command execution ordering is explicit. Pub/Sub is a useful server-side fan-out feature that exercises client state without yet requiring distributed topology.

## Transactions

Required:

- `MULTI`
- command queueing
- `EXEC`
- `DISCARD`
- `WATCH` / `UNWATCH`
- per-key versioning or equivalent optimistic-lock mechanism

Learning target:

Redis transactions queue commands and execute them as one uninterrupted batch; they are not SQL-style rollback transactions.

## Pub/Sub

Required subset:

- `SUBSCRIBE`
- `UNSUBSCRIBE`
- `PUBLISH`
- subscribed-client mode constraints

Later RESP3 behavior may relax some RESP2 subscribed-client restrictions.

---

# v0.8 — Replication and partial resynchronization

## Why replication comes before HA and Cluster

Sentinel and Cluster both depend on nodes already being able to replicate state and reason about replication progress.

## Primary question

**How does a replica become and remain an equivalent copy of a primary?**

## Target flow

```text
replica connect
     ↓
handshake
     ↓
full sync snapshot + offset
     ↓
stream mutations
     ↓
track replication offset
     ↓ disconnect/reconnect
backlog contains missing range?
     ├── yes -> partial resync
     └── no  -> full resync
```

## Required implementation

- primary/replica role
- replication ID
- global replication offset
- backlog ring buffer
- full synchronization
- ongoing command propagation
- partial resynchronization
- replica read behavior documented

## Critical invariants

- replica eventually matches primary after successful sync;
- mutation order is preserved;
- offsets monotonically advance;
- partial resync is accepted only when the backlog covers the requested range.

---

# v0.9 — Sentinel-inspired high availability

## Primary question

**Replication copies data, but who decides that a failed primary should be replaced?**

## Required educational mechanisms

- monitoring/ping state
- subjective down vs quorum-confirmed down distinction
- leader election for a failover round
- replica candidate ranking
- promotion
- remaining replicas reconfigured to the new primary
- epoch/term-like monotonic configuration version

## Scope rule

The goal is to expose failure-detection and failover coordination concepts, not implement every Sentinel command or timing edge case.

---

# v0.10 — Cluster, hash slots and redirection

## Primary question

**How can Redis shard keys across nodes while clients still address one logical key space?**

## Required implementation

### Hash slots

- 16384-slot model or a documented smaller educational constant;
- CRC16-compatible or explicitly educational hash function;
- hash tags `{...}`;
- deterministic key -> slot mapping.

### Routing

- node owns slot ranges;
- `MOVED` redirection;
- `ASK` during migration;
- client test harness that follows redirects.

### Slot migration

- migrating/importing states;
- key transfer;
- no silent dual ownership.

### Cluster metadata/gossip

- node IDs;
- topology view;
- heartbeat/gossip-like exchange;
- configuration epoch conflict resolution.

## Non-goal

Do not hide networking/distributed-state complexity behind a centralized coordinator; the learning target is distributed slot ownership.

---

# v0.11 — RESP3 and richer client semantics

## Goal

Teach how protocol evolution can add richer server push and typed responses without rewriting storage semantics.

Potential scope:

- RESP3 map/set/push/null/boolean/double subset;
- `HELLO` protocol negotiation;
- server-push representation;
- Pub/Sub integration using push messages;
- protocol compatibility tests for RESP2 vs RESP3.

---

# v0.12 — Observability and performance engineering

## Goal

Turn the completed architecture into something that can explain its own performance.

Potential mechanisms:

- command latency histogram;
- slowlog;
- per-command counters;
- client buffer metrics;
- memory/expiry/eviction counters;
- replication lag metrics;
- `INFO`-like output;
- benchmark harness;
- CPU/memory profiling exercises;
- optional I/O-thread experiment after the single-reactor model is understood.

## Performance rule

Every optimization must state:

1. bottleneck;
2. mechanism;
3. expected trade-off;
4. benchmark method;
5. correctness invariants that must remain unchanged.

---

# 6. Cross-version invariants

These should remain stable unless a version explicitly documents a justified change.

## Protocol boundary

RESP framing is transport/protocol code, not command semantics.

## Command boundary

Command metadata/dispatch is separate from storage representation.

## Store semantics

Handlers request logical state operations; they should not know whether the backing dictionary is Go `map`, a custom incremental-rehash table, snapshot-aware storage, or a cluster-local shard.

## Persistence semantics

Persistence records logical state transitions or snapshots, not accidental in-memory pointers/implementation details.

## Expiry semantics

An expired key is logically missing even if physical reclamation has not happened yet.

## Version discipline

Later optimizations must preserve a readable explanation of the simpler mechanism they replace.

---

# 7. Core test matrix by subsystem

| Subsystem | Must prove |
|---|---|
| RESP | framing, truncation/error handling, partial reads, encoding |
| Command registry | lookup, arity, metadata, unknown commands |
| Store | logical CRUD invariants and type safety |
| TTL | lazy invisibility + active reclamation + deadline correctness |
| Event loop | isolation, partial I/O, pipelining, backpressure |
| Dict | collision correctness, resize, incremental rehash invariants |
| Encodings | representation conversion preserves logical result |
| AOF | replay, fsync policy behavior, truncated tail, rewrite |
| RDB | save/load identity, checksum/corruption, atomic replacement |
| Eviction | policy-specific victim eligibility and bounded behavior |
| Transactions | queueing, uninterrupted EXEC, WATCH invalidation |
| Replication | convergence, ordered propagation, offsets, partial sync |
| Sentinel | quorum/election/failover state transitions |
| Cluster | slot mapping, MOVED/ASK, migration ownership |

---

# 8. Suggested learning order inside each version

For every version:

```text
1. read version.md
2. read architecture boundary
3. run baseline tests
4. trace one request end-to-end
5. implement smallest reference mechanism
6. add invariant/error tests
7. integrate with redis-cli or node harness
8. measure only after correctness is visible
9. document production Redis differences
10. freeze snapshot when Definition of Done is satisfied
```

---

# 9. Current roadmap priority

The repository currently integrates **v0.1**.

Next priority:

```text
v0.2
nonblocking sockets
      ↓
epoll Poller
      ↓
per-client buffers
      ↓
partial RESP frames
      ↓
ordered pipelining
      ↓
backpressure
      ↓
comparison with v0.1 goroutine server
```

Do not jump directly to List/Hash/ZSet merely because those commands are familiar. The educational sequence intentionally exposes Redis' execution model before adding broader data-type surface area.
