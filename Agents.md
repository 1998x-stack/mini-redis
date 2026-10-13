# Agents.md — mini-redis Development Contract

> Mandatory for every iteration. Read this file before changing code, tests, docs, branch scope, protocol behavior, persistence semantics or architecture.

## 1. Mission

mini-redis is an educational Redis implementation in Go. Optimize for architectural clarity, explicit invariants, readable mechanisms, end-to-end observability and version-to-version diffs that each teach one dominant idea.

```text
one version
   ↓
one primary mental model
   ↓
small explicit mechanism
   ↓
invariant tests
   ↓
end-to-end demo
```

Do not optimize for full Redis compatibility, production hardening or feature count.

## 2. Mandatory reading order

On `main`:

1. `Agents.md`
2. `Plan.md`
3. `version.md`
4. `docs/ARCHITECTURE.md`
5. relevant source/tests

Historical `v0.x` branches are frozen learning snapshots and must not absorb later mechanisms.

## 3. Branch model

`main` is always the newest validated integrated state.

```text
main ───────────────────────────────► newest validated
 ├── v0.1 request/state/AOF
 ├── v0.2 event loop
 ├── v0.3 Dict/rehash
 ├── v0.4 collections/encodings
 ├── v0.5 RDB/AOF rewrite
 ├── v0.6 maxmemory/eviction
 ├── v0.7 transactions/PubSub
 ├── v0.8 replication
 ├── v0.9 Sentinel-inspired HA
 ├── v0.10 Cluster/hash slots
 ├── v0.11 RESP3
 └── v0.12 observability/performance
```

Create a version branch from current `main`. Definition of Done requires exact-SHA CI on the version branch, fast-forwarding `main` to that same SHA, then exact-SHA CI on `main`.

## 4. Architecture direction

```text
client TCP
   ↓
RESP command parser
   ↓
connection/session state
   ├── protocol 2/3
   ├── transaction/WATCH
   └── Pub/Sub
   ↓
v0.10 cluster routing gate when applicable
   ↓
Server.persistenceMu
   ↓
command registry / Store
   ↓
logical state + canonical mutation commands
   ├── AOF/RDB
   └── v0.8 replication
          ↓
    primary / replicas
          ↑
    v0.9 Sentinel control

logical reply
   ↓
v0.11 protocol view
   ├── RESP2
   └── RESP3

all established mechanisms
   ↓
v0.12 observation points
   ↓
atomic counters + bounded histograms
   ↓
immutable snapshots / INFO / demos / profiles
```

Rules:

- protocol representation never owns database semantics;
- RESP parsing/encoding does not depend on Store internals;
- Store never owns network/failover/slot/protocol/metrics metadata;
- command handlers do not inspect private encoding payloads;
- persistence and replication serialize logical state/commands, never client protocol or metrics state;
- Sentinel monitors/reconfigures replication topology and never copies database values;
- Cluster ownership/migration remains separate from Store values;
- Cluster migration crosses nodes via `SnapshotEntry`;
- response adaptation occurs only after command and Cluster authorization;
- observability records completed/attempted mechanism work but never decides mechanism behavior.

## 5. Concurrency / atomicity

- Store mutexes own in-memory value/TTL atomicity.
- `Server.persistenceMu` is the canonical logical mutation-order boundary from v0.5 onward.
- v0.7 holds it across WATCH validation and complete EXEC.
- v0.8 uses it for mutation publication and snapshot/offset/subscriber handoff.
- v0.9 uses it across runtime replication-role transitions.
- v0.10 holds it across migration-sensitive existence observation, routing authorization and command execution.
- `ClusterNode.TransferKey` holds source `persistenceMu` from export through target acknowledgement and source delete.
- `cluster.Topology.mu` owns node/slot/epoch/migration metadata.
- v0.11 protocol mode is connection-local session state protected by `clientSession.mu`.
- transports may read protocol mode to encode bytes, but must not implement separate HELLO semantics.
- v0.12 hot-path counters use atomics; dynamic metric-label creation/snapshot copying may use their own small mutexes.
- v0.12 histograms are fixed-bucket bounded structures; no unbounded per-request samples.
- metrics locks/atomics never replace Store, persistence, replication, Sentinel or Cluster synchronization.
- never claim atomicity without naming its lock, filesystem or protocol boundary.

## 6. Established transaction / persistence / memory rules

- MULTI queues work; it does not promise SQL rollback.
- queue-time validation errors cause EXECABORT; execution-time errors remain result elements.
- WATCH is optimistic conflict detection.
- successful EXEC mutations are one AOF transaction frame and one replication frame.
- only successful logical mutations enter AOF.
- relative TTL becomes an absolute deadline across persistence/replication/migration.
- `SnapshotEntry` is the stable logical serialization boundary.
- expiration and eviction are different mechanisms.
- eviction-generated DEL belongs to durability and replication order.
- LRU/LFU heat is runtime policy state, never database state.

## 7. Replication rules — v0.8

- one primary runtime owns one repl-id and canonical byte offset;
- backlog evicts whole frames only;
- snapshot + base offset + live subscriber attachment share `persistenceMu`;
- matching repl-id + retained frame boundary permits CONTINUE; otherwise FULLRESYNC;
- replica offset advances only after complete frame application;
- replica client writes are READONLY;
- client RESP2/RESP3 mode never changes replication frames;
- v0.8 has no replica→primary ACK protocol, so a primary cannot truthfully report authoritative remote applied-offset lag.

## 8. Sentinel rules — v0.9

- SDOWN is monitor-local;
- ODOWN requires configured SDOWN quorum;
- one controller serializes failover Tick operations;
- each failover attempt receives a monotonic epoch;
- candidate must be quorum-visible as replica;
- teaching ranking is reachable votes DESC → replication offset DESC → node ID ASC;
- promotion creates fresh replication identity/backlog/listener;
- returning old primary becomes replica of the chosen primary;
- no fencing exists; consensus/linearizability are not claimed.

## 9. Cluster rules — v0.10

- exactly 16384 logical slots using CRC-16/XMODEM modulo 16384;
- first non-empty `{...}` hash tag replaces whole-key hash input;
- multi-key routed commands must share one slot or fail CROSSSLOT before mutation;
- stable wrong owner returns MOVED;
- source→target migration exposes ASK/ASKING explicitly;
- ASKING is one request on one connection;
- values move as logical `SnapshotEntry` and preserve absolute TTL;
- source deletes only after target import acknowledgement;
- partial migration remains visible and retryable;
- stable ownership switches only after migration completion;
- bounded gossip + deterministic epoch/LWW merge is not consensus;
- authoritative cluster traffic uses the cluster-aware facade/endpoint.

## 10. RESP3 rules — v0.11

- every new connection starts in RESP2;
- `HELLO 3` and `HELLO 2` change only that connection;
- protocol state never enters Store, SnapshotEntry, AOF, replication backlog or Cluster topology;
- `resp.Value` may represent richer logical values and RESP2 explicitly downgrades them;
- current rich views include HGETALL→Map, SMEMBERS→Set, SISMEMBER→Boolean and ZSCORE→Double;
- Pub/Sub Push is RESP3 representation of the same delivery mechanism;
- incomplete nested frames are never partially consumed;
- legacy/reactor engines share the same session protocol state;
- Cluster HELLO negotiation never bypasses MOVED/ASK/CROSSSLOT.

## 11. Observability rules — v0.12

### Observer-only contract

- metrics are process/control-plane runtime evidence, never database state;
- metrics never enter Store, SnapshotEntry, AOF, RDB, replication frames or slot topology records;
- metrics never decide command success, eviction, expiration, persistence, replication, routing or failover;
- instrumentation is optional: an established mechanism must still work if its metric sink is absent;
- observation failure must not convert a successful database operation into an error.

### Snapshot contract

- `Server.MetricsSnapshot`, `Topology.MetricsSnapshot` and `Controller.MetricsSnapshot` expose copies/atomic loads only;
- callers cannot mutate internal metric maps or mechanism state through snapshots;
- `INFO` is a read-only server teaching view assembled from a snapshot;
- Cluster and Sentinel keep separate snapshots because they own separate control-plane state.

### Measurement contract

- command/network/persistence timing wraps real existing operations instead of synthetic substitutes;
- legacy socket bytes are counted from actual `net.Conn` reads/writes;
- reactor bytes are counted from actual `syscall.Read/Write`, and epoll wait is measured at `Poller.Wait`;
- fixed histogram buckets bound memory independently of request count;
- persistence byte evidence comes from actual encoded AOF work or resulting RDB/rewrite files;
- replication exposes primary offset/backlog/sync counts and replica-local applied offset/work; no remote lag is invented without ACKs;
- Cluster redirect/migration/gossip and Sentinel probe/failover metrics are recorded after/around existing policy decisions, never consulted by them.

### Performance claims

- microbenchmarks and end-to-end workloads answer different questions;
- every quoted performance result must identify commit, environment and workload/configuration;
- do not claim production Redis parity or context-free speedups;
- profile before changing a mechanism and rerun the identical workload afterward.

## 12. Testing baseline

Before reporting a version clean, run on the exact SHA:

```bash
gofmt -d .
go vet ./...
go test -race ./...
go build ./cmd/mini-redis-server
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make session-demo
make replication-demo
make sentinel-demo
make cluster-demo
make resp3-demo
make observability-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

For v0.12 specifically prove:

- concurrent counters/histograms are race-free and bounded;
- metric snapshots do not expose mutable maps;
- INFO is read-only;
- command/protocol/connection/actual-wire-byte/latency metrics work on legacy and reactor;
- reactor poll observation does not change event-loop behavior;
- expiration/eviction observation preserves established semantics;
- AOF/RDB/rewrite metrics measure real operations/bytes and errors;
- replication metrics expose truthful local/full/partial/backlog evidence without inventing remote ACK state;
- Cluster MOVED/ASK/CROSSSLOT/gossip/migration metrics come from real paths;
- Sentinel probe/failover metrics come from the existing quorum/election path;
- `make observability-demo` produces a coherent cross-layer snapshot;
- all historical demos remain green.

Do not claim a check passed unless it ran on the exact reported SHA.

## 13. Documentation contract

Every version keeps synchronized:

- `Agents.md`
- `Plan.md`
- `version.md`
- `docs/ARCHITECTURE.md`
- README when user-visible behavior changes
- CHANGELOG / VERSION

`version.md` explains prior-version delta, mechanisms, invariants, non-goals, reading order and next dependency.

## 14. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant + error-path tests exist;
- [ ] race/build/wire/demo/smoke validation passes;
- [ ] changed persistence/replication/topology/protocol/observability boundaries tested;
- [ ] docs and VERSION synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] next-version rationale documented.

## 15. Current directive

Freeze candidate: **v0.12 — bounded observer-only metrics, immutable snapshots/INFO, cross-layer mechanism evidence and repeatable profiling workflow**.

v0.12 completes the original architecture-learning sequence. Do not start a v0.13 feature list by momentum alone; a next arc should begin from measured evidence and one explicit new learning question.
