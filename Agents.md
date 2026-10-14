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
 ├── v0.12 observability/performance
 └── v0.13 replication ACK / authoritative lag
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
   └── replication data stream
          ↓
       primary ───────────────► replica
          ▲   canonical frame      │
          │                        │ apply complete frame
          └──── v0.13 ACK ─────────┘
               applied offset

v0.9 Sentinel control observes/reconfigures topology separately.

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
- Store never owns network/failover/slot/protocol/metrics/ACK metadata;
- command handlers do not inspect private encoding payloads;
- persistence and replication serialize logical state/commands, never client protocol or metrics state;
- ACK control frames are not canonical mutation frames and never enter AOF/RDB/backlog;
- Sentinel monitors/reconfigures replication topology and never copies database values;
- Cluster ownership/migration remains separate from Store values;
- Cluster migration crosses nodes via `SnapshotEntry`;
- response adaptation occurs only after command and Cluster authorization;
- observability records completed/attempted mechanism work but never decides mechanism behavior;
- ACK-derived lag is observable protocol state and must not silently become Sentinel/failover policy.

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
- v0.13 `replication.Primary.mu` owns backlog end offset, live subscribers, connection-local replica IDs and accepted ACK progress together; ACK validation is atomic with respect to those values.
- removing a replication subscriber must remove its live ACK/progress entry under the same `Primary.mu`.
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

## 7. Replication rules — v0.8 foundation

- one primary runtime owns one repl-id and canonical byte offset;
- backlog evicts whole frames only;
- snapshot + base offset + live subscriber attachment share `persistenceMu`;
- matching repl-id + retained frame boundary permits CONTINUE; otherwise FULLRESYNC;
- replica offset advances only after complete frame application;
- replica client writes are READONLY;
- client RESP2/RESP3 mode never changes replication frames;
- the v0.8 protocol alone had no replica→primary ACK, which is the limitation v0.13 explicitly extends.

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
- Cluster redirect/migration/gossip and Sentinel probe/failover metrics are recorded after/around existing policy decisions, never consulted by them.

### Performance claims

- microbenchmarks and end-to-end workloads answer different questions;
- every quoted performance result must identify commit, environment and workload/configuration;
- do not claim production Redis parity or context-free speedups;
- profile before changing a mechanism and rerun the identical workload afterward.

## 12. Replication ACK rules — v0.13

### Separate data and control directions

```text
primary → replica : canonical mutation frames
replica → primary : REPLCONF ACK <connection-replica-id> <applied-offset>
```

- ACK frames are control metadata; they never enter the canonical backlog and never advance primary replication offset;
- FULLRESYNC/CONTINUE handshakes include one connection-local replica ID;
- reconnect creates a fresh connection-local replica ID even when repl-id/offset permit CONTINUE;
- replica ID is not durable node identity.

### Applied-progress contract

- FULLRESYNC ACK is sent only after `RestoreSnapshot` succeeds and local replica offset becomes the snapshot base offset;
- frame ACK is sent only after the complete frame decodes and all commands apply successfully;
- accepted ACK offsets obey `0 <= previous_ack <= new_ack <= primary_offset`;
- equal ACK is valid and may refresh ACK age without inventing progress;
- wrong-ID, negative, regressing and ahead-of-primary ACKs are rejected without modifying accepted progress;
- protocol-level ACK is trusted receiver-reported progress; the primary cannot independently prove that a malicious/manual peer truly applied the data.

### Live progress / lag contract

For a connected acknowledged replica:

```text
lag_bytes = max(primary_offset - acknowledged_offset, 0)
ack_age_ms = now - last_accepted_ack_time
```

- no ACK yet is represented explicitly, not as fake zero lag;
- disconnect or subscriber eviction removes live replica progress;
- `ack_age_ms` measures age of the last applied-progress ACK, not heartbeat age;
- an idle caught-up replica can have growing `ack_age_ms` while `lag_bytes == 0`;
- Sentinel health/quorum/candidate logic must not read ACK age or lag in v0.13;
- ACK does not mean disk/fsync durability because teaching replicas remain memory-only.

## 13. Testing baseline

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
make replication-ack-demo
make sentinel-demo
make cluster-demo
make resp3-demo
make observability-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

For v0.13 specifically prove:

- ACK registry accepts monotonic in-range progress and rejects wrong/negative/regressing/ahead values;
- ACK traffic does not change primary canonical offset or backlog framing;
- real replica FULLRESYNC sends ACK only after snapshot installation;
- real replica live/backlog frames ACK only after complete apply;
- withholding ACK while primary advances makes `lag_bytes > 0`;
- later ACK returns lag to zero;
- disconnect removes live progress;
- CONTINUE reconnect creates a fresh connection-local replica ID and can re-establish lag zero from the retained offset;
- INFO/immutable snapshots expose ACK progress without mutable subscriber state;
- `make replication-ack-demo` demonstrates the full lag lifecycle;
- all historical demos remain green.

Do not claim a check passed unless it ran on the exact reported SHA.

## 14. Documentation contract

Every version keeps synchronized:

- `Agents.md`
- `Plan.md`
- `version.md`
- `docs/ARCHITECTURE.md`
- README when user-visible behavior changes
- CHANGELOG / VERSION

`version.md` explains prior-version delta, mechanisms, invariants, non-goals, reading order and next dependency.

## 15. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant + error-path tests exist;
- [ ] race/build/wire/demo/smoke validation passes;
- [ ] changed persistence/replication/topology/protocol/observability boundaries tested;
- [ ] docs and VERSION synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] version branch and `main` compare identical;
- [ ] next-version rationale documented.

## 16. Current directive

Freeze candidate: **v0.13 — bidirectional replication ACK, connection-local replica identity, primary-visible applied offsets and authoritative byte lag**.

Do not turn ACK age/lag into failover policy in this version. A future milestone should again start from a separate systems question and measured evidence rather than feature momentum.
