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
 └── v0.11 RESP3
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
```

Rules:

- protocol representation never owns database semantics;
- RESP parsing/encoding does not depend on Store internals;
- Store never owns network/failover/slot/protocol metadata;
- command handlers do not inspect private encoding payloads;
- persistence and replication serialize logical state/commands, never client protocol state;
- Sentinel monitors/reconfigures replication topology and never copies database values;
- Cluster ownership/migration remains separate from Store values;
- Cluster migration crosses nodes via `SnapshotEntry`;
- response adaptation occurs only after command and Cluster authorization.

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
- client RESP2/RESP3 mode never changes replication frames.

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

### Negotiation

- every new connection starts in RESP2;
- `HELLO 3` switches only that connection to RESP3;
- `HELLO 2` switches only that connection back to RESP2;
- protocol state never enters Store, SnapshotEntry, AOF, replication backlog or Cluster topology;
- command requests remain canonical arrays of blob strings.

### Logical value versus wire form

- `resp.Value` may represent Null, Boolean, Double, Big Number, Verbatim String, Map, Set and Push;
- the RESP3 encoder emits native markers;
- RESP2 encoding explicitly downgrades richer values rather than changing command semantics;
- selected command replies gain richer RESP3 views only after execution/authorization.

Current view adaptations:

```text
HGETALL    RESP2 Array  → RESP3 Map
SMEMBERS   RESP2 Array  → RESP3 Set
SISMEMBER  RESP2 0/1    → RESP3 Boolean
ZSCORE      RESP2 bulk   → RESP3 Double
null bulk/array          → RESP3 Null
```

### Push

- Pub/Sub delivery remains one mechanism;
- RESP3 represents subscription acknowledgements/messages as Push;
- RESP2 downgrades Push to Array;
- a RESP3 subscriber may issue ordinary commands because Push disambiguates asynchronous frames;
- RESP2 subscribed-mode restrictions remain for backward compatibility.

### Parser / transport

- `ParseValue` must not consume an incomplete nested frame;
- nested value parsing has a bounded depth;
- legacy and reactor engines share session-level HELLO/protocol semantics;
- transports only encode the logical reply using the current session protocol;
- Cluster HELLO negotiation must never bypass MOVED/ASK/CROSSSLOT routing.

## 11. Testing baseline

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
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

For v0.11 specifically prove:

- RESP3 native Map/Set/Boolean/Double/Null/Push encoding;
- RESP2 downgrade of richer logical values;
- fragmented nested value parsing at every split boundary;
- HELLO 3 and HELLO 2 are connection-local;
- rich command views on both legacy and reactor engines;
- Pub/Sub Push and ordinary commands while subscribed in RESP3;
- RESP2 historical Pub/Sub behavior remains green;
- Cluster HELLO 3 does not bypass MOVED/ASK/CROSSSLOT;
- all v0.8 replication, v0.9 Sentinel and v0.10 Cluster demos remain green.

Do not claim a check passed unless it ran on the exact reported SHA.

## 12. Documentation contract

Every version keeps synchronized:

- `Agents.md`
- `Plan.md`
- `version.md`
- `docs/ARCHITECTURE.md`
- README when user-visible behavior changes
- CHANGELOG / VERSION

`version.md` explains prior-version delta, mechanisms, invariants, non-goals, reading order and next dependency.

## 13. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant + error-path tests exist;
- [ ] race/build/wire/demo/smoke validation passes;
- [ ] persistence/replication/topology/protocol boundaries tested where changed;
- [ ] docs and VERSION synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] next-version rationale documented.

## 14. Current directive

Freeze candidate: **v0.11 — HELLO 2/3, connection-local protocol mode, richer response types, RESP2 downgrade and Pub/Sub Push**.

Next milestone after exact-SHA validation: **v0.12 — Observability / Performance**.

Do not begin v0.12 until v0.11 and `main` are identical on one green exact SHA.
