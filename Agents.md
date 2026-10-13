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
 └── v0.10 Cluster/hash slots
```

Create a version branch from current `main`. Definition of Done requires exact-SHA CI on the version branch, fast-forwarding `main` to that same SHA, then exact-SHA CI on `main`.

## 4. Architecture direction

```text
client / RESP
      ↓
connection/session layer
      ↓
v0.10 cluster routing gate
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
       v0.9 Sentinel topology control

v0.10 orthogonal topology:
key → CRC16/hash-tag → slot → primary owner
```

Rules:

- RESP does not depend on Store internals;
- Store never owns network/failover/slot metadata;
- command handlers do not inspect private encoding payloads;
- persistence and replication serialize logical state, never unstable Go layout;
- Sentinel monitors/reconfigures replication topology and does not copy database values;
- Cluster slot ownership/migration state is separate from ordinary key/value Store state;
- Cluster migration crosses nodes via `SnapshotEntry`, not private Object encodings.

## 5. Concurrency / atomicity

- Store mutexes own in-memory value/TTL atomicity.
- `Server.persistenceMu` is the canonical logical mutation-order boundary from v0.5 onward.
- v0.7 holds it across WATCH validation and complete EXEC execution.
- v0.8 uses it for mutation publication and snapshot/offset/subscriber handoff.
- v0.9 uses it across runtime replication role transitions.
- v0.10 cluster routing holds it across migration-sensitive existence observation, route authorization and command execution.
- `ClusterNode.TransferKey` holds the source `persistenceMu` from logical export through target acknowledgement and source delete.
- The v0.10 teaching migrator assumes one directed slot migration operation at a time; concurrent opposite-direction migrations are outside scope.
- `cluster.Topology.mu` owns node/slot/epoch/migration metadata.
- one cluster RESP connection serially owns its `cluster.Session`; ASKING is connection-local, not shared process state.
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
- private Dict/Object encodings never cross replication.

## 8. Sentinel rules — v0.9

- SDOWN is monitor-local;
- ODOWN requires configured SDOWN quorum;
- one controller serializes failover Tick operations;
- each failover attempt receives a monotonic epoch;
- candidate must be quorum-visible as replica;
- teaching ranking is reachable votes DESC → replication offset DESC → node ID ASC;
- promotion creates fresh replication identity/backlog/listener;
- surviving replicas follow the promoted primary;
- returning old primary becomes a replica of the chosen primary;
- no fencing exists, so split-brain prevention/consensus/linearizability are not claimed.

## 9. Cluster rules — v0.10

### Slots and hashing

- exactly `16384` logical slots;
- key slot = CRC-16/XMODEM modulo 16384;
- first non-empty `{...}` hash tag replaces the whole key as hash input;
- malformed/empty hash tags fall back to the whole key;
- every routed key deterministically maps to one slot.

### Multi-key routing

- routed keys are identified before Store execution;
- all routed keys in one command must share one slot;
- otherwise fail with `CROSSSLOT` before mutation;
- v0.10 cluster facade deliberately rejects MULTI/EXEC rather than promise incomplete cross-command routing semantics.

### Stable routing

- local stable owner executes;
- wrong stable owner returns `MOVED <slot> <addr>`;
- unassigned slot returns CLUSTERDOWN;
- MOVED means stable ownership, not temporary migration state.

### Migration routing

For `owner=source, migrationTarget=target`:

- source + existing key → execute locally;
- source + missing key → `ASK <slot> <target>`;
- target without ASKING → MOVED to stable source;
- target with ASKING → execute exactly one routed request;
- ASKING is consumed by the next command on that same connection.

### Key transfer

- slot migration state becomes visible before data moves;
- values cross the node boundary as logical `SnapshotEntry`;
- source deletes only after target import acknowledges success;
- target import reconstructs through existing command/persistence/replication machinery;
- absolute TTL is preserved;
- partial transfer failure leaves the migration marker visible;
- retry resumes from source's remaining keys;
- stable owner switches only after all remaining source keys transfer successfully.

### Topology dissemination

- gossip work is bounded by `GossipPage(start, limit)`;
- merge prefers higher topology epoch;
- equal epochs use a deterministic `(ownerID, migrationTarget)` tuple tie-break for slots and address tie-break for nodes;
- this LWW rule is a teaching convergence mechanism, not distributed consensus;
- do not claim fencing, authoritative conflict resolution, production anti-entropy or Redis Cluster bus compatibility.

### Endpoint boundary

- `Server.ServeCluster` is an explicit cluster-aware RESP2 endpoint;
- the historical standalone endpoint intentionally remains outside cluster routing in v0.10;
- authoritative cluster traffic must use the cluster facade/endpoint in tests and demos.

## 10. Testing baseline

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
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

For v0.10 specifically prove:

- CRC16/hash-tag vectors;
- CROSSSLOT rejection before mutation;
- stable MOVED;
- ASK versus MOVED during migration;
- ASKING one-shot semantics in direct and real TCP tests;
- logical key + TTL migration;
- source delete only after target success;
- partial failure remains visibly migrating and retry resumes;
- stable owner switches only after migration completion;
- bounded gossip reconstructs topology;
- equal-epoch gossip conflicts converge independent of arrival order;
- all v0.8 replication and v0.9 Sentinel tests/demos remain green.

Do not claim a check passed unless it ran on the exact reported SHA.

## 11. Documentation contract

Every version keeps synchronized:

- `Agents.md`
- `Plan.md`
- `version.md`
- `docs/ARCHITECTURE.md`
- README when user-visible behavior changes
- CHANGELOG / VERSION

`version.md` explains prior-version delta, mechanisms, invariants, non-goals, reading order and next dependency.

## 12. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant + error-path tests exist;
- [ ] race/build/wire/demo/smoke validation passes;
- [ ] persistence/replication/topology boundaries tested where changed;
- [ ] docs and VERSION synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] next-version rationale documented.

## 13. Current directive

Freeze candidate: **v0.10 — 16384 slots, MOVED/ASK/ASKING, resumable SnapshotEntry migration and bounded topology dissemination**.

Next milestone after exact-SHA validation: **v0.11 — RESP3 negotiation, richer types and push messages**.

Do not begin v0.11 until v0.10 branch and main are identical on one green exact SHA.
