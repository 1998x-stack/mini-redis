# Agents.md — mini-redis Development Contract

> Mandatory for every iteration. Read this file before changing code, tests, docs, branch scope, protocol behavior, persistence semantics or architecture.

## 1. Mission

mini-redis is an educational Redis implementation in Go. Optimize for architectural clarity, explicit invariants, readable mechanisms, end-to-end observability and version-to-version diffs that each teach one dominant idea.

Do not optimize for full Redis compatibility, production hardening, source-code imitation or feature count.

```text
one version
   ↓
one primary mental model
   ↓
small explicit mechanism
   ↓
invariant tests
   ↓
end-to-end demonstration
```

## 2. Mandatory reading order

On `main`:

1. `Agents.md`
2. `Plan.md`
3. `version.md`
4. `docs/ARCHITECTURE.md`
5. relevant source/tests

On `v0.x`: read `Agents.md`, then `version.md`, then subsystem docs/source/tests. Historical branches must not absorb later mechanisms.

## 3. Branch model

`main` is always the newest validated integrated state. Version branches are educational snapshots.

```text
main ───────────────────────────────► newest validated
 ├── v0.1 request/state/AOF
 ├── v0.2 event loop
 ├── v0.3 Dict/rehash
 ├── v0.4 collections/encodings
 ├── v0.5 RDB/AOF rewrite
 ├── v0.6 maxmemory/eviction
 └── v0.7 transactions/PubSub
```

Create a new version from current `main`. After Definition of Done, validate the exact version commit, fast-forward `main`, validate that same SHA on `main`, then freeze the version branch.

## 4. Architecture direction

```text
TCP/client lifecycle
      ↓
RESP
      ↓
clientSession / connection mode
      ↓
server coordination
      ↓
command registry / semantics
      ↓
memory-pressure policy
      ↓
Store contract
      ↓
Object{Type,Encoding,payload}
      ↓
Dict / collection encodings / TTL
      ↓
logical state / SnapshotEntry
      ↓
AOF / RDB
      ↓
future replication stream
```

Rules:

- RESP never depends on handlers;
- storage never imports network/server;
- command handlers do not inspect private encoding payloads;
- per-client transaction/subscription state never lives in Store;
- Store owns atomic value/TTL invariants;
- Dict/SkipList implement mechanisms, not command policy;
- persistence serializes logical state/history, never unstable Go memory layout;
- eviction heat and WATCH/session metadata never become persistence-format state;
- replication must later consume canonical logical mutation order, not private memory layouts.

## 5. Concurrency / atomicity

- v0.1 legacy transport: one goroutine/client, Memory mutex.
- v0.2+ reactor: client I/O/parse/output state belongs to one event-loop owner.
- Memory mutex remains the Store synchronization boundary because legacy engine still shares it.
- Collection read-modify-write is one Store operation.
- From v0.5, `Server.persistenceMu` names the logical command mutation + persistence ordering boundary.
- v0.6 keeps memory-pressure decision and eviction persistence inside that boundary.
- v0.7 holds `persistenceMu` across WATCH validation and an entire EXEC command sequence.
- v0.7 maintenance expiration and RDB snapshot capture share that boundary so they cannot interleave/observe half of EXEC.
- AOF `Log.mu` owns the live descriptor, rewrite buffer and final descriptor swap.
- Never claim atomicity without naming the lock/file-system/protocol boundary that enforces it.

## 6. Transaction/session rules

v0.7 establishes a connection-state layer.

Requirements:

- `MULTI` queues commands; it does not promise rollback;
- queue-time command/arity errors dirty the transaction and cause `EXECABORT`;
- execution-time errors remain individual EXEC result elements and do not undo successful siblings;
- `WATCH` is optimistic conflict detection, not a lock;
- write versions detect change-then-change-back conflicts;
- existence recheck catches watched-key expiration;
- `DISCARD` and successful/conflicted EXEC clear WATCH state;
- transaction/session state is dropped on connection close;
- EXEC persistence uses AOF framing so a crash-truncated final transaction cannot replay a prefix.

The current teaching subset intentionally rejects `PUBLISH`, subscription changes and `UNWATCH` inside MULTI. Do not silently widen semantics without tests/docs.

## 7. Pub/Sub rules

- subscriptions belong to `clientSession`, never keyspace objects;
- the hub may route live messages but never persist them;
- subscribed mode is an explicit connection role;
- disconnect must remove every channel membership;
- delivery count means matched subscribed sessions, not durable acknowledgement;
- no message history/backlog is implied by Pub/Sub.

Do not confuse Pub/Sub with future replication backlog or Streams.

## 8. Dictionary/object rules

The custom Dict remains the canonical keyspace mechanism. Lookup/update/delete must work while rehashing; no logical duplicate may exist across `ht[0]/ht[1]`; one `RehashStep(N)` has bounded work.

From v0.4 onward logical Type and physical Encoding are distinct:

```text
String : int/raw
List   : listpack/quicklist
Hash   : listpack/hashtable
Set    : intset/hashtable
ZSet   : listpack/skiplist
```

Commands validate Type, not payload implementation. Representation conversion preserves logical results and TTL. Persistence never serializes private encoding structures.

## 9. Persistence rules

Established invariants:

- only successful logical mutations are appended to AOF;
- relative TTL persists as an absolute deadline;
- restart/replay must not resurrect expired state;
- `SnapshotEntry` is the stable logical persistence boundary;
- RDB-like files validate magic/version/length/checksum;
- publication uses same-directory temp → file fsync → atomic rename → directory fsync;
- failure before rename leaves the old target valid;
- AOF rewrite emits logical reconstruction commands;
- writes after the rewrite snapshot boundary enter the rewrite buffer;
- AOF is authoritative at startup when enabled;
- truncated final ordinary AOF frames are ignored under the explicit crash-tail policy;
- v0.7 AOF MULTI/EXEC frames are applied only when the closing EXEC is complete;
- corrupt/incompatible complete persistence input fails startup.

## 10. Memory-pressure rules

- expiration is time validity; eviction is memory-pressure policy;
- `noeviction` rejection cannot leave a partially applied logical mutation;
- sampling work is bounded and observable;
- eviction removes value + expiry together;
- LRU/LFU heat is runtime policy state and never persisted.

## 11. Testing baseline

Before reporting a version clean, run on the exact code:

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
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

For v0.7 specifically test queue-time versus execution-time failures, WATCH conflict/no-conflict, TTL conflict, transaction AOF truncation, subscriber disconnect cleanup and both network engines.

Do not claim a check passed unless it ran on the exact reported SHA.

## 12. Documentation contract

Every version must keep synchronized:

- `Agents.md`
- `Plan.md`
- `version.md`
- `docs/ARCHITECTURE.md`
- README when user-visible behavior changes
- CHANGELOG / VERSION

`version.md` must explain what changed relative to prior version, why, critical invariants, non-goals and the next dependency.

## 13. Commit discipline

Prefer coherent architectural commits. Do not commit binaries, AOF/RDB runtime files, profiles or generated temporary data. Avoid mixing the next version's mechanism into a current-version fix.

## 14. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant/error-path tests exist;
- [ ] race/build/smoke/demo validation passes;
- [ ] persistence recovery tested when changed;
- [ ] version/docs synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] next-version rationale documented.

## 15. Current directive

Freeze candidate: **v0.7 — per-client sessions, WATCH/MULTI/EXEC and live Pub/Sub**.

After exact-SHA validation, next milestone is **v0.8 — replication full sync, canonical mutation propagation, offsets/backlog and partial resynchronization**.

For v0.8:

1. preserve transaction order in the replication stream;
2. establish a consistent snapshot + post-snapshot suffix boundary before optimizing transfer;
3. make offsets explicit and testable;
4. use a bounded backlog for reconnect, never as durable database state;
5. keep replicas read-only initially;
6. do not begin Sentinel election or Cluster slots yet.
