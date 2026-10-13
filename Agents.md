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
 ├── v0.7 transactions/PubSub
 └── v0.8 replication
```

Create a new version from current `main`. After Definition of Done, validate the exact version commit, fast-forward `main`, validate that same SHA on `main`, then freeze the version branch.

## 4. Architecture direction

```text
client TCP/RESP
      ↓
clientSession / connection mode
      ↓
server coordination / persistenceMu
      ↓
command registry / memory policy / Store
      ↓
Object + Dict + TTL + collection encodings
      ↓
logical SnapshotEntry / canonical mutation commands
      ├── RDB/AOF
      └── replication feed
             ↓
       repl-id / offset / backlog
             ↓
            replica
             ↓
      future Sentinel topology
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
- replication consumes canonical logical mutation order, never private Object/Dict memory layout;
- Sentinel must later monitor/reconfigure replication topology, not create another data-copy path.

## 5. Concurrency / atomicity

- v0.1 legacy transport: one goroutine/client, Memory mutex.
- v0.2+ reactor: client I/O/parse/output state belongs to one event-loop owner.
- Memory mutex remains the Store synchronization boundary because legacy engine still shares it.
- Collection read-modify-write is one Store operation.
- From v0.5, `Server.persistenceMu` names the logical mutation + durability ordering boundary.
- v0.6 keeps memory-pressure decision and eviction persistence inside that boundary.
- v0.7 holds `persistenceMu` across WATCH validation and a complete EXEC sequence; maintenance/RDB capture share that boundary.
- v0.8 uses the **same** `persistenceMu` order to publish replication frames and to capture full-sync snapshot + base offset + live-subscriber handoff.
- AOF `Log.mu` owns the live descriptor, rewrite buffer and final descriptor swap.
- `replication.Primary.mu` owns replication identity/backlog subscription handoff; backlog has its own internal lock but is always reached through a stable primary ordering path for live publication.
- Never claim atomicity without naming the lock/file-system/protocol boundary that enforces it.

## 6. Transaction/session rules

- `MULTI` queues commands; it does not promise rollback;
- queue-time command/arity errors dirty the transaction and cause `EXECABORT`;
- execution-time errors remain individual EXEC result elements and do not undo successful siblings;
- `WATCH` is optimistic conflict detection, not a lock;
- write versions detect change-then-change-back conflicts;
- existence recheck catches watched-key expiration;
- `DISCARD` and successful/conflicted EXEC clear WATCH state;
- transaction/session state is dropped on connection close;
- AOF EXEC persistence uses `MULTI ... EXEC` framing;
- v0.8 successful EXEC mutations also become one replication frame so live and catch-up replication cannot cut a transaction boundary.

The teaching subset intentionally rejects `PUBLISH`, subscription changes and `UNWATCH` inside MULTI. Do not silently widen semantics without tests/docs.

## 7. Pub/Sub rules

- subscriptions belong to `clientSession`, never keyspace objects;
- the hub routes live messages but never persists them;
- subscribed mode is an explicit connection role;
- disconnect removes every channel membership;
- delivery count means matched subscribed sessions, not durable acknowledgement;
- Pub/Sub backlog/history is not implied.

Do not confuse Pub/Sub with the v0.8 **replication backlog** or future Streams.

## 8. Dictionary/object rules

The custom Dict remains the canonical keyspace mechanism. Lookup/update/delete must work while rehashing; no logical duplicate may exist across `ht[0]/ht[1]`; one `RehashStep(N)` has bounded work.

Logical Type and physical Encoding remain distinct:

```text
String : int/raw
List   : listpack/quicklist
Hash   : listpack/hashtable
Set    : intset/hashtable
ZSet   : listpack/skiplist
```

Commands validate Type, not payload implementation. Representation conversion preserves logical results and TTL. Persistence and replication never serialize private encoding structures.

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
- AOF MULTI/EXEC frames apply only when closing EXEC is complete;
- corrupt/incompatible complete persistence input fails startup.

## 10. Memory-pressure rules

- expiration is time validity; eviction is memory-pressure policy;
- `noeviction` rejection cannot leave a partially applied logical mutation;
- sampling work is bounded and observable;
- eviction removes value + expiry together;
- eviction-generated `DEL` belongs to AOF **and** primary replication reconstruction order;
- LRU/LFU heat is runtime policy state and never persisted or replicated as database state.

## 11. Replication rules

v0.8 establishes the data-copy topology layer.

### Stream identity and offset

- one primary process owns one replication identity;
- a fresh primary process gets a fresh identity in v0.8;
- offsets count canonical RESP **mutation payload bytes**, not TCP envelope bytes;
- replica offset advances only after a complete frame applies successfully.

### Canonical feed

- ordinary successful logical mutations become one replication frame;
- relative expiry is rewritten to absolute `PEXPIREAT` before replication;
- eviction-generated `DEL` is included;
- failed/read-only commands emit no replication frame;
- successful EXEC mutations are wrapped as one `MULTI ... EXEC` frame.

### Full sync

While holding `persistenceMu`, primary must:

```text
capture SnapshotEntry[]
      ↓
capture current replication offset
      ↓
attach live subscriber
      ↓
release persistenceMu
```

Only then may it transfer the snapshot. This prevents a write from falling between base snapshot and live suffix attachment.

### Backlog / partial sync

- backlog is bounded runtime state, never durable database state;
- eviction happens in complete frame units;
- resume accepts only current end or retained complete-frame boundaries;
- matching repl-id + complete retained suffix → partial resync;
- stale identity, missing suffix or wrapped history → full sync;
- catch-up frames must preserve original frame boundaries.

### Replica

- client-side mutating commands return READONLY;
- replication apply bypasses that client gate;
- v0.8 replicas intentionally disable local AOF and maxmemory eviction policy;
- full base installs before suffix application;
- private in-memory encodings never cross the replication protocol.

## 12. Testing baseline

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
make replication-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

For v0.8 specifically prove:

- frame-aware backlog wrap and boundary lookup;
- full sync convergence;
- write-after-snapshot live propagation;
- disconnect/reconnect via CONTINUE when suffix is retained;
- fallback FULLRESYNC after backlog loss;
- repl-id/offset convergence;
- transaction framing across replication;
- replica READONLY behavior;
- race cleanliness of reconnect/fan-out/state inspection.

Do not claim a check passed unless it ran on the exact reported SHA.

## 13. Documentation contract

Every version must keep synchronized:

- `Agents.md`
- `Plan.md`
- `version.md`
- `docs/ARCHITECTURE.md`
- README when user-visible behavior changes
- CHANGELOG / VERSION

`version.md` must explain what changed relative to prior version, why, critical invariants, non-goals and the next dependency.

## 14. Commit discipline

Prefer coherent architectural commits. Do not commit binaries, AOF/RDB runtime files, profiles or generated temporary data. Avoid mixing the next version's mechanism into a current-version fix.

## 15. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant/error-path tests exist;
- [ ] race/build/smoke/demo validation passes;
- [ ] persistence/recovery/replication boundaries tested when changed;
- [ ] version/docs synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] next-version rationale documented.

## 16. Current directive

Freeze candidate: **v0.8 — full synchronization, canonical mutation propagation, repl-id/offset, bounded backlog and partial resynchronization**.

After exact-SHA validation, next milestone is **v0.9 — Sentinel-inspired health monitoring and failover**.

For v0.9:

1. build on the v0.8 replication topology; do not add another copy mechanism;
2. distinguish subjective-down from quorum-backed objective-down;
3. make election/failover epochs explicit and testable;
4. promote the healthiest sufficiently fresh replica under a documented teaching policy;
5. reconfigure surviving replicas and handle old-primary return;
6. do not begin Cluster slot ownership yet.
