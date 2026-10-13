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

`main` is always the newest validated integrated state. Version branches are frozen educational snapshots.

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
 └── v0.9 Sentinel-inspired HA
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
logical state + canonical mutation commands
      ├── RDB/AOF
      └── v0.8 replication data plane
             ↓
       primary / replicas
             ↑
             │ topology control only
             │
       v0.9 Sentinel plane
       SDOWN → ODOWN → epoch
       → promote → reconfigure
             ↓
       future Cluster slots
```

Rules:

- RESP never depends on command handlers;
- storage never imports network/server;
- command handlers do not inspect private encoding payloads;
- per-client transaction/subscription state never lives in Store;
- persistence serializes logical state/history, never unstable Go memory layout;
- replication consumes canonical logical mutation order, never private Object/Dict layout;
- Sentinel monitors and reconfigures replication topology; it never becomes a second data-copy path;
- future Cluster slot ownership must remain separate from ordinary value payloads.

## 5. Concurrency / atomicity

- Memory mutex remains the Store synchronization boundary because both network engines share it.
- `Server.persistenceMu` is the logical mutation-order boundary from v0.5 onward.
- v0.7 holds it across WATCH validation + a complete EXEC.
- v0.8 uses it for mutation publication and full-sync snapshot/offset/subscriber handoff.
- v0.9 also uses it across runtime replication role transitions so a logical mutation cannot cross promotion/demotion halfway through.
- `replControlMu` owns runtime role/upstream/current-primary identity.
- role-specific contexts cancel obsolete replica-follow or primary-listen goroutines after a topology switch.
- `replication.Primary.mu` owns repl-id/backlog/live-subscriber ordering.
- Sentinel `Controller.tickMu` serializes one controller's failover/repair Tick operations.
- monitor state has independent locks because SDOWN is monitor-local.
- never claim atomicity without naming the lock, file-system or protocol boundary that enforces it.

## 6. Transaction/session rules

- `MULTI` queues commands; it does not promise rollback;
- queue-time validation errors cause `EXECABORT`;
- execution-time errors remain EXEC result elements without undoing successful siblings;
- `WATCH` is optimistic conflict detection, not a lock;
- successful EXEC mutations are one AOF transaction frame and one replication frame;
- transaction/session state dies with the connection;
- Pub/Sub subscriptions/messages remain ephemeral runtime state.

## 7. Persistence / memory rules

Established invariants:

- only successful logical mutations enter AOF;
- relative TTL becomes absolute deadline across persistence/replication;
- `SnapshotEntry` is the logical serialization boundary;
- AOF rewrite buffers post-snapshot mutations;
- truncated final transaction frame does not replay a prefix;
- expiration and eviction are distinct;
- noeviction rejection cannot leave a partial logical mutation;
- eviction-generated DEL belongs to durability and replication order;
- LRU/LFU heat is runtime policy state, never database state.

## 8. Replication rules

v0.8 remains the only database-copy mechanism.

### Primary stream

- one primary runtime owns one repl-id and byte offset;
- offsets count canonical RESP mutation payload bytes;
- ordinary accepted mutation = one frame;
- successful EXEC = one `MULTI ... EXEC` frame;
- backlog evicts complete frames only.

### Full/partial sync

While holding `persistenceMu` the primary captures:

```text
SnapshotEntry[]
      ↓
base offset
      ↓
live subscriber attachment
```

Then snapshot transfer may occur. Matching repl-id + retained frame boundary gives CONTINUE; otherwise FULLRESYNC.

### Replica

- client writes return READONLY;
- replication apply bypasses the client gate;
- offset advances only after whole-frame application;
- v0.8/v0.9 teaching replicas have no independent local AOF or maxmemory eviction policy.

## 9. Sentinel rules

v0.9 establishes topology control above replication.

### Health

- every monitor owns its own last-success/last-failure/failure-since state;
- one monitor exceeding `down-after` marks only SDOWN;
- ODOWN requires configured quorum of SDOWN monitor states;
- recovery clears that monitor's SDOWN state.

### Failover epoch

- one failover attempt receives one monotonically increasing epoch;
- the teaching leader is the lexicographically first SDOWN voter;
- this deterministic rule is for observability and is **not** a distributed-consensus claim;
- one controller serializes Tick calls so overlapping promotions cannot originate from the same controller instance.

### Candidate selection

Candidate must have quorum reachability and quorum observation as `replica`.

Ranking:

```text
reachable votes DESC
replication offset DESC
node ID ASC
```

An unreachable/stale candidate must not win over an eligible healthier/fresher candidate under this policy.

### Promotion / topology repair

- `PromoteToPrimary` cancels upstream following, creates a fresh repl-id/backlog, opens a replication listener and becomes writable;
- surviving replicas are told to follow the promoted primary;
- following a different primary resets remembered repl-id/offset and re-enters safe FULLRESYNC;
- a returning old primary is reconfigured as replica of the chosen primary;
- dynamic demotion rejects local AOF or maxmemory policy in v0.9 rather than silently violating replica invariants.

### Explicit limitation

Sentinel observation does not fence an unreachable old primary. A partition can temporarily leave an old primary writable while a new primary is promoted. v0.9 therefore does **not** claim linearizability, split-brain prevention, leases, consensus, or production Redis Sentinel compatibility.

Do not hide this limitation in docs/tests.

## 10. Testing baseline

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
make sentinel-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

For v0.9 specifically prove:

- one SDOWN vote cannot create ODOWN;
- quorum ODOWN creates one epoch/failover;
- candidate reachability and freshness ranking;
- runtime replica promotion and fresh repl-id;
- surviving replica reconfiguration and convergence;
- old-primary return as replica;
- repeated Tick after a healthy failover does not create another promotion;
- real three-node failover under `go test -race`;
- deterministic `sentinel-demo`;
- previous v0.8 replication behavior remains green.

Do not claim a check passed unless it ran on the exact reported SHA.

## 11. Documentation contract

Every version keeps synchronized:

- `Agents.md`
- `Plan.md`
- `version.md`
- `docs/ARCHITECTURE.md`
- README when user-visible behavior changes
- CHANGELOG / VERSION

`version.md` explains prior-version delta, mechanism, invariants, non-goals and next dependency.

## 12. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant/error-path tests exist;
- [ ] race/build/smoke/demo validation passes;
- [ ] persistence/replication/topology boundaries tested when changed;
- [ ] version/docs synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] next-version rationale documented.

## 13. Current directive

Freeze candidate: **v0.9 — SDOWN/ODOWN, failover epoch, deterministic candidate promotion and topology repair**.

After exact-SHA validation, next milestone is **v0.10 — Cluster hash slots, MOVED/ASK and migration**.

For v0.10:

1. keep slot ownership separate from Store value representation;
2. implement deterministic key→slot routing before migration complexity;
3. distinguish stable `MOVED` from temporary migration `ASK` semantics;
4. reuse replication/failover mechanisms rather than creating a parallel copy/failure stack;
5. do not claim production Redis Cluster consensus semantics.