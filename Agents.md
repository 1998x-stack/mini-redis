# Agents.md — mini-redis Development Contract

> Mandatory for every iteration. Read this file before changing code, tests, docs, branch scope, protocol behavior, persistence semantics or architecture.

## 1. Mission

mini-redis is an educational Redis implementation in Go. Optimize for architectural clarity, explicit invariants, readable mechanisms, end-to-end observability and version-to-version diffs that each teach one dominant idea.

Do not optimize for full Redis compatibility, production hardening, source-code imitation or feature count.

```text
one version → one primary mental model → explicit mechanism → invariant tests → demo
```

## 2. Mandatory reading order

On `main`: `Agents.md` → `Plan.md` → `version.md` → `docs/ARCHITECTURE.md` → relevant source/tests.

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
 ├── v0.6 memory pressure/eviction
 └── ...
```

Create a new version from current `main`. After Definition of Done, validate the exact version commit, fast-forward `main`, validate that same SHA on `main`, then freeze the version branch.

## 4. Architecture direction

```text
TCP/client lifecycle
      ↓
RESP
      ↓
command registry / semantics
      ↓
server ordering + persistence boundary
      ↓
memorypolicy.Manager          # runtime-only policy
      ↓
Store / Object / TTL / Dict
      ↓
logical state / SnapshotEntry
      ↓
AOF / RDB
```

Rules:

- RESP never depends on handlers;
- storage never imports network/server;
- command handlers do not inspect private encoding payloads;
- Store owns atomic value/TTL invariants;
- Dict/SkipList implement mechanisms, not command policy;
- persistence serializes logical state/history, never unstable Go memory layout;
- eviction heat/counters are runtime policy state and never persistence-format state.

## 5. Scope discipline

Before adding anything ask whether it teaches the current mental model, is required by acceptance criteria, is a direct dependency, or hides a more fundamental lesson. Record later work in `Plan.md`; do not smuggle it into the current version.

## 6. Concurrency / atomicity

- legacy transport: one goroutine/client with Memory mutex;
- reactor: client I/O/parse/output state belongs to one event-loop owner;
- collection read-modify-write is one Store operation;
- `Server.persistenceMu` orders command mutation/AOF append against snapshot/rewrite start and v0.6 memory-pressure decisions;
- AOF `Log.mu` owns the live descriptor, rewrite buffer and descriptor swap;
- never claim atomicity without naming the lock/file-system boundary.

## 7. Dictionary / object rules

- custom Dict remains the canonical top-level keyspace;
- lookup/update/delete work during rehash;
- no logical duplicate across `ht[0]/ht[1]`;
- `RehashStep(N)` has bounded explicit work;
- logical Type stays separate from physical Encoding;
- representation changes preserve logical results and TTL.

## 8. Persistence rules

Established through v0.5:

- only successful acknowledged mutations enter AOF;
- relative TTL persists as absolute deadline;
- `SnapshotEntry` is the stable logical state boundary;
- RDB validates magic/version/length/checksum;
- publication is temp → file fsync → atomic rename → directory fsync;
- rewrite buffers every acknowledged mutation after the snapshot boundary;
- AOF is authoritative at startup when enabled;
- truncated final AOF frame is tolerated; corrupt/incompatible RDB fails startup.

Any new runtime metadata must justify whether it is logical durable state. LRU/LFU heat is explicitly not durable.

## 9. Memory-pressure rules

From v0.6:

- expiration is time validity; eviction is memory-pressure policy;
- logical byte estimates are deterministic teaching estimates, not heap/RSS claims;
- `noeviction` rejection cannot leave a partial logical mutation or AOF record;
- removal-only commands remain usable above the limit;
- one eviction removes value + expiry + runtime access metadata together;
- random/LRU/LFU use bounded candidate scoring per victim selection;
- sampled LRU/LFU do not maintain a globally sorted exact policy structure;
- eviction is persisted as logical `DEL`, but LRU/LFU metadata is never serialized.

## 10. Testing baseline

Before reporting a version clean, run on the exact commit:

```bash
gofmt -d .
go vet ./...
go test -race ./...
go build ./cmd/mini-redis-server
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

Mechanism tests are mandatory: collisions/rehash, encoding conversion, SkipList rank, TTL, persistence crash/recovery, expiration-vs-eviction, noeviction rollback/AOF invariance, bounded LRU/LFU sampling and restart state after eviction.

## 11. Documentation contract

Every version keeps `Agents.md`, `Plan.md`, `version.md`, `docs/ARCHITECTURE.md`, README, CHANGELOG and VERSION synchronized.

## 12. Commit discipline

Prefer coherent architectural commits. Do not commit runtime AOF/RDB files, binaries, benchmark profiles or temporary data. Do not rewrite unrelated historical mechanisms merely to make the current version look cleaner.

## 13. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant/error-path tests exist;
- [ ] race/build/demos/smoke pass;
- [ ] persistence interaction tested when changed;
- [ ] version/docs synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] next-version rationale documented.

## 14. Current directive

Current milestone after integration: **v0.6 — approximate maxmemory accounting, expiration vs eviction, random/sampled LRU/sampled LFU policies**.

Next milestone: **v0.7 — MULTI/EXEC/DISCARD, WATCH/UNWATCH and basic Pub/Sub**.

For v0.7:

1. preserve v0.6 memory/persistence behavior;
2. define per-client transaction state outside Store values;
3. validate queued command syntax before EXEC while deferring execution;
4. add key versioning for optimistic WATCH conflicts;
5. make EXEC one explicit ordered server boundary and define AOF behavior;
6. model Pub/Sub connection state explicitly;
7. do not begin replication yet.
