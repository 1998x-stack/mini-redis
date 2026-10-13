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
future memory-pressure / replication layers
```

Rules:

- RESP never depends on handlers;
- storage never imports network/server;
- command handlers do not inspect private encoding payloads;
- Store owns atomic value/TTL invariants;
- Dict/SkipList implement mechanisms, not command policy;
- persistence serializes logical state/history, never unstable Go memory layout;
- later eviction policy must not become persistence-format state.

## 5. Version scope discipline

Before adding anything ask:

1. Does it explain this version's primary question?
2. Is it needed by an acceptance criterion?
3. Is it a dependency of the mechanism?
4. Does it prematurely hide a more fundamental lesson?

Record later work in `Plan.md`; do not smuggle it into the current version.

## 6. Concurrency / atomicity

- v0.1 legacy transport: one goroutine/client, Memory mutex.
- v0.2+ reactor: client I/O/parse/output state belongs to one event-loop owner.
- Memory mutex remains the Store synchronization boundary because the legacy engine still shares it.
- Collection read-modify-write is one Store operation.
- From v0.5, `Server.persistenceMu` names the command mutation + AOF append boundary relative to logical snapshot + rewrite start.
- AOF `Log.mu` owns live file descriptor, rewrite buffer and final descriptor swap.
- Never claim atomicity without naming the lock/file-system boundary that enforces it.

## 7. Dictionary rule

The custom Dict is the canonical keyspace mechanism from v0.3 onward.

Invariant requirements:

- lookup/update/delete work while rehashing;
- no logical duplicate across `ht[0]/ht[1]`;
- one `RehashStep(N)` has bounded work;
- server schedules only through the optional `RehashStepper` boundary.

Do not silently replace it with Go map in the keyspace.

## 8. Object/encoding rules

From v0.4 onward, distinguish logical type from physical encoding.

```text
String : int/raw
List   : listpack/quicklist
Hash   : listpack/hashtable
Set    : intset/hashtable
ZSet   : listpack/skiplist
```

Requirements:

- commands validate Type, not payload implementation;
- representation conversion preserves logical results and TTL;
- `OBJECT ENCODING` may expose encoding for learning;
- compact thresholds are policy, not public semantics;
- do not serialize Go-private payload structures as persistence format.

## 9. Persistence rules

Established v0.1-v0.5 invariants:

- only successful mutations are appended to AOF;
- relative TTL persists as an absolute deadline;
- `write()` is not equivalent to durable storage;
- restart/replay must not resurrect expired state;
- `SnapshotEntry` is the stable logical state boundary for state persistence;
- RDB-like files include explicit magic/version/length/checksum validation;
- RDB serialization must cover every value accepted by current command semantics;
- persistence publication uses same-directory temp → file fsync → atomic rename → directory fsync;
- failure before rename must leave the old persistence file intact;
- AOF rewrite emits logical reconstruction commands, not physical encodings;
- writes after the rewrite snapshot boundary must also enter the rewrite buffer;
- when AOF is enabled it is authoritative at startup; RDB is loaded only when AOF is disabled;
- truncated final AOF frames are ignored; corrupt/incompatible RDB fails startup.

For future persistence changes, test corruption, crash windows, fsync boundaries, restart state and interactions with new logical types.

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
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

Add mechanism-specific tests, not only happy-path commands. Examples:

- forced hash collision during rehash;
- compact→general conversion with same logical results;
- SkipList order/rank against a sorted reference;
- `WRONGTYPE` errors and TTL survival;
- logical snapshot round-trip independent of encoding;
- corrupt/version-mismatched snapshot rejection;
- crash before rename preserving old file;
- post-snapshot AOF rewrite mutations preserved;
- persistence restart/recovery.

Do not claim a check passed unless it ran on the exact reported SHA.

## 11. Documentation contract

Every version must keep synchronized:

- `Agents.md`
- `Plan.md`
- `version.md`
- `docs/ARCHITECTURE.md`
- README when user-visible behavior changes
- CHANGELOG / VERSION

`version.md` must explain what changed relative to the prior version, why, critical invariants, non-goals and the next dependency.

## 12. Commit discipline

Prefer coherent architectural commits such as:

```text
feat(store): add logical snapshot boundary
feat(rdb): add checksummed crash-safe logical snapshots
feat(aof): add buffered crash-safe rewrite
feat(server): integrate RDB startup and coordinated AOF rewrite
test(persistence): add recovery invariant coverage
docs: document v0.5 persistence boundary
```

Do not commit binaries, AOF/RDB runtime files, profiles or generated temporary data.

## 13. Definition of Done

- [ ] primary mental model explicit;
- [ ] promised mechanism implemented;
- [ ] invariant/error-path tests exist;
- [ ] race/build/smoke validation passes;
- [ ] persistence recovery tested when changed;
- [ ] version/docs synchronized;
- [ ] exact version SHA passes CI;
- [ ] `main` fast-forwarded to same SHA;
- [ ] same SHA passes CI on `main`;
- [ ] next-version rationale documented.

## 14. Current directive

Current milestone after integration: **v0.5 — logical snapshot boundary, RDB-like state persistence and buffered AOF rewrite**.

Next milestone: **v0.6 — expiration vs maxmemory eviction, approximate memory accounting and sampled LRU/LFU**.

For v0.6:

1. preserve all v0.5 persistence semantics;
2. keep expiration and eviction conceptually separate;
3. establish memory accounting before policy complexity;
4. make victim sampling bounded and testable with injected randomness/time;
5. remove value + expiry metadata together;
6. ensure `noeviction` failures cannot leave partial mutations;
7. do not begin transactions or replication yet.
