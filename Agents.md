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
 └── ...
```

Create a new version from current `main`. After Definition of Done, validate the exact version commit, fast-forward `main`, validate that same SHA on `main`, then freeze the version branch.

## 4. Architecture direction

```text
TCP/client lifecycle
      ↓
RESP
      ↓
command registry
      ↓
command semantics
      ↓
Store contract
      ↓
Object{Type,Encoding,payload}
      ↓
Dict / collection encodings / TTL
      ↓
logical state
      ↓
AOF / RDB
```

Rules:

- RESP never depends on handlers;
- storage never imports network/server;
- command handlers do not inspect private encoding payloads;
- Store owns atomic state invariants and TTL coordination;
- Dict/SkipList implement mechanisms, not command policy;
- persistence must serialize logical state/history, not unstable Go memory layouts.

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
- Collection read-modify-write must be one Store operation; command handlers may not emulate atomicity with separate `Get`/`Set` calls.
- Never claim atomicity without naming the boundary.

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
- do not serialize Go-private payload structures as a persistence format.

## 9. Persistence rules

Established invariants:

- successful mutations only are appended to AOF;
- relative TTL must persist as an absolute deadline;
- `write()` is not equivalent to durable storage;
- restart/replay must not resurrect expired state;
- a future RDB/AOF rewrite must publish files only through atomic replacement.

For persistence work test truncated/corrupt input, crash windows, fsync policy and restart state.

## 10. Testing baseline

Before reporting a version clean, run on the exact commit:

```bash
gofmt -d .
go vet ./...
go test -race ./...
go build ./cmd/mini-redis-server
make rehash-demo
make encodings-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

Add mechanism-specific tests, not only happy-path commands. Examples:

- forced hash collision during rehash;
- compact→general conversion with same logical results;
- SkipList order/rank against a sorted reference;
- `WRONGTYPE` errors;
- TTL survival across collection mutation;
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
feat(zset): add span-aware skip list ordered index
feat(object): add collection types and adaptive encodings
feat(store): add atomic collection operations with TTL semantics
feat(command): add list hash set and sorted-set commands
test(types): verify encoding transitions and collection invariants
docs: document v0.4 collection encoding boundary
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

Current milestone after integration: **v0.4 — List/Hash/Set/ZSet and adaptive encodings**.

Next milestone: **v0.5 — RDB-like snapshots and AOF rewrite**.

For v0.5:

1. keep all v0.4 command semantics stable;
2. define a logical serialization representation independent of in-memory encoding;
3. preserve absolute expiry deadlines;
4. implement blocking snapshot correctness before fork/COW experiments;
5. use temp-file + fsync + atomic rename for publication;
6. prove interrupted rewrite/snapshot cannot replace the valid old persistence file;
7. do not begin eviction or replication yet.
