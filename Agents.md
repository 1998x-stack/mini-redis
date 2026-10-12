# Agents.md — mini-redis Development Contract

> Mandatory for every future iteration: read this file before changing code, tests, docs, branch scope, protocol behavior, persistence semantics or architecture.

## 1. Mission

mini-redis is an educational Redis implementation in Go. It exposes one Redis mechanism at a time in dependency order.

Optimize for architectural clarity, correctness, explicit invariants, readable reference implementations, mechanism-level tests, end-to-end observability and version-to-version diffs with one dominant mental model. Do not optimize for full Redis API compatibility, production hardening, source copying or benchmark numbers before the architecture is understandable.

Guiding rule:

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

On `main`: `Agents.md` → `Plan.md` → `version.md` → `docs/ARCHITECTURE.md` → relevant source/tests.

On a historical version branch: `Agents.md` → `version.md` → relevant architecture/source/tests. Never pull future mechanisms backward merely for convenience.

## 3. Branch model

`main` is always the newest integrated, validated state. Version branches are educational snapshots:

```text
main ─────────────────────────────► newest validated
  ├── v0.1 request/state/persistence
  ├── v0.2 event loop
  ├── v0.3 dictionary/rehash
  └── ...
```

Validated active work must be synchronized into `main`. Frozen branches may receive documentation/test corrections or narrowly scoped fixes that preserve their milestone; do not backport later architecture.

## 4. Version lifecycle

Before coding: state the primary question, dependency on the previous version, ownership/dependency direction, invariants, non-goals and acceptance criteria; update `version.md`; verify `Plan.md`.

Implementation priority:

```text
correctness -> clarity -> testability -> architecture -> performance
```

Validation must prove mechanisms, not just final output. After validation, update docs, integrate the exact validated commit into `main`, verify branch equality, then freeze the snapshot.

## 5. `version.md` contract

Every version explains identity/status, previous version, theme, **what changed and why**, mechanisms added/unchanged, architecture, important files, tests/examples, learning goals, non-goals, remaining work and next-version direction.

## 6. Dependency direction

```text
TCP/client lifecycle
        ↓
RESP
        ↓
command registry/semantics
        ↓
Store contract
   ┌────┼────┐
   ↓    ↓    ↓
Object TTL  Dict
        ↓
memory state
        ↓
AOF / RDB
```

Rules: storage must not import server/network; RESP must not depend on handlers; poller reports readiness while event loop owns policy; persistence records logical state/history; dictionary stores entries while eviction policy later chooses victims.

## 7. Scope discipline

Before adding a feature ask whether it teaches the current mental model, is required by acceptance criteria or is a dependency. If it belongs later, document it rather than implement it. In particular: no collection APIs before dictionary/object boundaries, no RDB fork/COW before snapshot correctness, no replication before standalone state/persistence, and no I/O thread pool before the single-reactor model is understood.

## 8. Protocol rules

Keep protocol isolated from command semantics. Test valid frames, binary/empty payloads where supported, partial reads, malformed/truncated input, response encoding and pipeline boundaries. Do not claim complete RESP compatibility unless tested.

## 9. Concurrency / event-loop / dictionary rules

### v0.1 historical

Blocking networking, one goroutine/client, mutex-protected Store; atomicity only at the named Store-operation boundary.

### v0.2+ networking

Linux reactor uses nonblocking sockets + epoll; client parsing/output state belongs to the loop; commands execute serially on reactor goroutine; partial I/O cannot block the loop; writable readiness exists only with pending output; buffers have limits; slow/malformed clients cannot stall others; active expiration is a time event. AOF append remains synchronous and is outside the networking-isolation claim. `legacy` is only a reference path.

### v0.3 dictionary/object

- keyspace and expiry index use `internal/dict`, not Go maps;
- Dict is not concurrency-safe; Store synchronization owns concurrency policy;
- lookup/update/delete must work across `ht[0]` and `ht[1]`;
- one logical key must never become conflicting duplicates across tables;
- `RehashStep(N)` consumes at most `N` explicit work units;
- one work unit moves at most one entry or skips one empty bucket;
- new keys enter the target table during rehash;
- `Range` callbacks must not mutate the same Dict; callers snapshot keys first if mutation is needed;
- logical Object type and physical encoding remain separate;
- encoding changes cannot change normal String command bytes;
- persistence never serializes transient bucket layout or rehash cursor.

Never call work “atomic” or “bounded” without naming the boundary that enforces it.

## 10. Persistence rules

Reason explicitly about crash-before-write, write-without-fsync, truncated tail, restart replay, relative/absolute expiry, failed/successful mutations, atomic file replacement and corruption. Established invariant: a relative TTL persisted to AOF must not restart from its original duration after downtime. Distinguish `write`, page cache, `fsync` and disk durability.

## 11. Testing baseline

Before reporting a version clean, run on the exact code:

```bash
gofmt -d .
go vet ./...
go test ./...
go test -race ./...
go build ./cmd/mini-redis-server
NETWORK_ENGINE=reactor make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

For v0.3 also run `make rehash-demo`; invariant tests must include forced collisions and a reference-model operation sequence while rehash is active. Do not claim a command passed unless it actually ran.

## 12. Documentation rules

Synchronize `version.md`, `Agents.md`, `Plan.md` when sequencing changes, `docs/ARCHITECTURE.md`, and README for user-facing changes. `docs/ROADMAP.md` is only a navigator; `Plan.md` is canonical.

## 13. Commit rules

Prefer architectural commits such as:

```text
feat(dict): add explicit buckets and bounded incremental rehash
feat(object): separate logical string type from physical encoding
feat(store): replace keyspace maps with incremental Dict
feat(server): schedule bounded dictionary maintenance
test(dict): add rehash integration and smoke coverage
docs: document v0.3 dictionary learning boundary
```

Avoid opaque commits mixing unrelated subsystems.

## 14. Generated artifacts / dependencies

Do not commit binaries, AOF/RDB runtime files, benchmark profiles/results as universal claims, coverage output or temporary captures. Prefer the Go standard library until a dependency is directly justified by the milestone.

## 15. Performance rule

Preserve the readable path, prove correctness, then benchmark/profile, optimize educationally and compare trade-offs. Measurements are machine/runtime dependent and are never universal Redis claims.

## 16. Redis reference policy

Prefer Redis source and redis.io documentation for behavior/concepts. Verify concepts upstream, then implement original educational equivalents rather than mechanically porting Redis source.

## 17. Definition of Done

A version freezes only when its mental model is explicit, promised mechanism complete, unit/invariant/error/integration tests exist, race/recovery tests pass where relevant, docs match, generated artifacts are excluded, `main` contains the newest validated state and the next-version rationale is documented.

## 18. Current directive

Current integrated milestone after validation: **v0.3 — Redis-like Dict, bounded incremental rehash and Object encoding**.

Next milestone: **v0.4 — core collection types and multiple internal encodings**.

For v0.4: create from latest validated `main`; preserve v0.3 Dict invariants; build through `Object.Type`/`Object.Encoding`; establish `WRONGTYPE` before broad collection APIs; prove representation conversion preserves logical results; introduce ZSet's member→score dictionary + skip list only after the general encoding boundary works; do not begin persistence/distributed work until v0.4 is validated.
