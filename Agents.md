# Agents.md — mini-redis Development Contract

> Mandatory for every future iteration: read this file before changing code, tests, docs, branch scope, protocol behavior, persistence semantics or architecture.

## 1. Mission

mini-redis is an educational Redis implementation in Go. It exists to expose one Redis mechanism at a time in dependency order.

Optimize for:

1. architectural clarity;
2. correctness and explicit invariants;
3. readable reference implementations;
4. tests that prove mechanisms, not only outputs;
5. end-to-end observability from TCP bytes to state;
6. version-to-version diffs that teach one dominant mental model.

Do not optimize for:

- full Redis API compatibility;
- production deployment/security hardening;
- copying Redis source line-for-line;
- benchmark numbers before the architecture is clear;
- unrelated features merely because Redis has them.

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

### On `main`

1. `Agents.md`
2. `Plan.md`
3. `version.md`
4. `docs/ARCHITECTURE.md`
5. relevant source files
6. relevant tests

`Plan.md` is the canonical forward roadmap.

### On a version branch (`v0.x`, `v1.x`)

1. `Agents.md`
2. `version.md`
3. relevant architecture docs
4. relevant source
5. relevant tests

Never assume a historical branch should contain later-version mechanisms.

## 3. Branch model

`main` is always the newest integrated, validated state.

```text
main ─────────────────────────────────────► newest validated state
  │
  ├── v0.1 request/state/persistence snapshot
  ├── v0.2 event-loop snapshot
  ├── v0.3 dictionary/rehash snapshot
  └── ...
```

Version branches are educational snapshots, not parallel products.

A validated active branch must not remain newer than `main`.

Historical branches may receive only:

- documentation corrections;
- test corrections that preserve scope;
- narrowly scoped fixes needed for that version's promised behavior.

Do not backport later architecture into a frozen learning snapshot.

## 4. Version lifecycle

### Phase A — plan

Before coding:

1. state the primary question;
2. explain dependency on the previous version;
3. define subsystem ownership and dependency direction;
4. state invariants that tests must prove;
5. state non-goals;
6. update `version.md`;
7. verify scope against `Plan.md`.

### Phase B — reference mechanism

Priority:

```text
correctness
  ↓
clarity
  ↓
testability
  ↓
architecture
  ↓
performance
```

Keep a readable reference path when an optimized path would hide the idea.

### Phase C — validate

Tests should prove architectural behavior, for example:

- RESP partial frames remain buffered without corrupting parsing;
- event loop: one partial/slow client does not block another;
- dictionary: lookup remains correct during incremental rehash;
- AOF: only successful mutations persist and TTL deadlines survive restart;
- replication: offsets converge after synchronization;
- cluster: a key maps deterministically to one slot.

### Phase D — integrate

After validation:

1. update docs/examples;
2. run the exact required validation suite;
3. integrate the validated commit into `main`;
4. verify `main` is not behind the active version branch.

### Phase E — freeze

When Definition of Done is satisfied, freeze the version branch and create the next version branch from latest `main`.

## 5. `version.md` contract

Every version snapshot must explain:

1. identity/status;
2. previous version;
3. version theme/primary question;
4. what changed and why;
5. mechanisms added;
6. mechanisms intentionally unchanged;
7. architecture diagram;
8. important source files;
9. tests/examples;
10. learning goals;
11. non-goals;
12. remaining work;
13. next-version direction.

The most important question is always:

> What changed relative to the previous version, and why?

## 6. Architecture dependency direction

Preferred direction:

```text
TCP / client lifecycle
        ↓
RESP protocol
        ↓
command registry
        ↓
command semantics
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

Distributed layers are later consumers of the same state/command semantics:

```text
Replication → offsets/backlog
     ↓
Sentinel / Cluster
```

Rules:

- storage must not import network/server packages;
- RESP must not depend on command handlers;
- poller reports readiness; event loop owns lifecycle/policy;
- persistence records state history; fsync policy decides durability timing;
- dictionary stores entries; eviction policy chooses victims.

Separate policy from mechanism even when combining them would reduce file count.

## 7. Version scope discipline

Before adding a feature ask:

1. Does it explain this version's mental model?
2. Is it required by an acceptance criterion?
3. Is it a dependency of the current mechanism?
4. Would it hide a more fundamental subsystem that belongs first?

If it belongs later, record it in `Plan.md` instead of implementing it now.

Examples:

- no List/Hash/Set/ZSet before dictionary/object boundaries;
- no RDB fork/COW before snapshot correctness is explicit;
- no replication before standalone state/persistence are stable;
- no I/O thread pool before the single reactor model is understood.

## 8. Protocol rules

Protocol code remains isolated from command semantics.

For supported RESP features test:

- valid frames;
- empty/binary-safe payloads where applicable;
- partial reads;
- malformed length/type fields;
- truncated input;
- response encoding;
- pipeline boundaries.

Do not claim complete RESP compatibility unless tested.

## 9. Concurrency and event-loop rules

### v0.1 historical model

- one goroutine per client;
- blocking network API;
- mutex-protected Store;
- Store-operation atomicity only.

### v0.2 current model

- Linux reactor uses nonblocking sockets + epoll;
- client parsing/output state belongs to the event-loop owner;
- command handlers execute serially on the reactor goroutine;
- partial reads/writes must never block the loop;
- writable readiness is enabled only while output exists;
- client buffers must have explicit limits;
- one slow or malformed client must not stall unrelated clients;
- active expiration runs as an event-loop timer;
- AOF append remains synchronous and is explicitly outside the networking-isolation claim;
- legacy engine exists only as a reference/comparison path.

Never describe something as atomic without naming the boundary that makes it atomic.

## 10. Persistence rules

Persistence needs dedicated failure/restart reasoning:

- crash before write;
- write without fsync;
- truncated final record;
- replay after restart;
- relative vs absolute expiration;
- failed vs successful mutation;
- snapshot/rewrite atomic replacement;
- corrupted input.

Established invariant:

> A relative TTL persisted to AOF must not restart from its original duration after downtime.

Do not equate `write()` with durable storage; distinguish page cache, fsync and disk durability.

## 11. Testing baseline

New subsystems should include:

- happy path;
- invariant/property tests;
- error-path tests;
- integration tests;
- compatibility/reference checks where useful;
- race validation for concurrent/shared state.

Before reporting a version clean, run on the exact code:

```bash
gofmt -d .
go vet ./...
go test ./...
go test -race ./...
go build ./cmd/mini-redis-server
make smoke
```

For v0.2 also validate both network engines:

```bash
NETWORK_ENGINE=reactor make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

Do not claim a command passed unless it actually ran successfully.

## 12. Documentation rules

Update docs when changing:

- protocol behavior;
- public command behavior;
- Store contracts;
- persistence format/recovery;
- concurrency/atomicity model;
- version scope;
- branch policy;
- major invariants;
- setup/examples.

For each version review at least:

- `version.md`;
- `Agents.md`;
- `Plan.md` when sequencing/status changes;
- `docs/ARCHITECTURE.md`;
- README when user-facing behavior changes.

`docs/ROADMAP.md` is only a navigator. `Plan.md` is canonical.

## 13. Commit rules

Prefer architectural commits, e.g.:

```text
feat(net): add epoll poller and incremental RESP buffering
feat(server): add reactor networking engine and backpressure
test(net): add dual-engine smoke and comparison harness
docs: document v0.2 event-loop learning boundary
```

Avoid opaque commits that mix unrelated subsystems.

## 14. Generated artifacts and dependencies

Do not commit:

- binaries;
- `appendonly.aof`;
- RDB files;
- benchmark profiles/results as universal claims;
- coverage output;
- temporary captures/databases.

Keep dependencies minimal. Prefer the Go standard library until a dependency directly supports the current learning goal.

## 15. Performance rule

Performance work must explain the mechanism.

Preferred sequence:

```text
reference path
   ↓
correctness tests
   ↓
benchmark/profile
   ↓
optimized educational path
   ↓
compare trade-offs
```

Benchmark output is machine/kernel dependent. Never present one run as a universal Redis performance claim.

## 16. Redis reference policy

When behavior depends on Redis internals, prefer authoritative sources:

- Redis source repository;
- redis.io command/protocol documentation;
- official Redis persistence/architecture documentation.

Use upstream sources to verify concepts, then implement original educational equivalents. Do not mechanically port Redis source.

## 17. Definition of Done

A version may be frozen only when applicable items are true:

- [ ] primary mental model is explicit;
- [ ] promised mechanism is implemented;
- [ ] unit/invariant/error-path tests exist;
- [ ] integration example works;
- [ ] race tests pass;
- [ ] persistence recovery is tested where relevant;
- [ ] `version.md` describes the delta accurately;
- [ ] `Agents.md` exists;
- [ ] architecture docs are synchronized;
- [ ] generated artifacts are ignored;
- [ ] `main` contains the newest validated state;
- [ ] next-version dependency rationale is documented.

## 18. Current directive

Current integrated milestone: **v0.2 — nonblocking networking and an epoll reactor**.

Next milestone: **v0.3 — Redis-like Dict, incremental rehash and object metadata**.

For new work:

1. create v0.3 from latest validated `main`;
2. keep v0.2's RESP/command/reactor behavior stable;
3. replace hidden Go-map behavior behind the Store boundary rather than mixing hash-table logic into command handlers;
4. prove lookup/update/delete correctness during rehash;
5. do not begin collection encodings or distributed features until the dictionary milestone is explicit and validated.
