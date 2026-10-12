# Agents.md — mini-redis Development Contract

> **Mandatory instruction for every future iteration:** before changing code, tests, docs, branches, version scope, persistence semantics, protocol behavior, or architecture, read this file first.
>
> This file is an engineering contract. It is not optional guidance.

---

# 1. Repository mission

mini-redis is an educational Redis implementation in Go for understanding the mechanisms behind real Redis by building them in dependency order.

The project optimizes for:

1. architectural clarity;
2. correctness and explicit invariants;
3. protocol behavior that can be inspected end-to-end;
4. readable reference implementations;
5. tests that prove mechanisms rather than only outputs;
6. conceptual mapping to Redis internals;
7. version-to-version diffs that each teach one major mental model.

It does **not** optimize for:

- full Redis command compatibility;
- production deployment or security hardening;
- raw benchmark numbers before the mechanism is understandable;
- copying Redis source code line-for-line;
- adding features merely because Redis has them.

The guiding progression is:

```text
one version
   ↓
one primary Redis mental model
   ↓
small readable mechanism
   ↓
explicit invariant tests
   ↓
redis-cli / integration demonstration
```

---

# 2. Mandatory reading order before any work

Every agent must determine the current branch first.

## If current branch is `main`

Read in this order:

1. `Agents.md`
2. `Plan.md`
3. current `version.md` if present
4. `docs/ARCHITECTURE.md`
5. architecture docs relevant to the subsystem being changed
6. existing tests for that subsystem

`Plan.md` is the canonical forward architecture and version-sequencing document.

## If current branch is `v0.x` / `v1.x`

Read in this order:

1. `Agents.md`
2. `version.md`
3. relevant architecture docs
4. relevant source files
5. relevant tests

Do not assume a historical version branch should contain mechanisms introduced by a later version.

---

# 3. Branch model

## 3.1 `main` invariant

**`main` must always be the newest integrated, validated version of mini-redis.**

A version branch may be used for focused development, but validated desired behavior must be synchronized into `main` so `main` never intentionally trails the newest working state.

Forbidden:

```text
v0.3: validated newer implementation
main: stale v0.2 implementation
```

Required:

```text
v0.3 validated increment ─────────► main
                                    newest integrated state
```

## 3.2 Version branches

Version branches are educational snapshots, not parallel products.

Examples:

- `v0.1`: RESP2 -> command registry -> string store -> TTL -> AOF
- `v0.2`: nonblocking I/O and event loop
- `v0.3`: dictionary internals, incremental rehash, Redis-like object metadata
- later versions follow `Plan.md`

Every version branch must contain:

- `version.md`
- `Agents.md`
- tests for the mechanisms promised by that version

## 3.3 Historical branch freeze

After a version reaches Definition of Done, treat the branch as a historical learning snapshot.

Allowed after freeze:

- documentation corrections;
- test corrections that preserve scope;
- narrowly scoped fixes required for the version's own promised behavior.

Not allowed:

- backporting future subsystems for convenience;
- replacing the old architecture with a newer one;
- changing the branch so it no longer demonstrates its original learning milestone.

---

# 4. Version lifecycle

Every new version follows five phases.

## Phase A — plan

Before coding:

1. state the single primary mental model;
2. explain why it depends on the previous version;
3. define subsystem ownership and dependency direction;
4. identify invariants that tests must prove;
5. define explicit non-goals;
6. define acceptance criteria;
7. create or update `version.md`;
8. verify the scope still matches `Plan.md`.

## Phase B — implement the reference mechanism

Priority order:

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

When a production Redis technique is too complex for the first implementation, keep the educational reference path and document the production difference.

## Phase C — validate invariants

Tests must prove mechanism-level behavior.

Examples:

- RESP: partial/truncated frames fail predictably and binary-safe bulk strings round-trip;
- command registry: arity and unknown-command errors are independent of handlers;
- expiration: stale keys are invisible before physical cleanup and active expiration reclaims unread keys;
- AOF: only successful mutations persist and relative TTL does not restart after reboot;
- dictionary: lookups remain correct during incremental rehash;
- event loop: one slow client does not block unrelated clients;
- transactions: queued commands execute atomically with respect to the server execution loop;
- replication: master and replica offsets converge after synchronization;
- cluster: a key maps deterministically to exactly one hash slot.

## Phase D — integrate

After validation:

1. update docs and examples;
2. update `version.md` status/delta;
3. run the required validation suite on the exact commit;
4. integrate the validated state into `main`;
5. verify `main` is not behind the active version branch.

## Phase E — freeze

When Definition of Done is satisfied:

- freeze the version branch as a learning snapshot;
- create the next version branch from current `main`, never from a stale historical branch.

---

# 5. New version branch procedure

When starting `vX.Y`:

1. verify `main` is the newest validated state;
2. create `vX.Y` from current `main`;
3. create/update `version.md` immediately;
4. ensure `Agents.md` exists;
5. describe the delta from the previous version;
6. implement only that version's scope;
7. synchronize validated increments back to `main`;
8. freeze the branch after Definition of Done.

Correct:

```text
latest main
   └──► v0.4
```

Incorrect:

```text
v0.2
  └──► v0.4
```

when `main` already contains v0.3.

---

# 6. Required `version.md` contract

Each version snapshot must include a root `version.md` describing:

1. branch/version identity;
2. previous version and base relationship;
3. version theme;
4. **what changed relative to the previous version, and why**;
5. mechanisms added;
6. mechanisms intentionally unchanged;
7. architecture/mental-model diagram;
8. important source files;
9. tests and executable examples;
10. learning goals;
11. non-goals;
12. remaining work if active;
13. next-version direction.

A version document is not a changelog. It explains the learning boundary and architectural delta.

---

# 7. Architecture rules

## 7.1 Preserve dependency direction

Preferred high-level dependency flow:

```text
TCP / client lifecycle
        │
        ▼
   RESP protocol
        │
        ▼
 command registry
        │
        ▼
 command semantics
        │
        ▼
    Store API
        │
   ┌────┴─────┐
   ▼          ▼
objects      TTL
   │
   ▼
Dict / encodings

successful mutation
        │
        ▼
 persistence
 AOF / RDB
```

Later distributed layers sit above the same command/state semantics:

```text
Replication -> command propagation / offsets
Sentinel    -> topology and failover control
Cluster     -> key-slot ownership and redirection
```

Avoid cycles such as storage importing network/server packages or RESP depending on command handlers.

## 7.2 Separate policy from mechanism

Examples:

- RESP parser decodes frames; command registry decides command metadata.
- command handler defines semantic mutation; store owns state invariants.
- store decides whether a key exists; expiration policy decides when stale memory is physically reclaimed.
- AOF encodes mutation history; fsync policy decides durability timing.
- poller reports readiness; event loop decides client lifecycle and command execution order.
- dictionary stores entries; eviction policy chooses victims.
- replication transports state changes; failover logic chooses a new primary.

Do not merge independent responsibilities merely to reduce file count.

## 7.3 Stable boundaries before optimization

Before optimizing a subsystem, make the boundary explicit.

Preferred progression:

```text
reference implementation
      ↓
invariant tests
      ↓
profile / identify pressure
      ↓
optimized educational implementation
      ↓
comparison test / benchmark
```

Examples:

- Go map before custom Redis-like dictionary;
- goroutine-per-client before epoll reactor;
- direct AOF before AOF rewrite;
- simple expiry sampling before adaptive expiry cycle;
- straightforward list/hash/set before compact encodings.

## 7.4 Conceptual Redis compatibility

Map mechanisms to Redis concepts when pedagogically useful, but do not copy upstream internals blindly.

Important conceptual anchors include:

- RESP2/RESP3 framing;
- command table metadata;
- `dict` and incremental rehash;
- SDS-like dynamic strings;
- Redis object type/encoding separation;
- listpack/quicklist-style compact collection storage;
- skip list + hash table for sorted sets;
- event loop/file events/time events;
- lazy + active expiration;
- approximate LRU/LFU eviction;
- AOF + fsync + rewrite;
- RDB snapshot and fork/COW concepts;
- replication offsets/backlog/partial resync;
- Sentinel-style monitoring/election/failover;
- Cluster hash slots, MOVED/ASK, migration and gossip.

---

# 8. Scope discipline

Each version should introduce one dominant architectural idea.

Before adding a feature, ask:

1. Does it explain the current version's mental model?
2. Is it required by an acceptance criterion?
3. Is it a dependency of the current mechanism?
4. Would adding it now hide a more fundamental mechanism that should be taught first?

If it belongs later, record it in `Plan.md` or `version.md`; do not smuggle it into the current version.

Examples:

- Do not add Cluster before standalone command/state semantics and replication exist.
- Do not add compact list encodings before object/type boundaries are explicit.
- Do not add I/O threading before a single-threaded/nonblocking event-loop model is observable.
- Do not add RDB fork/COW behavior before basic snapshot semantics are testable.

---

# 9. Protocol and command compatibility rules

## 9.1 RESP behavior

Protocol code must remain isolated from command semantics.

For supported RESP features, test:

- valid frames;
- empty strings/arrays where applicable;
- binary-safe bulk payloads when the API supports bytes;
- partial reads;
- malformed length fields;
- truncated input;
- response encoding.

Do not claim complete RESP compatibility unless tested.

## 9.2 Redis command behavior

When a command intentionally follows Redis semantics, document edge cases and test them. If mini-redis deliberately simplifies behavior, say so in README/version docs.

Do not silently return a plausible but Redis-incompatible result merely to keep code short.

---

# 10. Concurrency and atomicity rules

Concurrency model is version-specific and must be explicit.

For v0.1:

- transport uses one goroutine per connection;
- `store.Store` operations are mutex-protected;
- atomicity is at store-operation granularity, not a claim of full Redis single-thread execution semantics.

For the event-loop version:

- command execution order and blocking behavior must be documented;
- nonblocking I/O must handle partial reads/writes;
- a slow client must not stall unrelated clients;
- background tasks must not mutate state without an explicit synchronization model.

Never describe an operation as atomic without stating the boundary that makes it atomic.

---

# 11. Persistence and durability rules

Persistence bugs can look correct during normal execution, so durability invariants require dedicated tests.

For AOF/RDB changes, reason about:

- crash before write;
- write without fsync;
- crash during final record;
- restart/replay;
- relative vs absolute expiry time;
- failed mutation vs successful mutation;
- rewrite/snapshot atomic replacement;
- corrupted input;
- startup source precedence.

Critical invariant already established in v0.1:

> A relative TTL persisted to AOF must not restart from its original duration after process downtime.

Relative expiry commands may therefore be rewritten to absolute deadlines for persistence.

Do not claim an OS `write()` is equivalent to durable storage; distinguish write, page cache, fsync and disk persistence.

---

# 12. Testing requirements

Every new subsystem should include, where applicable:

## A. Happy-path tests

Expected behavior for normal inputs.

## B. Invariant/property tests

Tests for the mechanism itself.

## C. Error-path tests

Malformed protocol, wrong arity/type, overflow, invalid persistence state and unsupported combinations should fail clearly.

## D. Compatibility/reference tests

Where useful, compare selected behavior with `redis-server` / `redis-cli` or documented Redis semantics. Keep the supported surface explicit.

## E. Integration tests

Exercise multiple layers together: protocol -> command -> store -> persistence, or server restart -> replay -> observable state.

## F. Concurrency/race validation

For code that uses goroutines or shared state:

```bash
go test -race ./...
```

Required baseline validation before reporting a version as clean:

```bash
gofmt -d .
go vet ./...
go test ./...
go test -race ./...
go build ./cmd/mini-redis-server
```

When `redis-cli` is available, also run:

```bash
make smoke
```

Do not claim a command passed unless it was actually run on the exact code being reported.

---

# 13. Documentation requirements

Update docs whenever a change modifies:

- protocol behavior;
- public command behavior;
- store contracts;
- persistence format or recovery behavior;
- concurrency/atomicity model;
- version scope;
- branch policy;
- learning sequence;
- major invariants;
- setup or example usage.

For a version change, review at minimum:

- `version.md`
- `Agents.md`
- `Plan.md` on `main` if sequencing changed
- `docs/ARCHITECTURE.md`
- README when user-facing behavior changed

`docs/ROADMAP.md` is only a short navigator. `Plan.md` is canonical.

---

# 14. Commit rules

Prefer commits that express architectural intent.

Examples:

```text
feat(resp): parse RESP2 bulk arrays
feat(expire): add lazy and active expiration
feat(aof): persist relative TTL as absolute deadline
feat(net): introduce poller abstraction
test(dict): preserve lookup correctness during rehash
docs: define v0.3 dictionary invariants
```

Avoid mixing unrelated subsystems in a single opaque commit when normal development allows smaller coherent commits.

Repository bootstrap may be one coherent commit because the files collectively establish the initial v0.1 learning snapshot.

---

# 15. Generated artifacts and dependencies

Do not commit runtime artifacts such as:

- `appendonly.aof`;
- RDB snapshots;
- benchmark profiles;
- coverage output;
- binaries;
- temporary client captures;
- generated test databases.

Tiny deterministic fixtures are allowed when required by tests.

Keep dependencies minimal. Prefer Go's standard library until a dependency is directly justified by the current version's learning goal.

---

# 16. Performance rule

Performance work must explain *why* the speedup exists.

A benchmark without a mechanism is not a learning milestone.

When adding an optimization:

1. preserve or document the reference path;
2. benchmark before and after;
3. state complexity and memory trade-offs;
4. add correctness tests that both paths satisfy;
5. document where production Redis differs.

Examples:

- goroutine-per-client vs epoll reactor;
- stop-the-world rehash vs incremental rehash;
- linked representation vs compact encoding;
- full expiry scan vs sampling;
- append-only growth vs rewrite/compaction.

---

# 17. Redis reference policy

When behavior depends on Redis internals, prefer authoritative sources:

- Redis source repository;
- redis.io command/protocol/docs;
- official Redis architecture or persistence documentation.

Use upstream sources to verify concepts and behavior. Implement original educational equivalents; do not reproduce large source passages or mechanically port implementation code.

When Redis versions differ materially, document which conceptual behavior mini-redis is modeling.

---

# 18. Definition of Done for any version

A version may be frozen only when all applicable items are true:

- [ ] primary mental model is clearly stated;
- [ ] implementation is complete for promised scope;
- [ ] unit tests pass;
- [ ] invariant tests pass;
- [ ] error-path tests exist;
- [ ] integration example works;
- [ ] race tests pass for concurrent code;
- [ ] protocol/reference checks exist where relevant;
- [ ] persistence recovery is tested where relevant;
- [ ] `version.md` accurately describes the delta;
- [ ] `Agents.md` exists;
- [ ] architecture docs are synchronized;
- [ ] generated artifacts are ignored;
- [ ] `main` contains the newest validated state;
- [ ] next version's dependency rationale is documented.

---

# 19. Current branch directive: `main`

`main` is the integration branch and canonical newest state.

Current integrated milestone: **v0.1 — core request/storage/persistence loop**.

The next planned architectural milestone is **v0.2 — nonblocking networking and an event loop**.

For work on `main`:

1. read `Plan.md` before choosing a subsystem;
2. do not bypass the version dependency sequence;
3. keep tests/docs synchronized with code;
4. keep v0.1's stable protocol/command/store boundaries unless a later version explicitly documents why they evolve;
5. do not begin collection encodings or replication work before the v0.2 event-loop learning milestone is explicit and validated.
