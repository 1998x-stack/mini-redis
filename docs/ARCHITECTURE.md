# Architecture — v0.9

## 1. Current stack

```text
                         Sentinel-inspired control plane
                    ┌──────────┬──────────┬──────────┐
                    │ monitor  │ monitor  │ monitor  │
                    │    s1    │    s2    │    s3    │
                    └────┬─────┴────┬─────┴────┬─────┘
                         │ health observations │
                         └──────────┬───────────┘
                                    ↓
                           SDOWN → quorum ODOWN
                                    ↓
                              failover epoch
                                    ↓
                           candidate selection
                                    ↓
                           topology operations
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ↓                           ↓                           ↓
   data node P                  data node R1                data node R2
 client RESP plane             client RESP plane           client RESP plane
        │                           │                           │
        └────────────── v0.8 replication data plane ───────────┘
                  SnapshotEntry base + RESP mutation frames
```

v0.9's central lesson is:

> **Replication answers “how does another node get the data?” Sentinel answers “which replication topology should be active?”**

Those layers are deliberately separate.

## 2. Why Sentinel does not live inside Store

Failure detection, election epoch and primary identity are not key/value data:

```text
Store database state
  keys / values / TTLs

Sentinel runtime state
  monitor observations
  SDOWN flags
  current chosen primary
  failover epoch
  last decision
```

Persisting monitor state inside the database would incorrectly make failover metadata subject to ordinary Redis commands, TTL, snapshots and replication.

The `internal/sentinel` package therefore depends on a small `Node` control interface rather than Store internals.

## 3. The Node control-plane boundary

Sentinel sees:

```go
ID()
Probe(ctx) -> NodeStatus
Promote(ctx)
Follow(ctx, upstream)
```

`NodeStatus` exposes only logical topology facts:

```text
node ID
role
client address
replication address
upstream
replication identity
offset
```

The adapter for a real mini-redis node is `ServerNode`.

A health probe first performs a real client-plane TCP `PING`. Only after that succeeds does it read the node's explicit replication topology state. Thus “healthy” means the data-serving endpoint is reachable, not merely that an in-process Go object exists.

## 4. Subjective failure: SDOWN

Each monitor owns an independent state record per node:

```text
last success
last failure
failure since
subjective-down flag
last successful NodeStatus
```

On the first failed probe:

```text
failureSince = now
SDOWN = false
```

Only when:

```text
now >= failureSince + downAfter
```

that monitor marks the node SDOWN.

A successful probe clears `failureSince` and SDOWN.

This state is intentionally monitor-local. If s1 is partitioned from the primary but s2 and s3 are healthy, only s1 has SDOWN.

## 5. Objective failure: ODOWN

The controller counts current monitor SDOWN states for the **current chosen primary**:

```text
SDOWN voters >= quorum ?
      │
   no │ yes
      ↓
 no failover       ODOWN
                    ↓
              candidate election
```

One timeout never implies objective failure.

With three monitors and quorum two:

```text
s1 SDOWN
s2 SDOWN
s3 healthy
   ↓
ODOWN
```

This makes the local-versus-shared-failure distinction observable and testable.

## 6. One controller Tick is one serialized decision boundary

`Controller.tickMu` serializes Tick calls.

One Tick performs:

```text
probe monitor × node matrix
       ↓
update monitor-local state
       ↓
compute current-primary SDOWN voters
       ↓
ODOWN?
  ├── no → optional topology repair
  └── yes
        ↓
     candidate ranking
        ↓
     next epoch
        ↓
     promotion
        ↓
     replica reconfiguration
        ↓
     commit chosen primary
```

This prevents two goroutines using the same Controller instance from overlapping promotions.

It is not a claim that independent controller processes have reached consensus.

## 7. Failover epoch and deterministic leader

Every failover attempt increments a monotonically increasing in-memory epoch.

For an educational deterministic rule, SDOWN voter IDs are sorted and the first voter is recorded as the leader/coordinator for the attempt.

```text
SDOWN voters = [s1, s2, s3]
leader = s1
epoch = previous + 1
```

Why have an epoch at all?

Without one, logs/tests cannot distinguish:

```text
one failover retried internally
```

from:

```text
a later, separate failover decision
```

The epoch creates an explicit identity for topology-changing decisions.

Production Redis Sentinel uses richer peer/election behavior. v0.9's deterministic rule is intentionally not presented as equivalent.

## 8. Candidate eligibility and ranking

For every non-primary node the controller evaluates the current probe matrix.

Eligibility requires:

```text
reachable votes >= quorum
AND
votes observing RoleReplica >= quorum
```

Then ranking is:

```text
1. reachable votes DESC
2. replication offset DESC
3. node ID ASC
```

This produces a stable teaching policy:

- a replica visible to only one of three monitors cannot be promoted when quorum is two;
- among equally reachable replicas, the fresher replication offset wins;
- exact ties are deterministic.

The policy is deliberately inspectable rather than trying to reproduce every Redis Sentinel candidate-priority rule.

## 9. Runtime promotion required a new server abstraction

v0.8 treated replication role as startup configuration. Sentinel cannot work if role is immutable.

v0.9 adds runtime topology state:

```text
replControlMu
  replRole
  replUpstream
  replPrimary
  replRoleCancel
  replRootCtx
```

The old static `cfg.ReplicationRole` still supplies startup intent, but request authorization and replication behavior consult runtime state.

### Promotion

```text
lock persistenceMu
   ↓
cancel replica-follow role
close old upstream connection
   ↓
create fresh replication.Primary
  → fresh repl-id
  → empty backlog / offset 0
   ↓
open replication listener
   ↓
role = primary
unlock
```

The promoted node is then writable because the client READONLY gate reads the runtime role.

The data itself is not reconstructed during promotion; the replica already owns its v0.8 synchronized state.

## 10. Role-scoped goroutines

A subtle failover bug would be:

```text
replica promoted
    ↓
old runReplicaLoop still alive
    ↓
it reconnects to old primary later
```

v0.9 prevents this with a role-specific context. Promotion/demotion cancels the previous role context and closes the old connection/listener.

Likewise, primary replication handlers capture the concrete `replication.Primary` instance that accepted them, rather than repeatedly dereferencing a server field that may have been replaced by promotion.

## 11. Runtime demotion / FollowPrimary

A node following a different primary performs:

```text
lock persistenceMu
  ↓
cancel old replication role
close old primary listener / upstream connection
  ↓
role = replica
upstream = promoted primary address
current primary feed = nil
remembered repl-id/offset = empty
  ↓
unlock
  ↓
run v0.8 replica loop
  ↓
PSYNC ? -1
  ↓
FULLRESYNC
```

Resetting repl-id/offset is intentional. An offset from the old primary's replication identity must never be interpreted inside a new primary's stream.

## 12. Why dynamic demotion rejects AOF/maxmemory in v0.9

v0.8 replica semantics intentionally disable:

```text
local AOF
local maxmemory eviction policy
```

A dynamically demoted old primary may have started with primary-only policies. Silently keeping them would be incorrect:

- its old AOF would no longer represent the new upstream mutation stream;
- local eviction could independently remove a different key from the chosen primary.

Therefore `FollowPrimary` in v0.9 explicitly refuses dynamic demotion when:

```text
appendonly is active
OR
maxmemory > 0
```

The Sentinel demo/tests run failover-capable nodes with `appendonly=false` and `maxmemory=0`.

This is a deliberate capability boundary, not production Redis behavior.

## 13. Topology repair

After a successful promotion the controller reconfigures reachable surviving replicas immediately.

The failed old primary is skipped while unreachable.

On a later Tick, if the chosen primary is healthy, `repairTopology` checks every reachable node:

```text
already replica of chosen primary?
  yes → nothing
  no  → FollowPrimary(chosen primary)
```

If that node still reports `RolePrimary`, the event is recorded as `rejoined`.

There is no attempt to merge divergent histories. The returning old primary becomes a replica and receives the chosen primary's state via v0.8 synchronization.

## 14. The fencing / split-brain boundary

This is the most important limitation of v0.9.

Suppose monitors cannot reach P but some clients still can:

```text
monitor quorum ──X──> P
clients A ──────────> P

Sentinel promotes R1

clients B ──────────> R1
```

Nothing in v0.9 remotely disables writes on unreachable P. Therefore two writable nodes can exist during the partition.

Sentinel-style health observation alone is not a fencing mechanism.

v0.9 does not claim:

- linearizability;
- split-brain prevention;
- leases;
- quorum writes;
- a replicated consensus log;
- production Redis Sentinel protocol compatibility.

When P becomes reachable to the control plane again, topology repair demotes it and FULLRESYNC makes it follow the chosen primary.

## 15. Failure simulation

The core controller accepts a `ProbeFunc` containing `monitorID`.

Tests can therefore model asymmetric reachability:

```text
s1 cannot reach P
s2 can reach P
s3 can reach P
```

without stopping P itself.

This distinction is essential for testing SDOWN separately from ODOWN.

## 16. Validation model

### Pure control-plane tests

- isolated monitor reaches SDOWN without ODOWN;
- quorum ODOWN promotes exactly once;
- freshest eligible replica wins;
- fresher but quorum-unreachable replica loses;
- repeated Tick after failover does not advance epoch;
- old primary rejoin is observable.

### Real server tests

- running replica can promote to writable primary;
- promotion creates a new repl-id/listener;
- old primary can become READONLY replica;
- old primary and surviving replica converge through v0.8 replication;
- three real data nodes complete an end-to-end Sentinel failover under `go test -race`.

### Demo

```bash
make sentinel-demo
```

prints:

```text
single observer → SDOWN only
quorum observers → ODOWN
epoch / deterministic leader
promotion
surviving replica follows new primary
old primary rejoin
post-repair convergence
split-brain/fencing warning
```

## 17. Runtime state versus database state

Database/logical state:

```text
keys
values
absolute expiry deadlines
```

Replication runtime state:

```text
repl-id
offset
backlog
upstream connection
```

Sentinel runtime state:

```text
monitor observations
SDOWN
ODOWN view
chosen primary
failover epoch
last decision
```

None of the Sentinel state is serialized as database content.

## 18. Next architectural layer

v0.10 changes the topology question again:

```text
v0.9: which node is primary for this replicated dataset?

v0.10: which primary owns this key's hash slot?
```

The next mechanisms are 16384 logical slots, key→slot routing, stable `MOVED`, migration-time `ASK`, and explicit slot ownership state.