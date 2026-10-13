# mini-redis v0.9 — Sentinel-inspired HA

## 1. Identity

- Historical learning branch: `v0.9`
- Previous snapshot: `v0.8`
- Version label: `v0.9.0`
- Theme: monitor-local failure detection, quorum-backed failover, deterministic promotion and replication-topology repair

## 2. Primary question

**Once replication already keeps copies of the data, how can multiple observers decide that the primary is actually unavailable, choose one sufficiently healthy/fresh replica, promote it once for one failover epoch, and repair the replication topology?**

```text
v0.8 data plane
primary → replicas
        ↑
        │ topology only
        │
v0.9 control plane
monitor observations
      ↓
SDOWN votes
      ↓ quorum
ODOWN
      ↓
failover epoch
      ↓
candidate ranking
      ↓
promotion
      ↓
replica reconfiguration
      ↓
old-primary rejoin
```

Sentinel coordinates topology. It never becomes a second database-copy mechanism.

## 3. Delta from v0.8

v0.8 roles were effectively startup topology. v0.9 makes topology runtime-managed.

Added:

- `internal/sentinel` control-plane package;
- independent monitor health histories;
- SDOWN and quorum-backed ODOWN;
- failover epoch and deterministic teaching leader;
- health/freshness candidate ranking;
- real TCP PING health adapter for mini-redis nodes;
- runtime `PromoteToPrimary`;
- runtime `FollowPrimary`;
- role-specific replication contexts/connections;
- fresh repl-id/backlog/listener on promotion;
- surviving-replica reconfiguration;
- returning old-primary repair;
- real three-node Sentinel integration tests;
- deterministic `make sentinel-demo`.

Unchanged:

- Store and object encodings;
- RESP client command semantics;
- v0.8 SnapshotEntry full sync;
- canonical RESP replication mutation stream;
- repl-id/offset/backlog/CONTINUE mechanics;
- transaction and Pub/Sub semantics.

## 4. Failure-state model

Each monitor records, per node:

```text
lastSuccess
lastFailure
failureSince
SubjectiveDown
last successful NodeStatus
```

One monitor failure long enough for `down-after` creates only SDOWN.

ODOWN requires:

```text
number of current-primary SDOWN voters >= quorum
```

Therefore an isolated monitor cannot initiate failover by itself.

## 5. Failover epoch

Every failover attempt increments one in-memory epoch.

The teaching coordinator rule is deterministic:

```text
sort SDOWN monitor IDs
leader = first ID
```

This is intentionally observable and testable. It is not presented as production Redis Sentinel's complete election protocol or as distributed consensus.

## 6. Candidate policy

Eligibility:

```text
quorum can reach candidate
AND
quorum observes candidate role = replica
```

Ranking:

```text
reachable votes DESC
replication offset DESC
node ID ASC
```

This prevents an unreachable replica with a stale high offset from winning over a quorum-visible candidate.

## 7. Promotion boundary

Promotion shares the existing `Server.persistenceMu` logical-order boundary.

```text
lock persistenceMu
      ↓
cancel upstream-follow role
close old replica connection
      ↓
create fresh replication.Primary
      ↓
new repl-id + offset 0 + empty backlog
      ↓
open replication listener
      ↓
runtime role = primary
      ↓
unlock
```

The node becomes writable through the same runtime role checked by the client READONLY gate.

Its database is not copied during promotion; it already owns the synchronized replica state.

## 8. Topology reconfiguration

After promotion:

```text
reachable replica
     ↓
FollowPrimary(new replication address)
     ↓
cancel old role
reset old repl-id/offset
     ↓
PSYNC ? -1
     ↓
FULLRESYNC from new primary
```

Explicit topology changes intentionally reset replication identity/offset because an offset from one primary stream has no meaning in another primary's fresh stream.

## 9. Old-primary return

The failed old primary is not modified while unreachable.

When it becomes reachable again, a later healthy Tick observes that it is not following the chosen primary and runs:

```text
old primary
   ↓ FollowPrimary(chosen primary)
read-only replica
   ↓
FULLRESYNC
   ↓
convergence
```

This is topology repair, not history merge.

## 10. Dynamic-demotion capability boundary

v0.9 dynamically demotable nodes intentionally require:

```text
appendonly = false
maxmemory  = 0
```

`FollowPrimary` rejects dynamic demotion otherwise.

Reason:

- keeping an old-primary AOF after demotion would create a stale durability story;
- keeping an independent maxmemory policy could evict a different victim than the new primary.

Production Redis handles broader combinations; v0.9 does not claim that scope.

## 11. Critical invariants

- one monitor timeout cannot create ODOWN;
- failover does not begin before quorum-backed ODOWN;
- one controller serializes failover Tick execution;
- one failover attempt belongs to one monotonically increasing epoch;
- candidate must be quorum-visible as a replica;
- health precedes freshness in candidate eligibility/ranking;
- runtime role changes cannot split one logical mutation across old/new roles because they share `persistenceMu`;
- promotion creates a fresh replication identity;
- surviving replicas follow the promoted primary using v0.8 replication;
- old-primary return becomes replica repair rather than a second authority selected by the controller;
- Sentinel never serializes or copies database values itself.

## 12. Explicit split-brain limitation

No fencing exists in v0.9.

If monitors lose access to the old primary while some clients still reach it, this can happen:

```text
old primary P remains writable
            +
replica R1 is promoted and becomes writable
```

The control plane cannot remotely disable an unreachable P.

Therefore v0.9 does **not** claim:

- linearizability;
- split-brain prevention;
- leases or fencing tokens;
- quorum writes;
- consensus-log semantics;
- production-compatible Redis Sentinel behavior.

When P becomes reachable again, it is reconfigured to follow the chosen primary.

## 13. Tests that prove the mechanism

Pure Sentinel tests prove:

- isolated SDOWN does not become ODOWN;
- quorum ODOWN creates epoch 1 and one promotion;
- freshness selects the better eligible replica;
- a fresher but quorum-unreachable replica loses;
- repeated healthy Tick does not advance epoch again;
- returning old primary is repaired.

Server topology test proves:

- a synchronized replica can promote at runtime;
- promoted node gets a new repl-id/listener and becomes writable;
- old primary can be dynamically demoted;
- old primary becomes READONLY and converges through replication.

Real integration test proves:

```text
three real data nodes
+ three logical monitor observers
+ primary partition from observers
        ↓
ODOWN
        ↓
r1 promotion
        ↓
r2 follows r1
        ↓
new writes replicate
        ↓
old p returns
        ↓
p follows r1
        ↓
all nodes converge
```

All run under `go test -race ./...`.

## 14. Demo

```bash
make sentinel-demo
```

shows:

```text
one observer failure → SDOWN only
quorum failure       → ODOWN
epoch + leader
candidate promotion
surviving replica reconfiguration
old-primary rejoin
post-repair convergence
explicit fencing/split-brain warning
```

## 15. Read this version in order

```text
internal/sentinel/sentinel.go
  ↓
internal/sentinel/server_node.go
  ↓
internal/server/topology.go
  ↓
internal/server/replication.go
  ↓
internal/sentinel/sentinel_test.go
internal/sentinel/integration_test.go
internal/server/topology_test.go
  ↓
examples/sentinel/main.go
```

## 16. Non-goals

- no production Redis Sentinel wire compatibility;
- no distributed consensus claim;
- no fencing or split-brain prevention;
- no persistent Sentinel epoch/observations;
- no automatic client service discovery;
- no dynamic demotion with local AOF/maxmemory in this version;
- no Cluster/hash slots/MOVED/ASK yet.

## 17. Next version

v0.10 adds the next topology dimension: **Cluster / hash-slot ownership**.

The central question changes from:

> Which replica becomes primary for this dataset?

into:

> Which primary owns this key's slot, and how is the client redirected while slots migrate?

Planned mechanisms: 16384 logical slots, deterministic key→slot mapping, `MOVED`, migration/importing state, `ASK`, and basic topology gossip.