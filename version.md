# mini-redis v0.9 — Sentinel-inspired HA

## 1. Identity

- Active development branch: `v0.9`
- Previous snapshot: `v0.8`
- Target version label: `v0.9.0`
- Theme: health observation, SDOWN/ODOWN, failover epoch, deterministic replica promotion and topology reconfiguration

## 2. Primary question

**Once replication already keeps copies of the data, how can multiple observers decide that the primary is actually unavailable, choose one sufficiently healthy/fresh replica, promote it exactly once for one failover epoch, and repair the replication topology?**

v0.9 adds a control plane above v0.8:

```text
                   Sentinel monitors
                 /        |        \
             probe      probe      probe
               \          |          /
                \      observations /
                 ↓        ↓        ↓
                  SDOWN / quorum ODOWN
                           ↓
                      failover epoch
                           ↓
                   candidate selection
                           ↓
                        promotion
                           ↓
                 topology reconfiguration
                           ↓
              v0.8 replication continues
```

Sentinel coordinates replication topology. It does **not** implement a second data-copy protocol.

## 3. Planned teaching policy

- each Sentinel has its own subjective health state;
- one Sentinel timeout produces only SDOWN;
- ODOWN requires configured quorum of Sentinel observations;
- failover decisions are tagged with a monotonically increasing epoch;
- one deterministic leader owns one failover attempt for an epoch;
- promotion ranking prefers reachable replicas, then freshest replication offset, then stable node ID tie-break;
- promoted replica becomes writable primary and creates a new replication identity;
- surviving replicas are reconfigured to follow the promoted primary;
- an old primary that returns is demoted/reconfigured as a replica of the new primary;
- failover reuses v0.8 full/partial sync semantics after topology changes.

## 4. Required validation

- isolated monitor may declare SDOWN but cannot create ODOWN without quorum;
- quorum-backed ODOWN advances one failover epoch;
- deterministic leader/candidate selection for the same observations;
- stale/unreachable replica loses to a healthier/fresher candidate;
- promoted node accepts client writes;
- surviving replicas follow promoted node and converge through v0.8 replication;
- old primary rejoins as replica;
- repeated observations in one epoch do not cause repeated promotions;
- race tests cover monitor observations, election state and topology changes;
- deterministic Sentinel demo prints SDOWN → ODOWN → election → promotion → repair.

## 5. Non-goals

- no production-compatible Redis Sentinel wire protocol completeness;
- no geographically distributed consensus claim;
- no Cluster/hash slots/MOVED/ASK;
- no client discovery protocol beyond the teaching demo;
- no second replication mechanism.

## 6. Definition of Done

The exact `v0.9` SHA must pass repository baseline validation plus Sentinel-specific unit/integration/demo tests. Only then may `main` fast-forward to the same SHA and re-run CI.

## 7. Next version

v0.10 will add Cluster/hash-slot ownership only after Sentinel-style topology failover is explicit and tested.