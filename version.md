# mini-redis v0.12 — Observability / Performance

## 1. Identity

- Historical learning branch: `v0.12`
- Previous snapshot: `v0.11`
- Version label: `v0.12.0`
- Theme: observer-only bounded metrics, immutable snapshots, latency/byte evidence and repeatable profiling

## 2. Primary question

**Once Redis mechanisms are explicit, how can we measure where time, queueing, durability cost, memory pressure, replication work and topology work are spent without changing their semantics?**

```text
existing mechanism
       ↓
real event / operation boundary
       ↓
observer-only counter / histogram
       ↓
immutable snapshot
       ↓
INFO / demo / benchmark / profile
```

The dominant lesson is:

> **Instrumentation is an observer of mechanism state and events. It is never database state, routing state or mutation policy.**

## 3. Delta from v0.11

Added:

- `internal/observability` package;
- atomic monotonic counters and signed gauges;
- dynamic label maps with synchronized creation and copy-based snapshots;
- fixed-bucket bounded latency histograms storing count/sum/max but no raw samples;
- command call/error/latency observation;
- RESP2/RESP3 request/reply and Push counts;
- accepted/active connection metrics;
- real legacy socket read/write byte counts;
- real reactor syscall read/write byte counts;
- reactor poll calls/events/wait histogram and buffer-limit events;
- expiration, eviction, maintenance and bounded rehash-work counts;
- AOF append, RDB save and AOF rewrite ops/bytes/failures/durations;
- primary replication identity/offset/backlog/full/partial-sync/subscriber snapshot;
- replica-local full/partial sync, link failure and applied frame/byte evidence;
- `Server.MetricsSnapshot()`;
- read-only `INFO [section]` teaching view;
- `Topology.MetricsSnapshot()` for Cluster routing/migration/gossip evidence;
- `Controller.MetricsSnapshot()` for Sentinel probe/failover evidence;
- race-tested primitive, wire, persistence and control-plane observability tests;
- `make observability-demo`;
- `make benchmark-observability`;
- `docs/PERFORMANCE.md` profiling and workload-reporting contract.

Unchanged:

- command semantics and mutation ordering;
- Store/Object encodings;
- AOF/RDB logical formats and fsync policy;
- replication wire frames and offset rules;
- Sentinel SDOWN/ODOWN/quorum/candidate policy;
- Cluster slot ownership/MOVED/ASK/migration policy;
- RESP2/RESP3 negotiation and reply adaptation.

## 4. Observer-only invariant

Metrics never become inputs to the mechanisms they describe.

They do not decide:

```text
command success
expiration
eviction
AOF persistence
replication publication
MOVED / ASK
slot migration commit
SDOWN / ODOWN
candidate ranking
promotion
protocol negotiation
```

Metrics also never enter:

```text
Store
SnapshotEntry
AOF / RDB
replication frames
Cluster slot records
Sentinel election state
```

Established mechanisms must still work when instrumentation is absent. This is tested by historical minimal-Server construction paths where `metrics == nil`.

## 5. Metrics primitives

### Atomic counters/gauges

Hot monotonic/signed updates use `sync/atomic` rather than sharing Store/topology locks.

### Dynamic labels

Command/engine label creation uses a small mutex. Existing-label increments remain atomic.

### Bounded histograms

Duration histograms use a fixed bucket set plus:

```text
count
sum_ns
max_ns
```

No individual samples are retained, so memory use stays bounded independently of request volume.

### Snapshot copying

Dynamic maps are copied on snapshot. Mutating a returned map cannot mutate internal metrics.

## 6. Server data-plane snapshot

`Server.MetricsSnapshot()` exposes:

### Command/session/protocol

- per-command calls/errors;
- command latency histogram;
- RESP2/RESP3 requests;
- RESP2/RESP3 replies;
- Push replies.

### Network

- accepted and active connections;
- actual read/write bytes by engine;
- reactor poll calls/events/wait histogram;
- input/output backpressure events.

### State/memory

- key count at snapshot time;
- memory-policy stats when available;
- expired/evicted keys;
- maintenance cycles and bounded rehash work.

### Persistence

- AOF append ops/bytes/failures/duration;
- RDB save ops/resulting bytes/failures/duration;
- AOF rewrite ops/resulting bytes/failures/duration.

### Replication

- role / repl-id / offset / upstream;
- primary backlog start/window;
- full/partial sync counts;
- live primary subscribers;
- replica-local link failures;
- replica full/partial syncs applied;
- frames/bytes applied.

## 7. INFO

`INFO [section]` is a read-only session command assembled from a `Server.MetricsSnapshot()`.

Sections:

```text
server
clients
commands
protocol
persistence
replication
memory
reactor
```

It returns a logical Map. RESP3 encodes it as Map; RESP2 uses the existing v0.11 downgrade path.

INFO does not reset counters or mutate database state and does not expose metric maps by reference.

Cluster/Sentinel are not folded into INFO because they own separate control-plane state.

## 8. Real network evidence

Legacy uses a transparent `metricConn` wrapper around actual `net.Conn` operations.

Reactor records at the actual syscall/poller boundary:

```text
syscall.Read
syscall.Write
Poller.Wait
```

This avoids treating encoded payload length as if it were successful socket I/O.

Both legacy and reactor share the same command/session metrics semantics.

## 9. Persistence evidence

AOF append timing wraps the real append call. RDB save and AOF rewrite timing wrap the established real persistence operations.

Successful RDB/rewrite byte evidence comes from resulting files. Persistence observation never changes which logical commands are durable or fsync policy.

## 10. Replication evidence and an explicit limit

The primary can truthfully expose:

```text
repl-id
canonical end offset
backlog start / retained window
FULLRESYNC count
CONTINUE count
live subscribers
```

The replica can truthfully expose locally:

```text
repl-id
applied offset
full/partial sync applied
frames/bytes applied
link failures
```

v0.8 has no replica→primary acknowledgement protocol. Therefore v0.12 does **not** invent an authoritative primary-visible remote applied offset or replica lag metric.

Adding such a metric correctly would first require a replication protocol change.

## 11. Cluster control-plane snapshot

`Topology.MetricsSnapshot()` exposes:

- routes and executes;
- MOVED / ASK / CLUSTERDOWN;
- CROSSSLOT;
- gossip pages, records processed and merges;
- migration runs, successful key transfers, errors and duration.

Route metrics are recorded after route decisions. Migration timing wraps existing `MigrateSlot`; no metric participates in ownership/commit decisions.

## 12. Sentinel control-plane snapshot

`Controller.MetricsSnapshot()` exposes:

- Tick count/errors/duration;
- Probe count/failures/duration;
- failover attempts/duration;
- promotions/reconfigurations;
- current epoch, primary and last decision.

The existing candidate ranking remains:

```text
reachable votes DESC
replication offset DESC
node ID ASC
```

No metric is consulted by that ranking.

Metrics are owned by each Controller, not by a process-global pointer registry.

## 13. Tests that prove the mechanism

### Primitive

- concurrent counters/maps under `go test -race`;
- histogram bucket totals/count/max and bounded structure;
- snapshot mutation isolation.

### Server/wire

On both network engines:

- real commands increment command counts/latency;
- HELLO changes protocol-request mix on the correct request boundary;
- actual socket bytes become non-zero;
- accepted/active connections are visible;
- reactor poll evidence is non-zero;
- INFO remains read-only.

### Persistence

Real temporary AOF/RDB files prove operation counts, byte evidence and duration observations.

### Cluster/Sentinel

- real MOVED/ASK/CROSSSLOT/gossip/migration paths produce Cluster evidence;
- real down-after/quorum/election/promotion paths produce Sentinel evidence.

Everything remains under the historical `go test -race ./...` gate.

## 14. Demo

```bash
make observability-demo
```

It drives a real TCP server through:

```text
SET
PING
HELLO 3
INFO commands
```

and then prints coherent Server evidence. It separately drives Cluster MOVED/ASK/gossip and a Sentinel failover, then prints those control-plane snapshots.

The demo's timing values are evidence from that run, not benchmark claims.

## 15. Performance workflow

Instrumentation microbenchmark:

```bash
make benchmark-observability
```

End-to-end legacy/reactor workload:

```bash
make benchmark
```

`docs/PERFORMANCE.md` requires performance claims to include commit SHA, Go/OS/CPU and workload configuration, and documents `go test -cpuprofile` + `go tool pprof` usage.

Microbenchmarks and end-to-end workloads are deliberately treated as different forms of evidence.

## 16. Critical invariants

- metrics never become database/persistence/replication/topology state;
- metrics never make business/control-plane decisions;
- established mechanisms work without a metric sink;
- dynamic maps returned by snapshots are copies;
- histogram memory is bounded independently of event count;
- socket-byte counters observe actual I/O boundaries;
- persistence metrics wrap real persistence operations;
- Cluster/Sentinel metrics remain owned by their control planes;
- no primary-visible remote replica lag is claimed without ACK protocol support;
- benchmark claims always name workload/configuration.

## 17. Explicit non-goals

v0.12 does not claim:

- Prometheus/OpenTelemetry compatibility;
- distributed metric aggregation;
- production alerting/SLO infrastructure;
- zero-cost instrumentation;
- production Redis performance parity;
- benchmark portability without workload/environment metadata;
- authoritative remote replica lag unavailable to the current protocol.

## 18. Read this version in order

```text
internal/observability/metrics.go
  ↓
internal/server/metrics.go
internal/server/command_metrics.go
  ↓
internal/server/legacy.go
internal/server/reactor_linux.go
  ↓
internal/server/server.go
internal/server/replication.go
  ↓
internal/cluster/metrics.go
internal/sentinel/metrics.go
  ↓
internal/server/observability_test.go
internal/cluster/metrics_test.go
internal/sentinel/metrics_test.go
  ↓
examples/observability/main.go
  ↓
docs/PERFORMANCE.md
```

## 19. Completion of the original arc

v0.12 completes the initial dependency-ordered mini-redis learning sequence.

A future v0.13 should begin a new learning arc only after measured evidence or a new systems question justifies one mechanism to extend or replace.
