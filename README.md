# mini-redis

An architecture-driven Redis learning implementation in Go. Each version makes one hidden Redis mechanism explicit, small and testable.

## Current milestone: v0.12

```text
v0.1  request → command → state → TTL → AOF
  ↓
v0.2  epoll event loop
  ↓
v0.3  Redis-like Dict + incremental rehash
  ↓
v0.4  List / Hash / Set / ZSet + adaptive encodings
  ↓
v0.5  RDB-like snapshots + AOF rewrite
  ↓
v0.6  maxmemory + sampled LRU/LFU
  ↓
v0.7  WATCH + MULTI/EXEC + Pub/Sub
  ↓
v0.8  replication / PSYNC / backlog
  ↓
v0.9  Sentinel-inspired failover
  ↓
v0.10 Cluster / 16384 slots / MOVED / ASK
  ↓
v0.11 HELLO 2/3 + richer RESP3 values + Push
  ↓
v0.12 observability / profiling / performance evidence  ← current
```

## v0.12 mental model

```text
existing mechanism
      ↓
real event / duration / byte count
      ↓
observer-only metric
      ↓
immutable snapshot
      ↓
INFO / demo / benchmark / profile
```

The central lesson is:

> **Metrics explain a mechanism; the mechanism never consults metrics to decide what to do.**

Instrumentation does not enter Store, `SnapshotEntry`, AOF, RDB, replication frames, Cluster slot metadata or Sentinel election state.

## What is observable

### Commands and protocol

- command calls, errors and bounded latency histogram;
- RESP2 / RESP3 requests and replies;
- RESP3 Push count;
- accepted and active connections.

### Network engines

Legacy counts actual bytes accepted by `net.Conn.Read/Write`.

The Linux reactor counts actual `syscall.Read/Write` bytes plus:

```text
epoll wait calls
returned events
wait-duration histogram
input/output buffer-limit events
```

### State and persistence

- expired / evicted keys;
- maintenance and rehash work;
- current key count / memory-policy snapshot;
- AOF append operations, bytes, failures and duration;
- RDB save operations, file bytes, failures and duration;
- AOF rewrite operations, resulting bytes, failures and duration.

### Replication

Primary evidence:

```text
repl-id
current offset
backlog start / retained bytes
FULLRESYNC count
CONTINUE count
live subscribers
```

Replica-local evidence:

```text
applied offset
full/partial syncs applied
frames / bytes applied
link failures
```

v0.8 has no replica→primary ACK protocol, so v0.12 deliberately does **not** invent a primary-visible authoritative replica-lag number.

### Cluster and Sentinel

Cluster exposes a separate `Topology.MetricsSnapshot()` for routes, MOVED/ASK/CROSSSLOT, migration and bounded gossip work.

Sentinel exposes a separate `Controller.MetricsSnapshot()` for Tick/probe/failover timing, probe failures, promotions, reconfiguration and current decision state.

They remain separate control-plane owners rather than being hidden inside Server metrics.

## INFO

A client can inspect the server data-plane snapshot with:

```text
INFO
INFO server
INFO clients
INFO commands
INFO protocol
INFO persistence
INFO replication
INFO memory
INFO reactor
```

`INFO` is read-only. It returns a logical Map; RESP3 clients receive a native Map and RESP2 clients use the v0.11 Map→flat-array compatibility encoding.

## Bounded histograms

Latency observations use fixed buckets plus:

```text
count
sum_ns
max_ns
```

Individual samples are never retained, so histogram memory does not grow with request count.

## Learning demos

```bash
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make session-demo
make replication-demo
make sentinel-demo
make cluster-demo
make resp3-demo
make observability-demo   # real TCP + Server + Cluster + Sentinel snapshots
```

## Performance workflow

Instrumentation microbenchmarks:

```bash
make benchmark-observability
```

End-to-end legacy/reactor workload:

```bash
make benchmark
```

See `docs/PERFORMANCE.md` for benchmark metadata requirements and pprof usage. A performance result without commit/environment/workload configuration is intentionally treated as incomplete evidence.

## Validation

```bash
gofmt -d .
go vet ./...
go test -race ./...
go build ./cmd/mini-redis-server
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make session-demo
make replication-demo
make sentinel-demo
make cluster-demo
make resp3-demo
make observability-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read v0.12 in this order

```text
internal/observability/metrics.go
  ↓ atomic counters / gauges / bounded histograms
internal/server/metrics.go
  ↓ immutable data-plane snapshot + INFO
internal/server/command_metrics.go
  ↓ command / protocol observation boundary
internal/server/legacy.go
internal/server/reactor_linux.go
  ↓ actual wire bytes / poll evidence
internal/server/server.go
internal/server/replication.go
  ↓ persistence / memory / replication evidence
internal/cluster/metrics.go
internal/sentinel/metrics.go
  ↓ separate control-plane snapshots
internal/server/observability_test.go
internal/cluster/metrics_test.go
internal/sentinel/metrics_test.go
  ↓ invariant tests
examples/observability/main.go
  ↓ cross-layer demo
docs/PERFORMANCE.md
  ↓ benchmark + profiling discipline
```

## Original learning arc complete

v0.1–v0.12 now cover:

```text
protocol
network scheduling
dictionary internals
object encodings
persistence
memory pressure
transactions / PubSub
replication
high availability
sharding
RESP3
observability / profiling
```

A future v0.13 should begin a new learning arc from measured evidence or one explicit systems question rather than simply adding another Redis feature.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical dependency-ordered roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
- `docs/PERFORMANCE.md` — measurement and profiling workflow.
