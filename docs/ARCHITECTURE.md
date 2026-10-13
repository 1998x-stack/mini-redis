# Architecture — v0.12

## 1. Current stack

```text
TCP connection
      ↓
command/session/protocol
      ↓
Cluster routing when applicable
      ↓
Server.persistenceMu
      ↓
command registry / Store
      ↓
logical mutation + reply
   ├── AOF / RDB
   ├── replication
   └── RESP2 / RESP3 representation

          existing mechanism events
                    │
                    ↓
          v0.12 observation points
                    │
        ┌───────────┼───────────┐
        ↓           ↓           ↓
     Server      Topology    Sentinel
     Metrics      Metrics     Metrics
        │           │           │
        └──── immutable snapshots ────┐
                                      ↓
                           INFO / demo / profile
```

v0.12's central lesson is:

> **Observability is evidence about mechanisms. It is not another mechanism that mechanisms depend on.**

## 2. Why metrics are not database state

A command counter, epoll wait duration or failover count must never survive because the database survives.

Therefore metrics are intentionally excluded from:

```text
Store Object
SnapshotEntry
RDB
AOF
replication frames
Cluster slot ownership
Sentinel election state
```

Restarting the process resets process-local metrics. Restoring a database does not restore yesterday's latency histogram.

This separation prevents accidental behavior such as replication or migration copying instrumentation state.

## 3. Bounded metrics primitives

`internal/observability` provides the smallest useful building blocks.

### Counter

```text
atomic.Uint64
```

Used for monotonic event counts and byte totals.

### Gauge

```text
atomic.Int64
```

Used for point-in-time values such as active connections.

### Dynamic maps

Labels such as command names or network-engine names are created under a small mutex. Once a label exists, steady-state updates hit an atomic value.

Snapshots allocate a copy:

```text
internal map
    ↓ copy keys + atomic loads
snapshot map
```

Mutating the returned map cannot mutate the metrics owner.

### Fixed-bucket histogram

Latency histograms keep:

```text
fixed upper bounds
bucket counters
count
sum_ns
max_ns
```

They never retain individual samples. Runtime memory therefore does not grow with request count.

## 4. Three separate observation owners

A single global metrics object would blur ownership that earlier versions deliberately separated.

v0.12 keeps three surfaces.

### Data plane: `Server.MetricsSnapshot()`

Owns evidence for:

- client commands/errors/latency;
- protocol request/reply mix;
- connection and transport bytes;
- reactor poll behavior;
- expiration/eviction/maintenance;
- AOF/RDB/rewrite work;
- server-visible replication state.

### Cluster control plane: `Topology.MetricsSnapshot()`

Owns evidence for:

- route decisions;
- MOVED / ASK / CLUSTERDOWN;
- CROSSSLOT;
- bounded gossip work;
- migration attempts/keys/errors/duration.

### Sentinel control plane: `Controller.MetricsSnapshot()`

Owns evidence for:

- Tick calls/errors/duration;
- Probe calls/failures/duration;
- failover attempts/duration;
- promotions/reconfigurations;
- current epoch/primary/last decision.

These snapshots can be displayed together, but their mutable state is never merged.

## 5. Command observation boundary

The shared session path is wrapped by `executeObservedClientCommand`:

```text
capture start + current protocol
       ↓
record command request
       ↓
execute existing session/command mechanism
       ↓
record duration + error outcome
```

The observer never changes:

- command validation;
- MULTI queueing;
- WATCH conflict logic;
- Store mutation;
- AOF/replication publication.

`INFO` is handled as session control/read-only introspection and returns a logical Map. RESP2 clients receive the existing v0.11 Map→flat-array downgrade; RESP3 clients receive a Map.

## 6. Network evidence measures actual I/O

Counting `len(EncodeValue(...))` would describe a planned payload, not what the socket actually accepted.

v0.12 instead instruments the transport boundary.

### Legacy engine

A transparent `metricConn` wraps the accepted `net.Conn`:

```text
net.Conn.Read  → actual n → bytes_read
net.Conn.Write → actual n → bytes_written
```

The wrapper does not buffer or retry.

### Reactor engine

The reactor already owns raw file descriptors, so instrumentation sits beside:

```text
syscall.Read
syscall.Write
Poller.Wait
```

It records:

- actual read/write bytes;
- epoll wait calls;
- returned event count;
- wait duration;
- input/output buffer-limit events.

The reactor remains the owner of scheduling, buffers and backpressure. Metrics do not affect interest masks or readiness decisions.

## 7. Expiration, eviction and maintenance

Expiration and eviction remain different mechanisms.

v0.12 observes them where they already become explicit:

```text
active maintenance ExpireDue
  → expired_keys

maxmemory EnforceMaxMemory
  → pressure.ExpiredKeys
  → pressure.EvictedKeys
```

Dictionary maintenance also exposes how much bounded rehash work a cycle performed.

A historical test directly constructs a minimal `Server` without `New()`. v0.12 therefore keeps instrumentation optional: if `metrics == nil`, maintenance and memory-policy mechanisms still execute normally. This is an intentional observer-independence invariant, not only a testing convenience.

## 8. Persistence evidence wraps real operations

### AOF append

The server wraps the existing append call:

```text
canonical commands
      ↓
calculate canonical encoded bytes
      ↓
real AOF AppendBatch / AppendTransaction
      ↓
ops / bytes / failures / duration
```

The observer does not change which commands are persisted or fsync policy.

### RDB save

`SaveRDB` measures the real snapshot-save operation and reads the resulting file size after success.

### AOF rewrite

`RewriteAOF` measures the established rewrite path and resulting published AOF size.

The distinction matters: persistence latency is evidence about durability work, not automatically a Store latency problem.

## 9. Replication evidence and the ACK boundary

A primary can truthfully know:

```text
repl-id
current canonical offset
backlog start
retained backlog byte window
FULLRESYNC count
CONTINUE count
live subscribers
```

A replica can truthfully know locally:

```text
repl-id
applied offset
FULLRESYNC applied
CONTINUE applied
frames applied
bytes applied
link failures
```

But v0.8 does **not** send replica acknowledgements back to the primary.

Therefore v0.12 deliberately does not expose:

```text
primary-visible authoritative remote replica applied offset
primary-visible authoritative replica lag
```

Inventing that number would make observability lie about the protocol. Adding ACK would be a future replication-protocol feature, not a metrics-only change.

## 10. Cluster metrics remain downstream of policy

The route algorithm still computes:

```text
EXECUTE / MOVED / ASK / CLUSTERDOWN
```

from slot ownership/migration state and key existence.

Only after the decision is produced does the Topology observer increment the corresponding counter.

CROSSSLOT is recorded only after `CommonSlot` has already rejected the request.

Migration observation wraps the existing `MigrateSlot` contract:

```text
BeginMigration
  ↓
TransferKey ...
  ↓
CommitMigration / error
  ↓
record run / successful-key count / error / duration
```

No metric is consulted by ownership or migration commit.

Gossip metrics count bounded work performed: pages emitted/merged and records processed. They do not claim unique topology-record cardinality.

## 11. Sentinel metrics remain downstream of election policy

`Controller.Tick` still owns:

```text
Probe
  ↓
monitor-local SDOWN
  ↓
quorum ODOWN
  ↓
epoch
  ↓
candidate ranking
  ↓
promotion / reconfiguration
```

v0.12 wraps those existing operations with timers/counters.

Candidate ranking still reads only topology/probe state:

```text
reachable votes DESC
replication offset DESC
node ID ASC
```

It never reads probe latency, failover count or any other metric.

Metrics lifecycle belongs directly to the Controller rather than a global pointer table, so observation does not accidentally retain old controllers.

## 12. INFO is a teaching view, not mutable state

`INFO [section]` is assembled from `Server.MetricsSnapshot()`.

Current sections include:

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

It intentionally does not hide Cluster/Sentinel state inside Server. Those have their own snapshots because they are separate control-plane owners.

INFO does not reset counters, mutate keyspace, change protocol policy or expose internal maps by reference.

## 13. Microbenchmark versus end-to-end workload

v0.12 keeps two measurement layers explicit.

### Instrumentation microbenchmark

```bash
make benchmark-observability
```

Answers questions such as:

- what is the steady-state cost of one atomic counter increment?
- what is the cost of a hot CounterMap label?
- what is the cost of placing a duration into a fixed histogram?

### End-to-end network benchmark

```bash
make benchmark
```

Uses the existing legacy/reactor comparison harness and includes TCP, parsing, command dispatch, Store and reply handling.

A microbenchmark result must never be presented as server throughput.

## 14. Profiling workflow

The recommended workflow is evidence-first:

```text
reproduce workload
      ↓
capture metrics snapshot
      ↓
identify suspicious mechanism
      ↓
profile
      ↓
change one mechanism
      ↓
rerun identical workload
      ↓
compare correctness + metrics + profile
```

`docs/PERFORMANCE.md` contains the benchmark metadata contract and pprof commands.

## 15. Validation model

### Primitive tests

- concurrent atomics/maps under `-race`;
- histogram boundedness/count consistency;
- returned maps are copies.

### Real network tests

On legacy and reactor:

- actual connections and bytes increase;
- command counts/latency increase;
- HELLO changes request protocol mix as expected;
- INFO does not mutate keyspace;
- reactor poll evidence is non-zero on reactor.

### Persistence tests

Real temporary AOF/RDB files prove operation counts, byte evidence and duration observations.

### Control-plane tests

- Cluster tests drive real MOVED, ASK, CROSSSLOT, migration and gossip paths;
- Sentinel tests drive real down-after/quorum/election/promotion paths.

### Demo

```bash
make observability-demo
```

shows server, Cluster and Sentinel evidence side by side.

## 16. Relationship to the full learning arc

```text
v0.1  request/state/AOF
v0.2  event loop
v0.3  Dict/rehash
v0.4  collections/encodings
v0.5  RDB/AOF rewrite
v0.6  maxmemory/eviction
v0.7  transactions/PubSub
v0.8  replication
v0.9  Sentinel
v0.10 Cluster
v0.11 RESP3
v0.12 observability/performance
```

The first eleven versions expose mechanisms. v0.12 makes those mechanisms measurable enough that a future optimization can begin from evidence instead of intuition.

## 17. Explicit limits

v0.12 does not claim:

- Prometheus/OpenTelemetry compatibility;
- distributed metrics aggregation;
- production monitoring/SLO infrastructure;
- zero-cost instrumentation;
- production Redis performance parity;
- portable benchmark numbers across different machines;
- remote replica lag that the current replication protocol cannot actually know.

A future version should start a new learning arc only after measurements identify one concrete mechanism worth replacing or extending.
