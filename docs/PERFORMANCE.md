# Performance and Profiling — v0.12

v0.12 treats performance numbers as evidence attached to a workload, not as timeless properties of the implementation.

## 1. Two different questions

```text
microbenchmark
  ↓
How expensive is one mechanism?

end-to-end benchmark
  ↓
How does the complete TCP → parser → command → Store → reply path behave?
```

Do not compare the two as if they measured the same thing.

## 2. Observability overhead microbenchmarks

Run:

```bash
make benchmark-observability
```

This executes steady-state benchmarks for:

- one atomic counter increment;
- one hot-label `CounterMap` increment after label creation;
- one fixed-bucket histogram observation.

For a profile rather than a single `ns/op` number:

```bash
go test -run '^$' -bench 'Benchmark(Counter|Histogram)' \
  -benchmem -cpuprofile cpu.pprof ./internal/observability

go tool pprof cpu.pprof
```

The histogram is deliberately bounded: it stores fixed bucket counters plus count/sum/max and never stores individual latency samples.

## 3. Network engine comparison

The existing comparison harness remains the end-to-end path:

```bash
make benchmark
```

It compares the legacy goroutine-per-client transport with the Linux reactor under the script's declared workload.

Before quoting a result, record at least:

```text
commit SHA
Go version
OS / CPU
network engine
connections
operations per connection
pipeline depth
command mix
payload size
AOF on/off + fsync policy
maxmemory policy
```

A statement such as “reactor is 20% faster” without these parameters is invalid for this project.

## 4. Read the metrics beside the benchmark

`INFO` and `Server.MetricsSnapshot()` expose evidence that helps explain a benchmark rather than merely ranking two runs:

```text
commands + errors
command latency histogram
connections + actual socket bytes
reactor poll calls / events / wait histogram
expiration / eviction work
AOF / RDB / rewrite bytes and latency
replication offsets / backlog / sync counts
RESP2 / RESP3 request and reply mix
```

Cluster and Sentinel intentionally expose their own snapshots:

```go
clusterTopology.MetricsSnapshot()
sentinelController.MetricsSnapshot()
```

They are separate control-plane owners and are not hidden inside the data-plane `Server` snapshot.

## 5. Replication lag limitation

v0.8 replication has no ACK message from replica back to primary. Therefore v0.12 does **not** invent an authoritative remote-replica lag metric.

What can be measured truthfully is:

```text
primary:
  current replication offset
  backlog start / retained byte window
  full sync count
  partial sync count
  live subscribers

replica (locally):
  replication id
  applied offset
  full/partial syncs applied
  frames / bytes applied
  link failures
```

A future version would need to add an acknowledgement protocol before a primary could calculate remote applied-offset lag. That would be a replication protocol change, not an observability-only change.

## 6. Profile before optimizing

A useful workflow is:

```text
1. reproduce workload
2. capture INFO / MetricsSnapshot
3. identify the mechanism dominating evidence
4. profile that mechanism
5. change one mechanism
6. rerun the identical workload
7. compare correctness + metrics + profile
```

Examples:

- high reactor poll wait with low event count may mean the workload is mostly idle;
- high command latency with low network bytes points away from socket throughput;
- high AOF append latency under `appendfsync=always` is durability evidence, not necessarily a Store problem;
- increasing FULLRESYNC count suggests backlog/identity/reconnect behavior should be investigated before optimizing command execution;
- high MOVED/ASK counts are topology/routing evidence, not a hash-table benchmark.

## 7. What v0.12 does not claim

- production Redis throughput parity;
- zero instrumentation overhead;
- portable benchmark numbers across different machines;
- Prometheus/OpenTelemetry compatibility;
- distributed metric aggregation;
- production alerting or SLO tooling.

The purpose is to make each educational mechanism measurable enough that later optimization begins from evidence rather than intuition.
