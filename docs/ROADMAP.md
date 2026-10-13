# Roadmap

`Plan.md` is canonical. This file is a short navigator.

```text
v0.1  RESP / command / TTL / AOF
v0.2  epoll event loop / buffers / backpressure
v0.3  Dict / bounded incremental rehash / Object metadata
v0.4  List / Hash / Set / ZSet / adaptive encodings
v0.5  logical snapshots / RDB / AOF rewrite
v0.6  maxmemory / expiration vs eviction / sampled LRU-LFU
v0.7  WATCH / MULTI-EXEC / PubSub                         ← current
v0.8  replication / full sync / backlog / partial resync ← next
v0.9  Sentinel-style failover
v0.10 Cluster / hash slots
v0.11 RESP3
v0.12 observability / profiling
```

The project advances by architecture dependency, not by adding commands as quickly as possible.
