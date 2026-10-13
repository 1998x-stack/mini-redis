# Roadmap

`Plan.md` is canonical. This file is a short navigator.

```text
v0.1  RESP / command / TTL / AOF
v0.2  epoll event loop / buffers / backpressure
v0.3  Dict / bounded incremental rehash / Object metadata
v0.4  List / Hash / Set / ZSet / adaptive encodings
v0.5  logical snapshots / RDB / AOF rewrite
v0.6  maxmemory / expiration vs eviction / sampled LRU-LFU   ← current
v0.7  transactions / WATCH / PubSub                           ← next
v0.8  replication / partial resync
v0.9  Sentinel-style failover
v0.10 Cluster / hash slots
v0.11 RESP3
v0.12 observability / profiling
```

The project advances by architecture dependency, not by adding commands as quickly as possible.
