# Roadmap

`Plan.md` is canonical. This file is a short navigator.

```text
v0.1  RESP / command / TTL / AOF
v0.2  epoll event loop / buffers / backpressure
v0.3  Dict / bounded incremental rehash / Object metadata
v0.4  List / Hash / Set / ZSet / adaptive encodings   ← current
v0.5  RDB-like snapshot / AOF rewrite                 ← next
v0.6  maxmemory / expiration refinement / LRU-LFU
v0.7  transactions / PubSub
v0.8  replication / partial resync
v0.9  Sentinel-style failover
v0.10 Cluster / hash slots
v0.11 RESP3
v0.12 observability / profiling
```

The project advances by architecture dependency, not by adding commands as quickly as possible.
