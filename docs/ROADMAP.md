# mini-redis roadmap navigator

`Plan.md` is the canonical roadmap. This file is only the short learning sequence.

```text
v0.1  RESP → command → Store → TTL → AOF
  ↓
v0.2  nonblocking sockets → epoll → reactor → client buffers/backpressure
  ↓
v0.3  Dict → collisions → two hash tables → incremental rehash → object metadata
  ↓
v0.4  List / Hash / Set / ZSet → internal encodings → skip list
  ↓
v0.5  RDB → AOF rewrite → restart/recovery trade-offs
  ↓
v0.6  maxmemory → expiration refinement → LRU/LFU eviction
  ↓
v0.7  MULTI/EXEC/WATCH → Pub/Sub
  ↓
v0.8  replication → offsets → backlog → partial resync
  ↓
v0.9  Sentinel-style monitoring/election/failover
  ↓
v0.10 Cluster → hash slots → MOVED/ASK → migration/gossip
  ↓
v0.11 RESP3
  ↓
v0.12 observability / profiling / performance engineering
```

Current integrated milestone: **v0.2**.
