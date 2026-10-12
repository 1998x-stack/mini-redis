# Roadmap navigator

`Plan.md` is the canonical detailed roadmap. This file is only the short learning sequence.

```text
v0.1  request -> state -> TTL -> AOF
  ↓
v0.2  nonblocking sockets -> epoll -> event loop -> client buffers
  ↓
v0.3  Dict -> collisions -> ht[0]/ht[1] -> bounded incremental rehash -> Object encoding
  ↓
v0.4  List / Hash / Set / ZSet -> multiple internal encodings -> skip list
  ↓
v0.5  RDB + AOF rewrite
  ↓
v0.6  maxmemory + expiration/eviction + approximate LRU/LFU
  ↓
v0.7  MULTI/EXEC/WATCH + Pub/Sub
  ↓
v0.8  replication offsets/backlog + partial resync
  ↓
v0.9  Sentinel-inspired failover
  ↓
v0.10 Cluster hash slots + MOVED/ASK
  ↓
v0.11 RESP3
  ↓
v0.12 observability + profiling/performance engineering
```

Current integrated target after validation: **v0.3**.

Next dependency: **v0.4**, because collection encodings need the type/encoding and dictionary boundaries made explicit in v0.3.
