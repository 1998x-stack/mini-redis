# Roadmap Navigator

`Plan.md` at the repository root is the **canonical development plan**. This file is intentionally short so the repository does not accumulate two competing roadmaps.

## Version sequence

| Version | Primary mental model |
|---|---|
| v0.1 | TCP → RESP → command → store → TTL → AOF |
| v0.2 | nonblocking sockets + epoll event loop + client buffers/backpressure |
| v0.3 | custom Dict + incremental rehash + object metadata |
| v0.4 | List/Hash/Set/ZSet + internal encodings + skip list |
| v0.5 | RDB + AOF rewrite + recovery trade-offs |
| v0.6 | active expiry refinement + maxmemory + eviction/LRU/LFU |
| v0.7 | MULTI/EXEC/WATCH + Pub/Sub |
| v0.8 | replication offsets/backlog + full/partial resync |
| v0.9 | Sentinel-inspired monitoring/election/failover |
| v0.10 | Cluster hash slots + MOVED/ASK + migration/gossip |
| v0.11 | RESP3 and richer client semantics |
| v0.12 | observability + profiling + performance experiments |

For scope, dependency rationale, required mechanisms, invariants, non-goals and Definition of Done, read [`../Plan.md`](../Plan.md).
