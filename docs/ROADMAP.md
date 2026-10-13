# Roadmap

`Plan.md` is canonical. This file is a short navigator.

```text
v0.1  RESP / command / TTL / AOF
v0.2  epoll event loop / buffers / backpressure
v0.3  Dict / bounded incremental rehash / Object metadata
v0.4  List / Hash / Set / ZSet / adaptive encodings
v0.5  logical snapshots / RDB / AOF rewrite
v0.6  maxmemory / expiration vs eviction / sampled LRU-LFU
v0.7  WATCH / MULTI-EXEC / PubSub
v0.8  replication / full sync / repl-id+offset / backlog / partial resync
v0.9  SDOWN / ODOWN / epoch / promotion / topology repair
v0.10 16384 slots / MOVED / ASK / ASKING / migration / bounded gossip
v0.11 HELLO 2/3 / RESP3 richer types / Push / RESP2 downgrade         ← current
v0.12 observability / metrics / profiling / repeatable performance     ← next
```

The project advances by architecture dependency, not by adding commands as quickly as possible.
