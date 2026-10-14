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
v0.11 HELLO 2/3 / RESP3 richer types / Push / RESP2 downgrade
v0.12 observer-only metrics / INFO / profiling / performance evidence
v0.13 replica ACK / applied offset / authoritative byte lag             ← current
```

v0.1–v0.12 form the original architecture-learning sequence.

v0.13 begins the second evidence-driven arc from one limitation revealed by v0.12: without receiver acknowledgements, a primary cannot truthfully know remote applied progress.

The v0.13 progression is:

```text
primary sends canonical stream
      ↓
replica applies complete boundary
      ↓
REPLCONF ACK applied-offset
      ↓
primary live progress registry
      ↓
authoritative protocol lag_bytes
```

Future milestones are intentionally uncommitted. Candidate questions include synchronous client write waiting, idle heartbeat/progress reporting, replica disk durability, ACK/freshness interaction with HA, allocator behavior, IO-thread ownership, Cluster bus depth, scripting or storage tiering.

Each future version must choose one explicit systems question based on evidence instead of bundling a feature list.
