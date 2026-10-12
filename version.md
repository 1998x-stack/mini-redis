# mini-redis v0.2 — Nonblocking I/O and Event Loop

## 1. Identity

- Integrated branch after validation: `main`
- Historical learning snapshot: `v0.2`
- Version label: `v0.2.0`
- Previous snapshot: `v0.1`
- Status: implemented and validated

## 2. Version theme

**Replace one blocking execution context per client with one readiness-driven reactor while preserving Redis command semantics.**

v0.1 answered: what happens from RESP bytes to state mutation and AOF?

v0.2 asks: how can one thread own many sockets without one blocked client stopping everybody else?

```text
                 nonblocking listen fd
                         │
                         ▼
                    epoll Poller
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
     client A         client B         client C
  input/output     input/output     input/output
     buffers          buffers          buffers
        └────────────────┼────────────────┘
                         ▼
                     Event Loop
          read → parse → execute → queue reply
                         → write when ready
                         │
                         ▼
               timer hook / expiration
```

## 3. What changed relative to v0.1, and why?

v0.1 used blocking sockets and one goroutine per client. v0.2 keeps RESP semantics, command registry, Store, TTL and AOF unchanged but replaces transport/runtime ownership with nonblocking fds, epoll readiness, per-client buffers and one reactor command loop.

This controlled replacement makes networking architecture the independent variable.

## 4. Mechanisms added

- `internal/netpoll` Poller abstraction;
- Linux epoll implementation;
- nonblocking listener and accepted clients;
- per-client input/output buffers;
- incremental `resp.ParseCommand`;
- pipelined command parsing;
- ordered response queueing;
- partial write handling;
- writable readiness only while output exists;
- configurable input/output buffer limits;
- event-loop active-expiration timer;
- idle-client timer cleanup;
- wake-pipe cancellation;
- explicit `legacy` reference engine.

Linux `auto` selects reactor; non-Linux `auto` selects legacy so the binary stays runnable while the reactor remains explicitly epoll-based.

## 5. Critical invariants proved by tests

- every truncated prefix of a valid command remains incomplete and consumes zero logical bytes;
- one partial client frame does not block another client's `PING`;
- pipeline replies preserve command order;
- a client exceeding the output buffer limit is disconnected without harming another client;
- malformed protocol produces an error then closes that connection;
- epoll add/readiness/delete lifecycle works;
- legacy transport still produces the same command-level behavior.

## 6. Atomicity/concurrency model

Reactor command handlers execute serially on the event-loop goroutine. The v0.1 mutex-protected Store remains intentionally unchanged so this version isolates transport scheduling.

AOF `everysec` still has its fsync goroutine and AOF append remains synchronous before success acknowledgement. v0.2 therefore does not claim every process activity occurs on one OS thread.

## 7. Important source files

```text
internal/netpoll/poller.go
internal/netpoll/poller_linux.go
        ↓
internal/resp/incremental.go
        ↓
internal/server/reactor_linux.go
        ↓
internal/server/server.go
        ↓
internal/server/legacy.go
        ↓
internal/config/config.go
```

## 8. Validation

Validated with:

```bash
gofmt -d .
go vet ./...
go test ./...
go test -race ./...
go build ./cmd/mini-redis-server
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

Additional checks covered real TCP pipelining, AOF restart/replay through the reactor, and Darwin/Windows cross-compilation.

## 9. Benchmark path

`make benchmark` compares legacy vs reactor from the same build for idle-connection RSS, pipelined PING throughput and slow-client isolation. Results are environment dependent and are not universal Redis performance claims.

## 10. Non-goals

- no I/O thread pool;
- no io_uring/TLS;
- no kqueue/IOCP reactor;
- no custom dictionary or incremental rehash;
- no List/Hash/Set/ZSet;
- no RDB/AOF rewrite;
- no eviction;
- no transactions/PubSub;
- no replication/Sentinel/Cluster.

## 11. Next version

v0.3 replaces hidden Go-map behavior with a Redis-like dictionary:

```text
Go map
   ↓
Dict: ht[0] + ht[1] + rehash index
   ↓
bounded incremental bucket migration
   ↓
object type / physical encoding metadata
```

Primary question: how can a hash table grow while the event loop keeps serving commands without one large stop-the-world resize?
