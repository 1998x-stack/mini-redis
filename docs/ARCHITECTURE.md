# Architecture — v0.2

## 1. Architectural goal

mini-redis is a teaching implementation. v0.2 keeps protocol, command, storage and persistence semantics stable while replacing the network/runtime boundary with a readiness-driven reactor.

```text
                             Linux
                              │
                              ▼
                       nonblocking sockets
                              │
                              ▼
                      +---------------+
                      | epoll Poller  |
                      +-------+-------+
                              │ readiness
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                 client A  client B  client C
                 in/out    in/out    in/out
                 buffers   buffers   buffers
                    └─────────┼─────────┘
                              ▼
                       +--------------+
                       | Event Loop   |
                       +------+-------+
                              │
                       parse / execute
                              │
            +-----------------+-----------------+
            ▼                                   ▼
     command.Registry                       RESP reply
            │                                   │
            ▼                                   ▼
       store.Store                       client output buf
        │       │                               │
        ▼       ▼                               ▼
      values   TTL                       writable readiness
            │
            ▼
            AOF
```

## 2. Poller vs event loop

These are intentionally separate responsibilities.

`internal/netpoll` answers only:

> Which file descriptors are ready for which kinds of I/O?

The server event loop answers:

> What should mini-redis do because those descriptors are ready?

That separation prevents epoll-specific details from leaking into RESP, command handlers or storage.

## 3. Why nonblocking I/O needs buffers

TCP is a byte stream. One `read(2)` does not equal one Redis command and one `write(2)` does not equal one complete Redis response.

Therefore every client owns:

```text
input buffer                         output buffer
────────────                         ─────────────
partial command bytes                encoded replies
multiple pipelined commands          bytes not yet accepted by kernel
```

### Read side

```text
EPOLLIN
  ↓
read until EAGAIN
  ↓
append bytes to client input
  ↓
parse complete commands repeatedly
  ↓
leave incomplete suffix buffered
```

### Write side

```text
reply encoded
  ↓
append to client output
  ↓
request EPOLLOUT
  ↓
write until EAGAIN or empty
  ↓
if empty: stop requesting EPOLLOUT
```

Always registering writable readiness would make epoll repeatedly wake for sockets that are normally writable, causing unnecessary busy work.

## 4. Incremental RESP parsing

The blocking v0.1 parser can wait for more bytes. A reactor cannot block the event-loop thread waiting for one client.

`resp.ParseCommand` therefore has a tri-state result:

```text
complete command  -> argv + consumed bytes
valid prefix      -> complete=false, consumed=0
malformed frame   -> protocol error
```

The implementation deliberately reuses the existing RESP Reader semantics rather than inventing a second protocol grammar.

## 5. Command execution ordering

Within the reactor, commands execute serially on the event-loop goroutine.

For one pipeline:

```text
SET n 1
INCR n
GET n
```

execution and response queueing are:

```text
SET  -> +OK
INCR -> :2
GET  -> $1 2
```

The output buffer preserves this order even if the kernel only accepts part of the bytes on the first write.

Across clients, readiness order is determined by epoll delivery; once one command begins executing it completes before the loop moves to another command.

## 6. Slow-client isolation and backpressure

A slow reader should not stall the event loop.

v0.2 never blocks waiting for that socket to become writable. Instead it retains the unsent suffix and continues serving other ready descriptors.

But a client that never drains replies can create unbounded server memory. v0.2 therefore applies a per-client output-buffer ceiling:

```text
queued bytes + next reply > limit
             ↓
         disconnect client
```

This is intentionally simpler than production Redis' differentiated output-buffer policies.

An input-buffer ceiling also prevents an indefinitely incomplete request from growing without bound.

## 7. Timer events

The reactor's `epoll_wait` timeout is chosen from the next server timer deadline.

Current timer work:

- active TTL expiration;
- optional idle-client sweep.

This teaches the relationship between file events and time events without requiring one timer per key or one goroutine per timer.

## 8. Wakeup and shutdown

A blocking `epoll_wait` must be interrupted when the parent context is cancelled.

v0.2 registers the read side of a nonblocking pipe with epoll. Cancellation writes one byte to the pipe:

```text
context cancelled
      ↓
wake pipe write
      ↓
epoll returns readable event
      ↓
event loop observes cancellation
      ↓
close clients + listener + poller
```

This is the only reactor-support goroutine; it does not process client commands.

## 9. Legacy engine as controlled baseline

The v0.1 mechanism remains available as `-network-engine=legacy`:

```text
legacy                              reactor
──────                              ───────
net.Listener                        raw nonblocking fd
Accept                              epoll readiness
blocking conn.Read                  read until EAGAIN
goroutine/client                    one event loop
immediate reply write               output queue + EPOLLOUT
expiration goroutine                event-loop timer
```

Keeping both behind one command/storage stack makes the networking architecture the independent variable in benchmarks.

## 10. Persistence boundary remains unchanged

AOF is intentionally not redesigned in v0.2.

Successful mutations are still appended before success is acknowledged. `appendfsync everysec` still uses its existing sync goroutine.

That means disk I/O may still delay the command loop. This is documented rather than hidden because v0.2 isolates **network scheduling**, not every possible blocking source in Redis.

## 11. Platform boundary

The reactor uses Linux epoll. `auto` chooses reactor on Linux and legacy elsewhere. The non-Linux binary still compiles, and explicit reactor selection reports that epoll is unavailable.

kqueue and IOCP are future optional portability work, not part of the v0.2 learning target.

## 12. Next architecture replacement

v0.3 leaves the reactor intact and replaces Go's hidden map implementation with an explicit educational dictionary capable of incremental rehash.
