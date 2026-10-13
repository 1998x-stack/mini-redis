# mini-redis

An architecture-driven Redis learning implementation in Go. Each version makes one hidden Redis mechanism explicit, small and testable.

## Current milestone: v0.11

```text
v0.1  request → command → state → TTL → AOF
  ↓
v0.2  epoll event loop
  ↓
v0.3  Redis-like Dict + incremental rehash
  ↓
v0.4  List / Hash / Set / ZSet + adaptive encodings
  ↓
v0.5  RDB-like snapshots + AOF rewrite
  ↓
v0.6  maxmemory + sampled LRU/LFU
  ↓
v0.7  WATCH + MULTI/EXEC + Pub/Sub
  ↓
v0.8  replication / PSYNC / backlog
  ↓
v0.9  Sentinel-inspired failover
  ↓
v0.10 Cluster / 16384 slots / MOVED / ASK
  ↓
v0.11 HELLO 2/3 + richer RESP3 values + Push  ← current
  ↓
v0.12 observability / performance
```

## v0.11 mental model

```text
TCP connection
      ↓
starts RESP2
      ↓
HELLO 3
      ↓
connection-local protocol = RESP3
      ↓
existing command / Store / Cluster semantics
      ↓
logical reply
      ↓
RESP3 representation

HELLO 2
      ↓
back to RESP2 on that connection only
```

The central lesson is:

> **RESP3 changes representation, not database semantics.**

Protocol mode belongs to one client connection. It is never written into Store, AOF, SnapshotEntry, replication state, Sentinel state or Cluster topology.

## HELLO negotiation

Every connection starts in RESP2 mode.

```text
HELLO 3
```

switches that connection to RESP3 and returns server metadata as a Map.

```text
HELLO 2
```

switches it back to RESP2. The same logical metadata is downgraded to a RESP2 flat array.

The server intentionally keeps command requests as arrays of blob strings, so the request/command path does not fork by protocol version.

## Rich RESP3 views

Selected existing commands expose better semantic types after `HELLO 3`:

| Command | RESP2 | RESP3 |
| --- | --- | --- |
| `HGETALL` | Array | Map `%` |
| `SMEMBERS` | Array | Set `~` |
| `SISMEMBER` | Integer 0/1 | Boolean `#` |
| `ZSCORE` | bulk text | Double `,` |
| missing value | null bulk/array | Null `_` |

The command handlers remain shared. A protocol-view adapter runs after command execution and, on the Cluster endpoint, after slot authorization.

## RESP3 value model

The protocol package now models and encodes:

```text
Simple String      +
Error              -
Integer            :
Blob String         $
Array              *
Null               _
Boolean            #
Double             ,
Big Number         (
Verbatim String    =
Map                %
Set                ~
Push               >
```

RESP2 remains first-class. Rich logical values have explicit downgrade rules, for example Map→flat Array, Set→Array, Boolean→0/1 and Push→Array.

## Pub/Sub finally uses Push

RESP2 has no dedicated asynchronous frame type, so historical subscribed-mode restrictions remain for RESP2 connections.

RESP3 has Push:

```text
>3
$7
message
$4
news
$5
hello

```

That makes asynchronous messages distinguishable from ordinary command replies. A RESP3 subscriber can therefore continue issuing ordinary commands while Pub/Sub messages arrive as Push frames.

The Pub/Sub delivery mechanism itself is unchanged; only its wire representation improves.

## Fragmented nested parser

`resp.ParseValue` incrementally parses RESP2/RESP3 values including nested Map/Set/Push containers.

Its key contract is:

```text
incomplete valid frame → complete=false, consumed=0
complete frame          → exact Value + byte count
malformed frame         → protocol error
```

Tests split a nested Push/Map/Set payload at every byte boundary.

## Both network engines share protocol semantics

The legacy goroutine transport and Linux epoll reactor use the same session-level negotiation code.

```text
transport
  ↓
executeClientCommand
  ↓
logical resp.Value
  ↓
session.protocol
  ↓
EncodeValueProtocol
```

The reactor remains about readiness/buffers/backpressure; RESP3 does not leak into its scheduling model.

## RESP3 + Cluster

The explicit Cluster endpoint also accepts `HELLO 2/3`, but protocol negotiation cannot bypass routing:

```text
GET key
  ↓
slot authorization first
  ├─ MOVED / ASK / CROSSSLOT
  └─ execute
       ↓
    response adaptation
       ↓
    RESP2 / RESP3 bytes
```

A real wire test negotiates RESP3 on a wrong-owner node and still requires `MOVED`.

## Persistence and replication stay canonical

A RESP3 client mutation still produces the same canonical mutation commands used by previous versions:

```text
RESP3 client SET
       ↓
command semantics
       ↓
canonical mutation
   ├── AOF
   └── replication backlog
```

HELLO does not change AOF replay, replication offsets, backlog framing or full/partial synchronization.

## Supported command subset

```text
PING ECHO
SET GET DEL EXISTS
INCR INCRBY
EXPIRE PEXPIREAT TTL
KEYS DBSIZE FLUSHDB
OBJECT ENCODING

LPUSH RPUSH LPOP RPOP LLEN LRANGE
HSET HGET HDEL HLEN HGETALL
SADD SREM SISMEMBER SMEMBERS SCARD
ZADD ZSCORE ZRANK ZRANGE ZREM

WATCH UNWATCH MULTI EXEC DISCARD
SUBSCRIBE UNSUBSCRIBE PUBLISH
```

Connection-level teaching command:

```text
HELLO [2|3]
```

The v0.10 Cluster facade additionally exposes `ASKING` and `CLUSTER KEYSLOT` and intentionally rejects cluster MULTI/EXEC semantics.

## Learning demos

```bash
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make session-demo
make replication-demo
make sentinel-demo
make cluster-demo
make resp3-demo        # RESP2 → HELLO 3 → rich values/Push → HELLO 2
```

## Validation

```bash
gofmt -d .
go vet ./...
go test -race ./...
go build ./cmd/mini-redis-server
make rehash-demo
make encodings-demo
make persistence-demo
make eviction-demo
make session-demo
make replication-demo
make sentinel-demo
make cluster-demo
make resp3-demo
make smoke
NETWORK_ENGINE=legacy PORT=16380 make smoke
```

## Read v0.11 in this order

```text
internal/resp/protocol.go
  ↓ protocol modes
internal/resp/value.go
  ↓ logical richer value model
internal/resp/writer.go
  ↓ RESP2 downgrade / RESP3 native encoding
internal/resp/value_parser.go
  ↓ incremental nested parsing
internal/server/session.go
  ↓ HELLO + response views + Pub/Sub Push
internal/server/legacy.go
internal/server/reactor_linux.go
  ↓ shared transport integration
internal/server/cluster_wire.go
  ↓ Cluster composition
internal/resp/resp3_test.go
internal/server/protocol_test.go
internal/server/cluster_wire_test.go
examples/resp3/main.go
```

## Important limits

v0.11 is not a complete production RESP3 implementation. It intentionally omits streamed aggregates/strings, full Attribute/Blob Error semantics, client tracking pushes and the complete Redis HELLO AUTH/SETNAME option surface.

Compare historical branches `v0.1` through `v0.11` to see one dominant architectural change at a time.

## Repository contracts

- `Agents.md` — mandatory engineering/version contract.
- `Plan.md` — canonical dependency-ordered roadmap.
- `version.md` — current learning boundary and delta.
- `docs/ARCHITECTURE.md` — current implementation reasoning.
