# mini-redis v0.11 — RESP3

## 1. Identity

- Historical learning branch: `v0.11`
- Previous snapshot: `v0.10`
- Version label: `v0.11.0`
- Theme: connection-local protocol negotiation, richer response types, RESP2 compatibility and asynchronous Push frames

## 2. Primary question

**How can one client connection negotiate a richer wire representation without changing command semantics, Store state, persistence, replication, Sentinel failover or Cluster routing?**

```text
TCP connection
      ↓
RESP command request
      ↓
clientSession.protocol = 2 by default
      ↕ HELLO 2 / HELLO 3
existing command / cluster semantics
      ↓
logical resp.Value
      ↓
protocol view adaptation
      ├─ RESP2 encoder
      └─ RESP3 encoder
```

The dominant v0.11 lesson is:

> **Protocol is representation state owned by a connection. It is not database state and it is not command semantics.**

## 3. Delta from v0.10

Added:

- `resp.Protocol2` / `resp.Protocol3` connection modes;
- `HELLO 2` and `HELLO 3` negotiation;
- connection-local protocol state inside `clientSession`;
- RESP3 Null, Boolean, Double, Big Number, Verbatim String, Map, Set and Push value types;
- protocol-aware encoder with explicit RESP2 downgrade rules;
- incremental nested RESP2/RESP3 value parser;
- semantic response adaptation for selected existing commands;
- RESP3 Pub/Sub Push frames;
- ordinary commands while subscribed in RESP3 mode;
- RESP3 negotiation on both legacy and epoll/reactor transports;
- RESP3 negotiation on the explicit Cluster endpoint without bypassing slot routing;
- `make resp3-demo`.

Unchanged:

- request command semantics and command registry;
- Store/Object/encoding implementation;
- AOF command frames;
- RDB logical snapshots;
- v0.8 replication mutation stream;
- v0.9 Sentinel topology control;
- v0.10 slot ownership, MOVED/ASK/CROSSSLOT and migration.

## 4. Negotiation model

Every new connection starts in RESP2 mode.

```text
connect
  ↓
protocol = RESP2
  ↓
HELLO 3
  ↓
protocol = RESP3
  ↓
HELLO 2
  ↓
protocol = RESP2
```

The protocol mode belongs to that connection only. Another client is unaffected.

`HELLO` returns logical server metadata including server name, mini-redis version, negotiated protocol, client ID, mode and replication role.

A standalone RESP3 connection receives the HELLO response as a RESP3 Map. Switching back with `HELLO 2` returns the same logical metadata through the RESP2 flat-array downgrade.

## 5. Request format stays stable

Command requests continue to use arrays of blob strings:

```text
*2\r\n
$5\r\nHELLO\r\n
$1\r\n3\r\n
```

That representation is valid for the teaching RESP2 and RESP3 paths. Keeping command requests stable is intentional:

- command parsing does not fork by protocol version;
- AOF can continue using canonical RESP command frames;
- replication can continue publishing canonical mutation command frames;
- protocol negotiation does not enter durability state.

## 6. Rich logical value model

v0.11 extends `resp.Value` with:

```text
Null             _
Boolean          #t / #f
Double           ,1.25
Big Number       (123...
Verbatim String  =<len>\r\ntxt:...
Map              %
Set              ~
Push             >
```

Existing Simple String, Error, Integer, Blob String and Array remain available.

The value model is protocol-neutral; the selected encoder chooses the wire representation.

## 7. Existing commands gain richer RESP3 views

Command execution still produces the same logical result. A small protocol-view adapter improves representation when RESP3 is active:

| Command | RESP2 view | RESP3 view |
| --- | --- | --- |
| `HGETALL` | flat Array | Map |
| `SMEMBERS` | Array | Set |
| `SISMEMBER` | Integer 0/1 | Boolean |
| `ZSCORE` | Blob String | Double |
| missing bulk/array values | `$-1` / `*-1` | Null `_` |

This adapter runs after command/Cluster authorization. It does not reach into Store internals.

## 8. RESP2 downgrade is explicit

Rich logical values can still be encoded for a RESP2 connection:

```text
Map      → flat Array [key, value, ...]
Set      → Array
Push     → Array
Boolean  → Integer 0/1
Double   → Blob String
BigNumber→ Blob String
Null     → null bulk string
Verbatim → ordinary bulk payload
```

Therefore existing RESP2 clients and historical tests keep their previous wire behavior.

## 9. Pub/Sub and Push

RESP2 Pub/Sub uses arrays for both acknowledgements/messages and ordinary replies. The teaching server therefore keeps the historical subscribed-mode command restriction for RESP2.

RESP3 has an explicit Push type:

```text
>3
$7 message
$4 news
$5 hello
```

So a RESP3 subscriber can distinguish asynchronous Pub/Sub frames from normal command replies. v0.11 therefore permits ordinary commands while a RESP3 client is subscribed.

Subscription acknowledgements and published messages use Push in RESP3 and downgrade to Array for RESP2.

## 10. Fragmented / nested parsing

`resp.ParseValue` parses one RESP2/RESP3 value without consuming an incomplete frame.

Tests split a nested Push → Map → Set payload at every byte boundary and require:

```text
partial prefix → complete=false, consumed=0
whole frame    → exact nested logical value
```

A bounded recursion depth prevents malformed inputs from creating unbounded parser recursion.

The normal server command path still uses the existing command parser; the general value parser exists for tests, demos and future asynchronous client tooling.

## 11. Network-engine boundary

Legacy and reactor transports do not implement independent RESP3 semantics.

Both transports:

1. parse the same command request;
2. call the same session/command path;
3. read the connection's current protocol mode;
4. encode the resulting logical `resp.Value` accordingly.

This keeps protocol semantics above transport scheduling.

## 12. Cluster boundary

`Server.ServeCluster` also supports HELLO negotiation, but routing remains earlier than representation:

```text
request
  ↓
HELLO? → protocol negotiation only
  ↓ otherwise
ClusterExecute
  ↓ CROSSSLOT / MOVED / ASK / execute
protocol-view adapter
  ↓
RESP2 or RESP3 encoder
```

A wire-level test proves that `HELLO 3` on a wrong-owner cluster connection still returns `MOVED`; RESP3 cannot bypass slot ownership.

## 13. Persistence and replication boundary

Client protocol mode is never serialized.

AOF and replication still use canonical command frames independent of whether the originating client used RESP2 or RESP3.

Therefore this remains true:

```text
RESP3 client SET
      ↓
normal command execution
      ↓
canonical mutation commands
      ├─ AOF
      └─ replication
```

No durability or replica state depends on HELLO negotiation.

## 14. Critical invariants

- every connection starts RESP2;
- HELLO changes only that connection's protocol mode;
- RESP2 behavior remains backward compatible;
- response adaptation happens after command/Cluster semantics;
- Cluster MOVED/ASK/CROSSSLOT cannot be bypassed by RESP3;
- AOF and replication remain independent of client protocol mode;
- protocol state is never stored in Store or SnapshotEntry;
- Push is asynchronous representation, not a second Pub/Sub delivery mechanism;
- both network engines share one negotiation/session implementation;
- fragmented value parsing never consumes an incomplete frame.

## 15. Tests that prove the mechanism

Codec tests prove:

- exact Map/Set/Boolean/Double/Null RESP3 encodings;
- deterministic RESP2 downgrade;
- special double values (`inf`, `-inf`, `nan`);
- fragmented nested Push/Map/Set parsing.

Network tests on legacy and reactor prove:

- default RESP2 behavior;
- `HELLO 3` Map response;
- HGETALL Map;
- SMEMBERS Set;
- SISMEMBER Boolean;
- ZSCORE Double;
- Pub/Sub Push;
- ordinary command while subscribed in RESP3;
- `HELLO 2` downgrade back to RESP2.

Cluster wire tests prove:

- HELLO 3 returns `mode=cluster`;
- wrong-owner routing still returns MOVED;
- rich response adaptation occurs only after slot authorization.

Everything runs under `go test -race ./...`.

## 16. Demo

```bash
make resp3-demo
```

runs a real TCP session and shows:

```text
RESP2 HGETALL       → Array
HELLO 3             → Map
RESP3 HGETALL       → Map
RESP3 SMEMBERS      → Set
RESP3 SISMEMBER     → Boolean
RESP3 ZSCORE        → Double
RESP3 SUBSCRIBE     → Push
async message       → Push
command while sub'd → Null
HELLO 2             → RESP2 Array downgrade
```

## 17. Explicit limitations

v0.11 does not claim:

- every RESP3 type or streamed aggregate/string form;
- complete production Redis HELLO options such as AUTH/SETNAME;
- RESP3 Attributes or complete Blob Error support;
- client tracking / invalidation pushes;
- production Redis protocol compatibility in every edge case;
- any change to Cluster consensus/fencing limitations from v0.10.

## 18. Read this version in order

```text
internal/resp/protocol.go
  ↓
internal/resp/value.go
  ↓
internal/resp/writer.go
  ↓
internal/resp/value_parser.go
  ↓
internal/server/session.go
  ↓
internal/server/legacy.go
internal/server/reactor_linux.go
  ↓
internal/server/cluster_wire.go
  ↓
internal/resp/resp3_test.go
internal/server/protocol_test.go
internal/server/cluster_wire_test.go
  ↓
examples/resp3/main.go
```

## 19. Next version

v0.12 moves to **Observability / Performance**.

The central question becomes:

> Once the mechanisms are explicit, how do we measure where time, memory, queueing, replication lag and topology operations are actually spent?
