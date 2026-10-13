# Architecture — v0.11

## 1. Current stack

```text
TCP connection
      ↓
command request parser
      ↓
clientSession
  ├── protocol = RESP2 by default
  ├── HELLO 2 / HELLO 3
  ├── MULTI / WATCH state
  └── Pub/Sub state
      ↓
cluster routing gate when applicable
  ├── CROSSSLOT
  ├── MOVED
  └── ASK / ASKING
      ↓
Server.persistenceMu
      ↓
command registry / Store
      ↓
logical reply (resp.Value)
      ↓
protocol view adapter
      ├── HGETALL → Map in RESP3
      ├── SMEMBERS → Set in RESP3
      ├── SISMEMBER → Boolean in RESP3
      └── ZSCORE → Double in RESP3
      ↓
protocol-aware encoder
      ├── RESP2 compatibility bytes
      └── RESP3 richer bytes
```

v0.11's central lesson is:

> **The database computes one logical result; the connection chooses how that result is represented on the wire.**

## 2. Why protocol mode belongs to the connection

Two clients can talk to the same server simultaneously:

```text
client A → RESP2
client B → HELLO 3 → RESP3
```

Neither should change the other's responses. Therefore protocol mode cannot be:

- a Server-global flag;
- Store metadata;
- SnapshotEntry data;
- AOF state;
- replication state.

It is stored in `clientSession`, protected by the session mutex.

## 3. HELLO is session control, not a Store command

HELLO is intercepted before the ordinary command registry because it changes connection interpretation rather than database state.

```text
new connection
  protocol = 2

HELLO 3
  protocol = 3
  reply = logical Map
  encoder = RESP3

HELLO 2
  protocol = 2
  same logical metadata
  encoder = RESP2 downgrade
```

HELLO is rejected inside MULTI so a queued database transaction cannot contain a mid-transaction wire-mode switch.

## 4. Requests stay canonical

The teaching server continues to accept command requests as arrays of blob strings.

That grammar already works for both RESP2 and RESP3 command requests and is also the canonical format used by AOF/replication command frames.

Keeping request parsing independent from response negotiation prevents protocol-version concerns from entering:

```text
command validation
Store
AOF
replication offset accounting
Cluster key routing
```

## 5. Rich logical values

`resp.Value` now models:

```text
Simple String
Error
Integer
Blob String
Array
Null
Boolean
Double
Big Number
Verbatim String
Map
Set
Push
```

This is deliberately a logical intermediate representation.

The same value can have two wire encodings. For example:

```text
Boolean(true)
   ├─ RESP2 → :1
   └─ RESP3 → #t
```

and:

```text
Map{k:v}
   ├─ RESP2 → *2 [k, v]
   └─ RESP3 → %1 {k => v}
```

## 6. Response adaptation sits above command semantics

The command implementation does not need a RESP2 branch and a RESP3 branch.

Example HGETALL:

```text
Store / command handler
      ↓
logical field/value array
      ↓
protocol adapter
  RESP2 → leave array
  RESP3 → Map
```

The current richer views are:

| Logical command result | RESP2 | RESP3 |
| --- | --- | --- |
| `HGETALL` pairs | Array | Map |
| `SMEMBERS` members | Array | Set |
| `SISMEMBER` 0/1 | Integer | Boolean |
| `ZSCORE` text score | Blob | Double |
| missing value | null bulk/array | Null |

The adapter does not inspect physical Store encodings.

## 7. RESP2 downgrade is a first-class path

RESP3 support is not allowed to break historical clients.

The protocol-aware writer maps richer logical values back to RESP2 forms:

```text
Map      → flat Array
Set      → Array
Push     → Array
Boolean  → Integer 0/1
Double   → Blob String
BigNumber→ Blob String
Null     → null bulk
Verbatim → normal blob payload
```

This lets internal code use richer semantic values without forcing every caller to understand RESP3.

## 8. Incremental nested value parser

`ParseValue` is separate from the command parser.

Its contract is:

```text
valid but incomplete prefix
  → complete=false
  → consumed=0

complete frame
  → logical Value
  → exact consumed byte count

provably malformed frame
  → protocol error
```

Nested Map/Set/Push parsing is recursion-bounded. Tests split a nested payload at every byte boundary.

This parser is useful for demos/tests and provides a future foundation for clients consuming Push frames.

## 9. Pub/Sub: representation finally matches asynchronous semantics

RESP2 represents Pub/Sub messages as ordinary arrays. A subscribed RESP2 connection therefore keeps the historical restricted command mode.

RESP3 has a dedicated Push marker:

```text
>3
$7
message
$4
news
$5
hello

```

So the receiver can distinguish:

```text
normal command reply
```

from:

```text
unsolicited async message
```

without relying on array contents.

The Pub/Sub hub is unchanged as a delivery mechanism. Only the frame representation changes.

## 10. Why RESP3 subscribers may issue ordinary commands

Once asynchronous frames are visibly Push values, normal replies are no longer ambiguous.

Therefore:

```text
RESP2 subscribed
  → historical SUBSCRIBE / UNSUBSCRIBE / PING restriction

RESP3 subscribed
  → ordinary commands allowed
  → Pub/Sub arrives as Push
```

This demonstrates one of RESP3's architectural benefits rather than treating new markers as cosmetic syntax.

## 11. Legacy and reactor transports share semantics

The goroutine-per-connection transport and epoll reactor do not contain separate HELLO state machines.

Both follow:

```text
parse command
   ↓
execute shared session path
   ↓
read session.protocol
   ↓
EncodeValueProtocol
```

The reactor still owns nonblocking buffers/backpressure; the legacy engine still owns blocking goroutine scheduling. Protocol negotiation is orthogonal to both.

## 12. Cluster composition

The explicit v0.10 Cluster endpoint now supports HELLO too, but the ordering is intentionally:

```text
ordinary command
   ↓
ClusterExecute
   ↓
slot authorization / redirect
   ↓
logical reply
   ↓
RESP3 adaptation + encoding
```

not:

```text
RESP3
   ↓
bypass Cluster routing
```

Wire tests prove a RESP3 client connected to the wrong owner still gets `MOVED`.

HELLO itself is connection control and returns `mode=cluster` on the Cluster endpoint.

## 13. Persistence and replication are protocol-independent

A client may write using a RESP3 connection, but successful mutation publication still produces the same canonical command sequence used before v0.11.

```text
RESP3 client
  SET k v
     ↓
command semantics
     ↓
canonical mutation
   ├── AOF RESP command frame
   └── replication RESP command frame
```

Thus changing HELLO mode cannot alter:

- AOF replay meaning;
- replication byte offsets;
- backlog frame boundaries;
- full/partial synchronization;
- SnapshotEntry representation.

## 14. Relationship to earlier topology layers

```text
v0.8 Replication
  answers: how does another node receive data?

v0.9 Sentinel
  answers: which replica should become primary?

v0.10 Cluster
  answers: which primary owns this key's slot?

v0.11 RESP3
  answers: how does this connection represent replies and async events?
```

RESP3 is above these mechanisms rather than replacing any of them.

## 15. Validation model

### Codec

- native RESP3 Map/Set/Boolean/Double/Null/Push encoding;
- explicit RESP2 downgrade;
- `inf`, `-inf`, `nan` doubles;
- nested fragmented parser boundaries.

### Network

On both legacy and reactor:

- HELLO 3 / HELLO 2;
- richer command replies;
- Pub/Sub Push;
- normal commands while subscribed in RESP3;
- unchanged RESP2 behavior.

### Cluster

- HELLO 3 reports cluster mode;
- wrong owner remains MOVED;
- rich views occur after successful slot authorization.

### Demo

```bash
make resp3-demo
```

performs real TCP negotiation and prints RESP2 versus RESP3 value categories.

## 16. Explicit limits

v0.11 does not implement every RESP3 feature. In particular it does not claim complete support for streamed aggregates/strings, Attributes, full Blob Error semantics, client tracking pushes, or the production Redis HELLO option surface.

Those omissions keep the learning boundary focused on negotiation, richer value representation and asynchronous Push.

## 17. Next architectural layer

v0.12 asks a different question:

> **Can we observe and measure the cost/behavior of all these explicit mechanisms without changing their semantics?**

The next layer is metrics, latency, lag, queueing, profiling and repeatable performance evidence.
