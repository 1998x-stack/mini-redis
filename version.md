# mini-redis v0.10 — Cluster / Hash Slots

## 1. Identity

- Active development branch: `v0.10`
- Previous snapshot: `v0.9`
- Target version label: `v0.10.0`
- Theme: 16384 hash slots, key routing, MOVED/ASK, explicit slot migration and bounded topology exchange

## 2. Primary question

**How can multiple writable primaries partition one logical keyspace so every key has one deterministic slot/owner, clients receive stable or temporary redirections, and a slot can migrate without hiding ownership state inside Store?**

## 3. Planned mechanism

```text
key
 ↓ CRC16 + hash-tag
slot ∈ [0,16383]
 ↓
slot table
 ├─ stable owner = local  → execute
 ├─ stable owner = remote → MOVED
 └─ migrating/importing
       ├─ source missing key → ASK target
       └─ target + ASKING    → execute once
```

Slot ownership and migration metadata are control/runtime state, not key/value payloads.

## 4. Required invariants

- every routed key maps to exactly one slot;
- hash tags `{...}` force related keys into the same slot when the tag is non-empty;
- multi-key commands spanning slots fail with CROSSSLOT before mutation;
- stable wrong-owner routing returns MOVED;
- ASK is temporary migration routing and never silently changes stable ownership;
- ASKING authorizes exactly one following request on an importing target;
- migration state is explicit and separate from Store;
- source deletes a transferred key only after target import succeeds;
- no production Redis Cluster consensus/anti-entropy claim.

## 5. Validation

The exact `v0.10` SHA must pass the historical baseline plus cluster-specific slot/routing/migration/demo tests, then `main` must fast-forward to the same SHA and pass CI again.

## 6. Next version

v0.11 will add RESP3 only after Cluster routing semantics are frozen.