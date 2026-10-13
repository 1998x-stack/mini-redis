# Architecture — v0.5

## 1. Current stack

```text
redis-cli / client
      ↓
RESP2 + reactor/legacy transport
      ↓
command.Registry
      ↓
store.Store
      ↓
versionedmemory.Memory
      ├── keyspace Dict[string → Object]
      └── expires  Dict[string → deadline]
                    │
                    ▼
            Object{Type, Encoding, payload}
                    │
                    ▼
              logical state
                    │
             SnapshotEntry[]
               ┌────┴────┐
               ▼         ▼
              RDB       AOF rewrite
```

v0.5 adds persistence below logical state while keeping protocol, command semantics and physical collection encodings independent.

## 2. Why a new serialization boundary?

Directly serializing the v0.4 payload would make disk compatibility depend on implementation accidents:

```text
bad persistence boundary:
quickNode pointers
Dict bucket count
rehashIndex
SkipList random levels/spans
```

Instead `SnapshotEntry` contains only stable meaning:

```text
Key
Type
logical values
absolute expiry deadline
```

Therefore:

```text
process A: List encoded as quicklist
              ↓ snapshot
       logical [a,b,c,...]
              ↓ restore
process B: current policy chooses any valid List encoding
```

Logical equality, not pointer/layout equality, is the persistence invariant.

## 3. Snapshot extraction and restoration

`Memory.Snapshot()` holds the Memory lock while establishing a point-in-time state view. It first removes already-expired entries, then walks sorted keys and exports logical values.

`RestoreSnapshot()` constructs fresh top-level Dicts and rebuilds each value through normal object constructors/mutators. Expired absolute deadlines are skipped rather than restored.

This design intentionally makes restoration an encoding-policy decision rather than a disk-format decision.

## 4. RDB-like file format

The project-specific v1 format is:

```text
0                                         N
+------+---------+-------------+-----------+------+
| MRDB | version | payload len | payload   | CRC  |
+------+---------+-------------+-----------+------+
 4 B      u16        u64         bytes      u32
```

The payload is canonical logical data. Keys, Hash fields, Set members and ZSet entries are placed in deterministic order. ZSet scores are wire strings parsed back to `float64`; this preserves finite values and infinities without relying on JSON's unsupported infinite numeric literals.

CRC32 is educational corruption detection, not authentication.

## 5. Atomic persistence publication

The final path must never intentionally expose an in-progress file:

```text
OLD VALID TARGET
      │
      │         create .target.tmp-XYZ
      │                   ↓
      │                  write
      │                   ↓
      │                fsync(file)
      │                   ↓
      └────────────── atomic rename
                          ↓
                     fsync(directory)
                          ↓
                    NEW VALID TARGET
```

Failure before rename means readers still see the old target. The temp file is best-effort cleaned up. The directory `fsync` exists because persisting file contents and persisting the renamed directory entry are separate durability concerns.

This models the Unix/Linux durability path used by this educational implementation.

## 6. Why AOF rewrite is necessary

AOF stores mutation history:

```text
SET k 1
SET k 2
SET k 3
DEL other
SET k 4
```

Current logical state may require only:

```text
SET k 4
```

`persistence.Commands()` converts `SnapshotEntry[]` into a reconstruction stream using existing commands. Absolute expiry uses `PEXPIREAT`, never a relative duration.

The rewrite therefore compacts **history → sufficient state reconstruction**.

## 7. Rewrite concurrency boundary

A naive implementation has a lost-write window:

```text
snapshot S0
          ← mutation happens here and is not in S0
start buffering
```

v0.5 closes that gap with `Server.persistenceMu`:

```text
lock persistenceMu
  Snapshot S0
  AOF.BeginRewrite()      # buffering becomes active
unlock persistenceMu
```

Every normal command executes its Store mutation and AOF append while holding the same server lock. Therefore no command can cross the snapshot/buffer boundary invisibly.

After buffering begins, command `C` is represented twice temporarily:

```text
C → old/live AOF      # preserves current durability path
C → rewrite buffer    # preserves replacement correctness
```

The compact base is written to a temp file without holding the AOF mutex. Final commit briefly locks the AOF:

```text
append rewriteBuffer to temp
fsync(temp)
rename(temp, AOF)
swap live file descriptor
fsync(directory)
```

After that, new appends go only to the replacement AOF.

## 8. Crash windows

### Before temp creation / during base generation

Old AOF/RDB remains authoritative.

### Temp partially written

Old target remains untouched; temp is not published.

### Temp complete but before rename

Old target remains untouched. Tests inject failure exactly here.

### After rename

The replacement is the visible file. The code then synchronizes the parent directory to make the renamed entry durable under the target filesystem model.

The implementation does not claim to simulate every storage-device failure mode; it teaches the file-publication boundary explicitly.

## 9. Startup precedence

Startup avoids ambiguous merging:

```text
appendonly=true
      ↓
replay AOF only
(RDB ignored)

appendonly=false
      ↓
RDB exists? ── no ─► empty DB
      │ yes
      ▼
validate magic/version/length/checksum
      ↓
restore logical snapshot
```

A complete invalid AOF command fails replay. A truncated final AOF frame is ignored as the explicit crash-tail policy inherited from v0.1. Corrupt/incompatible RDB does not silently turn into an empty database.

## 10. TTL correctness

TTL crosses persistence only as an absolute Unix-millisecond deadline:

```text
now=100s
EXPIRE key 60
absolute deadline=160s
       ↓ persistence / downtime
restart at 140s
       ↓
remaining ≈20s, NOT 60s
```

If restart happens after 160s, restoration/replay removes or skips the expired state. Downtime cannot extend key lifetime.

## 11. RDB and AOF roles

```text
RDB
  state snapshot
  compact restart input
  periodic work concentration

AOF
  mutation history
  append-oriented durability path
  grows with redundant history

AOF rewrite
  converts history back into compact state reconstruction
```

v0.5 deliberately keeps both visible because the learning goal is their trade-off, not hiding them behind one persistence abstraction.

## 12. Testing and demo

```bash
make persistence-demo
```

The demo shows:

- logical state saved and restored through RDB;
- redundant AOF history rewritten to a compact base;
- a mutation after the snapshot boundary retained through the rewrite buffer;
- the temp → fsync → rename → directory-fsync publication model.

CI also runs race detection, both network-engine smoke tests, v0.3/v0.4 demos and persistence recovery tests.

## 13. Next architectural layer

v0.6 introduces memory pressure:

```text
keyspace + expiry
      ↓
maxmemory accounting
      ↓
policy chooses victim
```

Expiration remains time validity; eviction will be memory-pressure policy. The two must stay separate even when both delete keys.
