// Package memorypolicy layers runtime memory-pressure policy above the stable
// Store contract. The wrapped Store remains responsible for values, TTL and
// dictionary invariants; this package owns approximate accounting, access heat
// and eviction. None of this runtime policy state is serialized by Snapshot().
package memorypolicy

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/store"
)

const policyMetadataBytes = int64(24)

type accessMeta struct {
	lastAccessUnixNano int64
	frequency          uint8
	lfuLastDecayNano   int64
}

// Manager implements store.Store by delegation and adds the optional v0.6
// MemoryController/Snapshotter/RehashStepper capabilities.
type Manager struct {
	base        store.Store
	snapshotter store.Snapshotter

	mu       sync.Mutex
	cfg      store.MemoryConfig
	meta     map[string]accessMeta
	rng      *rand.Rand
	evicted  uint64
	expired  uint64
	examined uint64
}

func New(base store.Store, cfg store.MemoryConfig) (*Manager, error) {
	if base == nil {
		return nil, errors.New("base store is required")
	}
	snapshotter, ok := base.(store.Snapshotter)
	if !ok {
		return nil, errors.New("memory policy requires store.Snapshotter")
	}
	m := &Manager{base: base, snapshotter: snapshotter, meta: make(map[string]accessMeta)}
	if err := m.ConfigureMemory(cfg); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) ConfigureMemory(cfg store.MemoryConfig) error {
	if cfg.Clock == nil {
		cfg.Clock = store.RealClock{}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg
	m.rng = rand.New(rand.NewSource(cfg.RandomSeed))
	m.meta = make(map[string]accessMeta)
	m.evicted = 0
	m.expired = 0
	m.examined = 0
	return nil
}

func (m *Manager) MemoryPolicyConfig() store.MemoryConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg
}

func (m *Manager) trackingEnabledLocked() bool {
	return m.cfg.MaxBytes > 0 && (m.cfg.Policy == store.PolicyAllKeysLRU || m.cfg.Policy == store.PolicyAllKeysLFU)
}

func decayLFU(meta accessMeta, now time.Time, decay time.Duration) accessMeta {
	if meta.lfuLastDecayNano == 0 {
		meta.lfuLastDecayNano = now.UnixNano()
		return meta
	}
	last := time.Unix(0, meta.lfuLastDecayNano)
	if !now.After(last) {
		return meta
	}
	periods := int(now.Sub(last) / decay)
	if periods <= 0 {
		return meta
	}
	if periods >= int(meta.frequency) {
		meta.frequency = 0
	} else {
		meta.frequency -= uint8(periods)
	}
	meta.lfuLastDecayNano = last.Add(time.Duration(periods) * decay).UnixNano()
	return meta
}

func (m *Manager) touchKnown(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.trackingEnabledLocked() {
		return
	}
	now := m.cfg.Clock.Now()
	meta := m.meta[key]
	meta.lastAccessUnixNano = now.UnixNano()
	if m.cfg.Policy == store.PolicyAllKeysLFU {
		meta = decayLFU(meta, now, m.cfg.LFUDecay)
		// Redis uses a probabilistic logarithmic counter. This educational
		// equivalent keeps the same idea: hotter counters become harder to
		// increment, so frequency grows sublinearly rather than per access.
		denominator := int(meta.frequency)*10 + 1
		if meta.frequency < 255 && m.rng.Intn(denominator) == 0 {
			meta.frequency++
		}
	}
	m.meta[key] = meta
}

func (m *Manager) forget(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		delete(m.meta, key)
	}
}

func (m *Manager) touchIfExists(key string) {
	if _, ok := m.base.Get(key); !ok {
		m.forget(key)
		return
	}
	m.touchKnown(key)
}

func (m *Manager) pruneMetadata(entries []store.SnapshotEntry) {
	live := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		live[entry.Key] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.meta {
		if _, ok := live[key]; !ok {
			delete(m.meta, key)
		}
	}
}

func (m *Manager) usageForEntries(entries []store.SnapshotEntry) int64 {
	var total int64
	live := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		total += store.ApproxSnapshotEntryBytes(entry)
		live[entry.Key] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.meta {
		if _, ok := live[key]; ok {
			total += policyMetadataBytes
		}
	}
	return total
}

func (m *Manager) MemoryStats() store.MemoryStats {
	entries, err := m.snapshotter.Snapshot()
	if err != nil {
		return store.MemoryStats{UsedBytes: -1}
	}
	m.pruneMetadata(entries)
	used := m.usageForEntries(entries)
	m.mu.Lock()
	defer m.mu.Unlock()
	return store.MemoryStats{
		UsedBytes:          used,
		MaxBytes:           m.cfg.MaxBytes,
		Policy:             m.cfg.Policy,
		Samples:            m.cfg.Samples,
		TrackedAccessKeys:  len(m.meta),
		EvictedKeys:        m.evicted,
		ExpiredForPressure: m.expired,
		CandidatesExamined: m.examined,
	}
}

func (m *Manager) KeyMemoryUsage(key string) (int64, bool) {
	entries, err := m.snapshotter.Snapshot()
	if err != nil {
		return 0, false
	}
	for _, entry := range entries {
		if entry.Key != key {
			continue
		}
		n := store.ApproxSnapshotEntryBytes(entry)
		m.mu.Lock()
		_, tracked := m.meta[key]
		m.mu.Unlock()
		if tracked {
			n += policyMetadataBytes
		}
		return n, true
	}
	m.forget(key)
	return 0, false
}

func sampleIndices(rng *rand.Rand, n, k int) []int {
	if k > n {
		k = n
	}
	if k <= 0 {
		return nil
	}
	// Floyd's algorithm samples k unique indexes with O(k) random draws and
	// without building a globally ordered candidate structure.
	selected := make(map[int]struct{}, k)
	for j := n - k; j < n; j++ {
		t := rng.Intn(j + 1)
		if _, exists := selected[t]; exists {
			selected[j] = struct{}{}
		} else {
			selected[t] = struct{}{}
		}
	}
	out := make([]int, 0, k)
	for idx := range selected {
		out = append(out, idx)
	}
	return out
}

func (m *Manager) chooseVictim(entries []store.SnapshotEntry) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(entries) == 0 {
		return -1, 0, errors.New("no eviction candidates")
	}

	if m.cfg.Policy == store.PolicyAllKeysRandom {
		idx := m.rng.Intn(len(entries))
		m.examined++
		return idx, 1, nil
	}

	indexes := sampleIndices(m.rng, len(entries), m.cfg.Samples)
	if len(indexes) == 0 {
		return -1, 0, errors.New("no eviction samples")
	}
	now := m.cfg.Clock.Now()
	best := indexes[0]
	bestMeta := m.meta[entries[best].Key]
	if m.cfg.Policy == store.PolicyAllKeysLFU {
		bestMeta = decayLFU(bestMeta, now, m.cfg.LFUDecay)
		m.meta[entries[best].Key] = bestMeta
	}

	for _, idx := range indexes[1:] {
		candidateMeta := m.meta[entries[idx].Key]
		if m.cfg.Policy == store.PolicyAllKeysLFU {
			candidateMeta = decayLFU(candidateMeta, now, m.cfg.LFUDecay)
			m.meta[entries[idx].Key] = candidateMeta
		}
		better := false
		switch m.cfg.Policy {
		case store.PolicyAllKeysLRU:
			better = candidateMeta.lastAccessUnixNano < bestMeta.lastAccessUnixNano
		case store.PolicyAllKeysLFU:
			better = candidateMeta.frequency < bestMeta.frequency ||
				(candidateMeta.frequency == bestMeta.frequency && candidateMeta.lastAccessUnixNano < bestMeta.lastAccessUnixNano)
		default:
			return -1, 0, fmt.Errorf("unsupported eviction policy %q", m.cfg.Policy)
		}
		if !better && candidateMeta.lastAccessUnixNano == bestMeta.lastAccessUnixNano && entries[idx].Key < entries[best].Key {
			better = true
		}
		if better {
			best = idx
			bestMeta = candidateMeta
		}
	}
	m.examined += uint64(len(indexes))
	return best, len(indexes), nil
}

func (m *Manager) EnforceMaxMemory() (store.PressureResult, error) {
	cfg := m.MemoryPolicyConfig()
	result := store.PressureResult{}
	if cfg.MaxBytes <= 0 {
		stats := m.MemoryStats()
		result.UsedBytes = stats.UsedBytes
		return result, nil
	}

	// First remove invalid-by-time data. These removals are expiration, not
	// eviction, and therefore are counted separately.
	result.ExpiredKeys = m.base.ExpireDue(0)
	if result.ExpiredKeys > 0 {
		m.mu.Lock()
		m.expired += uint64(result.ExpiredKeys)
		m.mu.Unlock()
	}

	entries, err := m.snapshotter.Snapshot()
	if err != nil {
		return result, err
	}
	m.pruneMetadata(entries)
	used := m.usageForEntries(entries)
	result.UsedBytes = used
	if used <= cfg.MaxBytes {
		return result, nil
	}
	if cfg.Policy == store.PolicyNoEviction {
		return result, store.ErrOOM
	}

	for used > cfg.MaxBytes && len(entries) > 0 {
		idx, examined, err := m.chooseVictim(entries)
		if err != nil {
			return result, err
		}
		result.CandidatesExamined += examined
		victim := entries[idx]
		freed := store.ApproxSnapshotEntryBytes(victim)
		m.mu.Lock()
		if _, tracked := m.meta[victim.Key]; tracked {
			freed += policyMetadataBytes
		}
		m.mu.Unlock()

		if m.base.Delete(victim.Key) == 1 {
			result.EvictedKeys = append(result.EvictedKeys, victim.Key)
			result.FreedBytes += freed
			used -= freed
			if used < 0 {
				used = 0
			}
			m.mu.Lock()
			delete(m.meta, victim.Key)
			m.evicted++
			m.mu.Unlock()
		}
		entries = append(entries[:idx], entries[idx+1:]...)
	}
	result.UsedBytes = used
	if used > cfg.MaxBytes {
		return result, store.ErrOOM
	}
	return result, nil
}

// ---- Store delegation + access metadata ----

func (m *Manager) Get(key string) (store.Object, bool) {
	obj, ok := m.base.Get(key)
	if ok {
		m.touchKnown(key)
	} else {
		m.forget(key)
	}
	return obj, ok
}

func (m *Manager) Set(key string, value store.Object, ttl *time.Duration) {
	m.base.Set(key, value, ttl)
	m.touchKnown(key)
}

func (m *Manager) Delete(keys ...string) int {
	n := m.base.Delete(keys...)
	m.forget(keys...)
	return n
}

func (m *Manager) Exists(keys ...string) int {
	n := m.base.Exists(keys...)
	for _, key := range keys {
		m.touchIfExists(key)
	}
	return n
}

func (m *Manager) IncrBy(key string, delta int64) (int64, error) {
	value, err := m.base.IncrBy(key, delta)
	if err == nil {
		m.touchKnown(key)
	}
	return value, err
}

func (m *Manager) Expire(key string, ttl time.Duration) bool {
	ok := m.base.Expire(key, ttl)
	if ok {
		m.touchIfExists(key)
	}
	return ok
}

func (m *Manager) ExpireAt(key string, deadline time.Time) bool {
	ok := m.base.ExpireAt(key, deadline)
	if ok {
		m.touchIfExists(key)
	}
	return ok
}

func (m *Manager) TTL(key string) store.TTLResult {
	result := m.base.TTL(key)
	if result.Exists {
		m.touchKnown(key)
	} else {
		m.forget(key)
	}
	return result
}

func (m *Manager) Keys(pattern string) ([]string, error) { return m.base.Keys(pattern) }

func (m *Manager) Flush() {
	m.base.Flush()
	m.mu.Lock()
	m.meta = make(map[string]accessMeta)
	m.mu.Unlock()
}

func (m *Manager) ExpireDue(limit int) int {
	n := m.base.ExpireDue(limit)
	if n > 0 {
		entries, err := m.snapshotter.Snapshot()
		if err == nil {
			m.pruneMetadata(entries)
		}
	}
	return n
}

func (m *Manager) Len() int { return m.base.Len() }

func (m *Manager) ListPush(key string, left bool, values []string) (int, error) {
	n, err := m.base.ListPush(key, left, values)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) ListPop(key string, left bool) (string, bool, error) {
	value, ok, err := m.base.ListPop(key, left)
	if err == nil {
		m.touchIfExists(key)
	}
	return value, ok, err
}

func (m *Manager) ListLen(key string) (int, error) {
	n, err := m.base.ListLen(key)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) ListRange(key string, start, stop int) ([]string, error) {
	values, err := m.base.ListRange(key, start, stop)
	if err == nil {
		m.touchIfExists(key)
	}
	return values, err
}

func (m *Manager) HashSet(key string, pairs []store.FieldValue) (int, error) {
	n, err := m.base.HashSet(key, pairs)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) HashGet(key, field string) (string, bool, error) {
	value, ok, err := m.base.HashGet(key, field)
	if err == nil {
		m.touchIfExists(key)
	}
	return value, ok, err
}

func (m *Manager) HashDelete(key string, fields ...string) (int, error) {
	n, err := m.base.HashDelete(key, fields...)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) HashLen(key string) (int, error) {
	n, err := m.base.HashLen(key)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) HashGetAll(key string) ([]store.FieldValue, error) {
	values, err := m.base.HashGetAll(key)
	if err == nil {
		m.touchIfExists(key)
	}
	return values, err
}

func (m *Manager) SetAdd(key string, members ...string) (int, error) {
	n, err := m.base.SetAdd(key, members...)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) SetRemove(key string, members ...string) (int, error) {
	n, err := m.base.SetRemove(key, members...)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) SetIsMember(key, member string) (bool, error) {
	ok, err := m.base.SetIsMember(key, member)
	if err == nil {
		m.touchIfExists(key)
	}
	return ok, err
}

func (m *Manager) SetMembers(key string) ([]string, error) {
	values, err := m.base.SetMembers(key)
	if err == nil {
		m.touchIfExists(key)
	}
	return values, err
}

func (m *Manager) SetCard(key string) (int, error) {
	n, err := m.base.SetCard(key)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) ZAdd(key string, pairs []store.ScoredMember) (int, error) {
	n, err := m.base.ZAdd(key, pairs)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) ZScore(key, member string) (float64, bool, error) {
	score, ok, err := m.base.ZScore(key, member)
	if err == nil {
		m.touchIfExists(key)
	}
	return score, ok, err
}

func (m *Manager) ZRank(key, member string) (int, bool, error) {
	rank, ok, err := m.base.ZRank(key, member)
	if err == nil {
		m.touchIfExists(key)
	}
	return rank, ok, err
}

func (m *Manager) ZRange(key string, start, stop int) ([]store.ScoredMember, error) {
	values, err := m.base.ZRange(key, start, stop)
	if err == nil {
		m.touchIfExists(key)
	}
	return values, err
}

func (m *Manager) ZRemove(key string, members ...string) (int, error) {
	n, err := m.base.ZRemove(key, members...)
	if err == nil {
		m.touchIfExists(key)
	}
	return n, err
}

func (m *Manager) Snapshot() ([]store.SnapshotEntry, error) {
	return m.snapshotter.Snapshot()
}

func (m *Manager) RestoreSnapshot(entries []store.SnapshotEntry) error {
	if err := m.snapshotter.RestoreSnapshot(entries); err != nil {
		return err
	}
	m.mu.Lock()
	m.meta = make(map[string]accessMeta)
	m.mu.Unlock()
	return nil
}

func (m *Manager) RehashStep(workUnits int) int {
	stepper, ok := m.base.(store.RehashStepper)
	if !ok {
		return 0
	}
	return stepper.RehashStep(workUnits)
}
