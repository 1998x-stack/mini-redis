package versionedmemory

import (
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/dict"
	"github.com/1998x-stack/mini-redis/internal/store"
)

// Memory is the v0.3 in-memory engine.
//
// data and expires remain separate dictionaries, preserving Redis' core idea
// that only keys with TTLs pay expiration-metadata overhead. Unlike v0.1/v0.2,
// the dictionaries are explicit educational hash tables with incremental
// rehash rather than Go maps.
type Memory struct {
	mu      sync.RWMutex
	data    *dict.Dict[store.Object]
	expires *dict.Dict[time.Time]
	clock   store.Clock
}

// DictStats is an inspectable snapshot for tests/teaching. It is intentionally
// not part of the command-facing store.Store contract.
type DictStats struct {
	Data    dict.Stats
	Expires dict.Stats
}

func New(clock store.Clock) *Memory {
	if clock == nil {
		clock = store.RealClock{}
	}
	return &Memory{
		data:    dict.New[store.Object](dict.FNV1a),
		expires: dict.New[time.Time](dict.FNV1a),
		clock:   clock,
	}
}

func (m *Memory) expiredLocked(key string, now time.Time) bool {
	deadline, ok := m.expires.Get(key)
	return ok && !deadline.After(now)
}

func (m *Memory) deleteIfExpiredLocked(key string, now time.Time) bool {
	if !m.expiredLocked(key, now) {
		return false
	}
	m.data.Delete(key)
	m.expires.Delete(key)
	return true
}

func (m *Memory) Get(key string) (store.Object, bool) {
	// A read may lazily delete an expired key and dictionary lookups may advance
	// incremental rehash, so v0.3 intentionally takes the write lock here.
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deleteIfExpiredLocked(key, m.clock.Now()) {
		return store.Object{}, false
	}
	return m.data.Get(key)
}

func (m *Memory) Set(key string, value store.Object, ttl *time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data.Set(key, value)
	if ttl == nil {
		m.expires.Delete(key)
		return
	}
	m.expires.Set(key, m.clock.Now().Add(*ttl))
}

func (m *Memory) Delete(keys ...string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	deleted := 0
	for _, key := range keys {
		m.deleteIfExpiredLocked(key, now)
		if !m.data.Delete(key) {
			continue
		}
		m.expires.Delete(key)
		deleted++
	}
	return deleted
}

func (m *Memory) Exists(keys ...string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	count := 0
	for _, key := range keys {
		if m.deleteIfExpiredLocked(key, now) {
			continue
		}
		if _, ok := m.data.Get(key); ok {
			count++
		}
	}
	return count
}

func (m *Memory) IncrBy(key string, delta int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	m.deleteIfExpiredLocked(key, now)

	current := int64(0)
	if obj, ok := m.data.Get(key); ok {
		if obj.Type != store.ObjectString {
			return 0, store.ErrNotInteger
		}
		if n, encoded := obj.Int64(); encoded {
			current = n
		} else {
			text, ok := obj.StringValue()
			if !ok {
				return 0, store.ErrNotInteger
			}
			n, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return 0, store.ErrNotInteger
			}
			current = n
		}
	}

	// Overflow-safe addition, equivalent to rejecting values outside int64.
	if (delta > 0 && current > math.MaxInt64-delta) ||
		(delta < 0 && current < math.MinInt64-delta) {
		return 0, store.ErrNotInteger
	}

	next := current + delta
	m.data.Set(key, store.NewString(strconv.FormatInt(next, 10)))
	// Redis INCR preserves an existing TTL, so expires is intentionally untouched.
	return next, nil
}

func (m *Memory) Expire(key string, ttl time.Duration) bool {
	return m.ExpireAt(key, m.clock.Now().Add(ttl))
}

func (m *Memory) ExpireAt(key string, deadline time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	if m.deleteIfExpiredLocked(key, now) {
		return false
	}
	if _, ok := m.data.Get(key); !ok {
		return false
	}
	if !deadline.After(now) {
		m.data.Delete(key)
		m.expires.Delete(key)
		return true
	}
	m.expires.Set(key, deadline)
	return true
}

func (m *Memory) TTL(key string) store.TTLResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	if m.deleteIfExpiredLocked(key, now) {
		return store.TTLResult{}
	}
	if _, ok := m.data.Get(key); !ok {
		return store.TTLResult{}
	}
	deadline, ok := m.expires.Get(key)
	if !ok {
		return store.TTLResult{Exists: true}
	}
	remaining := deadline.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	return store.TTLResult{Exists: true, HasExpiry: true, Remaining: remaining}
}

func (m *Memory) Keys(pattern string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Iteration policy forbids mutation from a Dict.Range callback, so take a
	// small snapshot of key names first, then perform lazy expiration/matching.
	candidates := m.data.Keys(0)
	now := m.clock.Now()
	out := make([]string, 0, len(candidates))
	for _, key := range candidates {
		if m.deleteIfExpiredLocked(key, now) {
			continue
		}
		matched, err := filepath.Match(pattern, key)
		if err != nil {
			return nil, err
		}
		if matched {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *Memory) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = dict.New[store.Object](dict.FNV1a)
	m.expires = dict.New[time.Time](dict.FNV1a)
}

func (m *Memory) ExpireDue(limit int) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Snapshot at most limit expiry keys. Dictionary order is intentionally not
	// stable; active expiry only promises bounded sampling in this version.
	keys := m.expires.Keys(limit)
	now := m.clock.Now()
	removed := 0
	for _, key := range keys {
		if m.deleteIfExpiredLocked(key, now) {
			removed++
		}
	}
	return removed
}

func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	for _, key := range m.expires.Keys(0) {
		m.deleteIfExpiredLocked(key, now)
	}
	return m.data.Len()
}

// RehashStep lets the server timer advance dictionary work even when command
// traffic is low. The supplied budget is split between the keyspace and expiry
// dictionaries so one cannot permanently starve the other.
func (m *Memory) RehashStep(workUnits int) int {
	if workUnits <= 0 {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	dataBudget := (workUnits + 1) / 2
	expireBudget := workUnits / 2
	used := m.data.RehashStep(dataBudget)
	used += m.expires.RehashStep(expireBudget)
	return used
}

func (m *Memory) Stats() DictStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return DictStats{Data: m.data.Stats(), Expires: m.expires.Stats()}
}
