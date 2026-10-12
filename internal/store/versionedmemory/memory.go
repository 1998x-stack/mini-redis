package versionedmemory

import (
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/store"
)

// Memory is the v0.1 in-memory engine.
//
// data and expires intentionally use separate maps, matching Redis' core idea:
// only keys with TTLs pay expiration metadata overhead.
type Memory struct {
	mu      sync.RWMutex
	data    map[string]store.Object
	expires map[string]time.Time
	clock   store.Clock
}

func New(clock store.Clock) *Memory {
	if clock == nil {
		clock = store.RealClock{}
	}
	return &Memory{
		data:    make(map[string]store.Object),
		expires: make(map[string]time.Time),
		clock:   clock,
	}
}

func (m *Memory) expiredLocked(key string, now time.Time) bool {
	deadline, ok := m.expires[key]
	return ok && !deadline.After(now)
}

func (m *Memory) deleteIfExpiredLocked(key string, now time.Time) bool {
	if !m.expiredLocked(key, now) {
		return false
	}
	delete(m.data, key)
	delete(m.expires, key)
	return true
}

func (m *Memory) Get(key string) (store.Object, bool) {
	// We need a write lock because a read can lazily delete an expired key.
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deleteIfExpiredLocked(key, m.clock.Now()) {
		return store.Object{}, false
	}
	v, ok := m.data[key]
	return v, ok
}

func (m *Memory) Set(key string, value store.Object, ttl *time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	if ttl == nil {
		delete(m.expires, key)
		return
	}
	m.expires[key] = m.clock.Now().Add(*ttl)
}

func (m *Memory) Delete(keys ...string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	deleted := 0
	for _, key := range keys {
		m.deleteIfExpiredLocked(key, now)
		if _, ok := m.data[key]; !ok {
			continue
		}
		delete(m.data, key)
		delete(m.expires, key)
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
		m.deleteIfExpiredLocked(key, now)
		if _, ok := m.data[key]; ok {
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
	if obj, ok := m.data[key]; ok {
		if obj.Type != store.ObjectString {
			return 0, store.ErrNotInteger
		}
		n, err := strconv.ParseInt(obj.Str, 10, 64)
		if err != nil {
			return 0, store.ErrNotInteger
		}
		current = n
	}

	// Overflow-safe addition, equivalent to rejecting values outside int64.
	if (delta > 0 && current > math.MaxInt64-delta) ||
		(delta < 0 && current < math.MinInt64-delta) {
		return 0, store.ErrNotInteger
	}

	next := current + delta
	m.data[key] = store.NewString(strconv.FormatInt(next, 10))
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
	if _, ok := m.data[key]; !ok {
		return false
	}
	if !deadline.After(now) {
		delete(m.data, key)
		delete(m.expires, key)
		return true
	}
	m.expires[key] = deadline
	return true
}

func (m *Memory) TTL(key string) store.TTLResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	if m.deleteIfExpiredLocked(key, now) {
		return store.TTLResult{}
	}
	if _, ok := m.data[key]; !ok {
		return store.TTLResult{}
	}
	deadline, ok := m.expires[key]
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

	now := m.clock.Now()
	out := make([]string, 0, len(m.data))
	for key := range m.data {
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
	m.data = make(map[string]store.Object)
	m.expires = make(map[string]time.Time)
}

func (m *Memory) ExpireDue(limit int) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	removed := 0
	visited := 0
	for key := range m.expires {
		if limit > 0 && visited >= limit {
			break
		}
		visited++
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
	for key := range m.expires {
		m.deleteIfExpiredLocked(key, now)
	}
	return len(m.data)
}
