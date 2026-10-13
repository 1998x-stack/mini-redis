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

// Memory is the in-memory engine. The top-level keyspace and expiry metadata
// use v0.3's incremental dictionaries; v0.4 objects may themselves own richer
// encoding-specific structures behind the stable store.Object boundary.
type Memory struct {
	mu      sync.RWMutex
	data    *dict.Dict[store.Object]
	expires *dict.Dict[time.Time]
	clock   store.Clock
}

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
	// A read may lazily delete expiry state and Dict.Get may advance rehash.
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
	m.deleteIfExpiredLocked(key, m.clock.Now())

	current := int64(0)
	if obj, ok := m.data.Get(key); ok {
		if obj.Type != store.ObjectString {
			return 0, store.ErrWrongType
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
	if (delta > 0 && current > math.MaxInt64-delta) ||
		(delta < 0 && current < math.MinInt64-delta) {
		return 0, store.ErrNotInteger
	}
	next := current + delta
	m.data.Set(key, store.NewString(strconv.FormatInt(next, 10)))
	// INCR preserves an existing TTL.
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

// objectLocked centralizes lazy expiration, missing-key creation and WRONGTYPE
// checks. Mutating collection methods keep the lock for the whole operation so
// command handlers never implement non-atomic read/modify/write sequences.
func (m *Memory) objectLocked(key string, typ store.ObjectType, create func() store.Object) (store.Object, bool, error) {
	m.deleteIfExpiredLocked(key, m.clock.Now())
	obj, ok := m.data.Get(key)
	if !ok {
		if create == nil {
			return store.Object{}, false, nil
		}
		obj = create()
		m.data.Set(key, obj)
		return obj, true, nil
	}
	if obj.Type != typ {
		return store.Object{}, false, store.ErrWrongType
	}
	return obj, true, nil
}

func (m *Memory) persistObjectLocked(key string, obj store.Object) {
	m.data.Set(key, obj)
}

func (m *Memory) deleteEmptyLocked(key string) {
	m.data.Delete(key)
	m.expires.Delete(key)
}

func (m *Memory) ListPush(key string, left bool, values []string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, _, err := m.objectLocked(key, store.ObjectList, store.NewList)
	if err != nil {
		return 0, err
	}
	n := obj.ListPush(left, values)
	m.persistObjectLocked(key, obj)
	return n, nil
}

func (m *Memory) ListPop(key string, left bool) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectList, nil)
	if err != nil || !ok {
		return "", false, err
	}
	value, found := obj.ListPop(left)
	if found {
		if obj.ListLen() == 0 {
			m.deleteEmptyLocked(key)
		} else {
			m.persistObjectLocked(key, obj)
		}
	}
	return value, found, nil
}

func (m *Memory) ListLen(key string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectList, nil)
	if err != nil || !ok {
		return 0, err
	}
	return obj.ListLen(), nil
}

func (m *Memory) ListRange(key string, start, stop int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectList, nil)
	if err != nil || !ok {
		return []string{}, err
	}
	return obj.ListRange(start, stop), nil
}

func (m *Memory) HashSet(key string, pairs []store.FieldValue) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, _, err := m.objectLocked(key, store.ObjectHash, store.NewHash)
	if err != nil {
		return 0, err
	}
	n := obj.HashSet(pairs)
	m.persistObjectLocked(key, obj)
	return n, nil
}

func (m *Memory) HashGet(key, field string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectHash, nil)
	if err != nil || !ok {
		return "", false, err
	}
	value, found := obj.HashGet(field)
	return value, found, nil
}

func (m *Memory) HashDelete(key string, fields ...string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectHash, nil)
	if err != nil || !ok {
		return 0, err
	}
	n := obj.HashDelete(fields)
	if obj.HashLen() == 0 {
		m.deleteEmptyLocked(key)
	} else {
		m.persistObjectLocked(key, obj)
	}
	return n, nil
}

func (m *Memory) HashLen(key string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectHash, nil)
	if err != nil || !ok {
		return 0, err
	}
	return obj.HashLen(), nil
}

func (m *Memory) HashGetAll(key string) ([]store.FieldValue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectHash, nil)
	if err != nil || !ok {
		return []store.FieldValue{}, err
	}
	return obj.HashGetAll(), nil
}

func (m *Memory) SetAdd(key string, members ...string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, _, err := m.objectLocked(key, store.ObjectSet, store.NewSet)
	if err != nil {
		return 0, err
	}
	n := obj.SetAdd(members)
	m.persistObjectLocked(key, obj)
	return n, nil
}

func (m *Memory) SetRemove(key string, members ...string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectSet, nil)
	if err != nil || !ok {
		return 0, err
	}
	n := obj.SetRemove(members)
	if obj.SetCard() == 0 {
		m.deleteEmptyLocked(key)
	} else {
		m.persistObjectLocked(key, obj)
	}
	return n, nil
}

func (m *Memory) SetIsMember(key, member string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectSet, nil)
	if err != nil || !ok {
		return false, err
	}
	return obj.SetIsMember(member), nil
}

func (m *Memory) SetMembers(key string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectSet, nil)
	if err != nil || !ok {
		return []string{}, err
	}
	return obj.SetMembers(), nil
}

func (m *Memory) SetCard(key string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectSet, nil)
	if err != nil || !ok {
		return 0, err
	}
	return obj.SetCard(), nil
}

func (m *Memory) ZAdd(key string, pairs []store.ScoredMember) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, _, err := m.objectLocked(key, store.ObjectZSet, store.NewZSet)
	if err != nil {
		return 0, err
	}
	n := obj.ZAdd(pairs)
	m.persistObjectLocked(key, obj)
	return n, nil
}

func (m *Memory) ZScore(key, member string) (float64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectZSet, nil)
	if err != nil || !ok {
		return 0, false, err
	}
	value, found := obj.ZScore(member)
	return value, found, nil
}

func (m *Memory) ZRank(key, member string) (int, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectZSet, nil)
	if err != nil || !ok {
		return 0, false, err
	}
	value, found := obj.ZRank(member)
	return value, found, nil
}

func (m *Memory) ZRange(key string, start, stop int) ([]store.ScoredMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectZSet, nil)
	if err != nil || !ok {
		return []store.ScoredMember{}, err
	}
	return obj.ZRange(start, stop), nil
}

func (m *Memory) ZRemove(key string, members ...string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok, err := m.objectLocked(key, store.ObjectZSet, nil)
	if err != nil || !ok {
		return 0, err
	}
	n := obj.ZRemove(members)
	if obj.ZLen() == 0 {
		m.deleteEmptyLocked(key)
	} else {
		m.persistObjectLocked(key, obj)
	}
	return n, nil
}
