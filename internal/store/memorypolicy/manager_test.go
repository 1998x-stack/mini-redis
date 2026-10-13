package memorypolicy

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newManagerForTest(t *testing.T, clock *testClock, policy store.EvictionPolicy, max int64, samples int) *Manager {
	t.Helper()
	cfg := store.DefaultMemoryConfig()
	cfg.Clock = clock
	cfg.Policy = policy
	cfg.MaxBytes = max
	cfg.Samples = samples
	cfg.LFUDecay = time.Hour
	cfg.RandomSeed = 1
	m, err := New(versionedmemory.New(clock), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestExpirationIsNotCountedAsEviction(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0)}
	m := newManagerForTest(t, clock, store.PolicyAllKeysLRU, 250, 8)

	ttl := time.Second
	m.Set("expired", store.NewString(strings.Repeat("x", 100)), &ttl)
	m.Set("live", store.NewString(strings.Repeat("y", 100)), nil)
	clock.Advance(2 * time.Second)

	result, err := m.EnforceMaxMemory()
	if err != nil {
		t.Fatal(err)
	}
	if result.ExpiredKeys != 1 {
		t.Fatalf("expired=%d want 1", result.ExpiredKeys)
	}
	if len(result.EvictedKeys) != 0 {
		t.Fatalf("expired key must not count as eviction: %#v", result.EvictedKeys)
	}
	if _, ok := m.Get("expired"); ok {
		t.Fatal("expired key survived pressure cycle")
	}
	if _, ok := m.Get("live"); !ok {
		t.Fatal("live key was evicted even though expiration freed enough space")
	}
}

func TestNoEvictionReportsOOMWithoutDeletingLiveKeys(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0)}
	m := newManagerForTest(t, clock, store.PolicyNoEviction, 100, 5)
	m.Set("keep", store.NewString(strings.Repeat("v", 200)), nil)

	result, err := m.EnforceMaxMemory()
	if !errors.Is(err, store.ErrOOM) {
		t.Fatalf("err=%v want ErrOOM", err)
	}
	if len(result.EvictedKeys) != 0 {
		t.Fatalf("noeviction deleted keys: %#v", result.EvictedKeys)
	}
	if _, ok := m.Get("keep"); !ok {
		t.Fatal("noeviction removed live key")
	}
}

func TestSampledLRUEvictsOldestCandidateFirst(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0)}
	m := newManagerForTest(t, clock, store.PolicyAllKeysLRU, 1, 10)

	m.Set("cold", store.NewString("c"), nil)
	clock.Advance(time.Second)
	m.Set("warm", store.NewString("w"), nil)
	clock.Advance(time.Second)
	m.Set("hot", store.NewString("h"), nil)

	result, err := m.EnforceMaxMemory()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EvictedKeys) == 0 || result.EvictedKeys[0] != "cold" {
		t.Fatalf("eviction order=%v want cold first", result.EvictedKeys)
	}
	if result.CandidatesExamined > 10*len(result.EvictedKeys) {
		t.Fatalf("examined=%d exceeds bounded sampling budget for %d evictions", result.CandidatesExamined, len(result.EvictedKeys))
	}
}

func TestSampledLFUPrefersColderFrequency(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0)}
	m := newManagerForTest(t, clock, store.PolicyAllKeysLFU, 1, 10)

	m.Set("cold", store.NewString("c"), nil)
	clock.Advance(time.Second)
	m.Set("hot", store.NewString("h"), nil)
	for i := 0; i < 500; i++ {
		if _, ok := m.Get("hot"); !ok {
			t.Fatal("hot key disappeared before enforcement")
		}
	}

	result, err := m.EnforceMaxMemory()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EvictedKeys) == 0 || result.EvictedKeys[0] != "cold" {
		t.Fatalf("eviction order=%v want cold first", result.EvictedKeys)
	}
}

func TestSnapshotDoesNotPersistAccessPolicyMetadata(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0)}
	m := newManagerForTest(t, clock, store.PolicyAllKeysLFU, 1<<20, 5)
	m.Set("k", store.NewString("v"), nil)
	for i := 0; i < 20; i++ {
		_, _ = m.Get("k")
	}
	before := m.MemoryStats()
	if before.TrackedAccessKeys != 1 {
		t.Fatalf("tracked=%d want 1", before.TrackedAccessKeys)
	}

	entries, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "k" || entries[0].StringValue != "v" {
		t.Fatalf("snapshot=%#v", entries)
	}

	fresh := newManagerForTest(t, clock, store.PolicyAllKeysLFU, 1<<20, 5)
	if err := fresh.RestoreSnapshot(entries); err != nil {
		t.Fatal(err)
	}
	after := fresh.MemoryStats()
	if after.TrackedAccessKeys != 0 {
		t.Fatalf("runtime heat leaked through snapshot: tracked=%d", after.TrackedAccessKeys)
	}
}
