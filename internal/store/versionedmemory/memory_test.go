package versionedmemory

import (
	"fmt"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/store"
)

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time          { return f.now }
func (f *fakeClock) Advance(d time.Duration) { f.now = f.now.Add(d) }

func TestLazyAndActiveExpiration(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	m := New(clock)
	ttl := 5 * time.Second
	m.Set("lazy", store.NewString("x"), &ttl)
	clock.Advance(6 * time.Second)
	if _, ok := m.Get("lazy"); ok {
		t.Fatal("expected lazy key to expire")
	}

	m.Set("active", store.NewString("x"), &ttl)
	clock.Advance(6 * time.Second)
	if removed := m.ExpireDue(0); removed != 1 {
		t.Fatalf("removed=%d want 1", removed)
	}
}

func TestIncrPreservesTTL(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	m := New(clock)
	ttl := 10 * time.Second
	m.Set("n", store.NewString("41"), &ttl)
	got, err := m.IncrBy("n", 1)
	if err != nil || got != 42 {
		t.Fatalf("got %d err %v", got, err)
	}
	result := m.TTL("n")
	if !result.Exists || !result.HasExpiry {
		t.Fatal("TTL should survive INCR")
	}
}

func TestSetWithoutTTLRemovesOldTTL(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	m := New(clock)
	ttl := 10 * time.Second
	m.Set("k", store.NewString("v1"), &ttl)
	m.Set("k", store.NewString("v2"), nil)
	result := m.TTL("k")
	if !result.Exists || result.HasExpiry {
		t.Fatalf("unexpected ttl result: %#v", result)
	}
}

func TestMemoryKeepsStringSemanticsDuringIncrementalRehash(t *testing.T) {
	m := New(&fakeClock{now: time.Unix(100, 0)})
	for i := 0; i < 9; i++ {
		key := fmt.Sprintf("k%d", i)
		m.Set(key, store.NewString(fmt.Sprintf("v%d", i)), nil)
	}
	if !m.Stats().Data.Rehashing {
		t.Fatalf("expected active data rehash, stats=%#v", m.Stats().Data)
	}

	// Reads/updates/deletes must work while keys are split across ht[0]/ht[1].
	for i := 0; i < 9; i++ {
		key := fmt.Sprintf("k%d", i)
		obj, ok := m.Get(key)
		if !ok {
			t.Fatalf("%s disappeared during rehash", key)
		}
		got, _ := obj.StringValue()
		if want := fmt.Sprintf("v%d", i); got != want {
			t.Fatalf("%s=%q want %q", key, got, want)
		}
	}
	m.Set("k0", store.NewString("updated"), nil)
	if deleted := m.Delete("k1"); deleted != 1 {
		t.Fatalf("Delete(k1)=%d", deleted)
	}
	if m.Len() != 8 {
		t.Fatalf("Len=%d want 8", m.Len())
	}

	for m.Stats().Data.Rehashing {
		if used := m.RehashStep(1); used > 1 {
			t.Fatalf("RehashStep(1) used %d units", used)
		}
	}
	obj, ok := m.Get("k0")
	if !ok {
		t.Fatal("updated key missing after rehash")
	}
	got, _ := obj.StringValue()
	if got != "updated" {
		t.Fatalf("k0=%q", got)
	}
	if _, ok := m.Get("k1"); ok {
		t.Fatal("deleted key reappeared after rehash")
	}
}

func TestExpiryDictionaryAlsoUsesIncrementalRehash(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	m := New(clock)
	ttl := time.Hour
	for i := 0; i < 9; i++ {
		m.Set(fmt.Sprintf("ttl%d", i), store.NewString("x"), &ttl)
	}
	if !m.Stats().Expires.Rehashing {
		t.Fatalf("expected expiry dictionary rehash, stats=%#v", m.Stats().Expires)
	}
	for i := 0; i < 9; i++ {
		result := m.TTL(fmt.Sprintf("ttl%d", i))
		if !result.Exists || !result.HasExpiry {
			t.Fatalf("ttl%d lost expiry during rehash: %#v", i, result)
		}
	}
}
