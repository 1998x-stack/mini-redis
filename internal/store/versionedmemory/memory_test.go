package versionedmemory

import (
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
