package versionedmemory

import (
	"errors"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/store"
)

func TestCollectionsPreserveTTLAndWrongType(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	m := New(clock)
	ttl := time.Minute
	m.Set("k", store.NewString("x"), &ttl)
	if _, err := m.ListPush("k", false, []string{"a"}); !errors.Is(err, store.ErrWrongType) {
		t.Fatalf("wrong type err=%v", err)
	}
	if got := m.TTL("k"); !got.Exists || !got.HasExpiry {
		t.Fatalf("ttl changed: %#v", got)
	}

	if n, err := m.ListPush("list", false, []string{"a", "b"}); err != nil || n != 2 {
		t.Fatalf("push n=%d err=%v", n, err)
	}
	if !m.Expire("list", time.Minute) {
		t.Fatal("expire")
	}
	if n, err := m.ListPush("list", false, []string{"c"}); err != nil || n != 3 {
		t.Fatal(n, err)
	}
	if got := m.TTL("list"); !got.HasExpiry {
		t.Fatal("list mutation lost ttl")
	}
}

func TestEmptyCollectionDeletesKeyAndTTL(t *testing.T) {
	m := New(&fakeClock{now: time.Unix(100, 0)})
	if _, err := m.SetAdd("s", "1"); err != nil {
		t.Fatal(err)
	}
	if !m.Expire("s", time.Minute) {
		t.Fatal("expire")
	}
	if n, err := m.SetRemove("s", "1"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if m.Exists("s") != 0 {
		t.Fatal("empty set key should disappear")
	}
	if got := m.TTL("s"); got.Exists {
		t.Fatal("ttl should disappear with key")
	}
}

func TestZSetEncodingVisibleThroughObject(t *testing.T) {
	m := New(&fakeClock{now: time.Unix(100, 0)})
	pairs := []store.ScoredMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
		{Member: "d", Score: 4},
		{Member: "e", Score: 5},
		{Member: "f", Score: 6},
		{Member: "g", Score: 7},
		{Member: "h", Score: 8},
		{Member: "i", Score: 9},
	}
	if n, err := m.ZAdd("z", pairs); err != nil || n != 9 {
		t.Fatal(n, err)
	}
	obj, ok := m.Get("z")
	if !ok || obj.Encoding != store.EncodingSkipList {
		t.Fatalf("obj=%#v ok=%v", obj, ok)
	}
	rank, ok, err := m.ZRank("z", "e")
	if err != nil || !ok || rank != 4 {
		t.Fatal(rank, ok, err)
	}
}
