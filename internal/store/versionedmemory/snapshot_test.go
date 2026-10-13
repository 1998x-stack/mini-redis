package versionedmemory

import (
	"reflect"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/store"
)

func TestSnapshotRoundTripLogicalStateAndAbsoluteExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	m := New(clock)
	m.Set("str", store.NewString("001"), nil)
	_, _ = m.ListPush("list", false, []string{"a", "b", "c"})
	_, _ = m.HashSet("hash", []store.FieldValue{{Field: "b", Value: "2"}, {Field: "a", Value: "1"}})
	_, _ = m.SetAdd("set", "3", "1", "2")
	_, _ = m.ZAdd("z", []store.ScoredMember{{Member: "b", Score: 2}, {Member: "a", Score: 1}})
	if !m.ExpireAt("list", time.Unix(150, 0)) {
		t.Fatal("expire list")
	}

	entries, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("entries=%d", len(entries))
	}
	for _, entry := range entries {
		if entry.Key == "list" && entry.ExpireAtUnixMilli != 150000 {
			t.Fatalf("absolute expiry=%d", entry.ExpireAtUnixMilli)
		}
	}

	restored := New(clock)
	if err := restored.RestoreSnapshot(entries); err != nil {
		t.Fatal(err)
	}
	again, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(entries, again) {
		t.Fatalf("round trip mismatch\nwant=%#v\n got=%#v", entries, again)
	}
}

func TestRestoreSnapshotSkipsExpiredEntries(t *testing.T) {
	clock := &fakeClock{now: time.Unix(200, 0)}
	m := New(clock)
	entries := []store.SnapshotEntry{
		{Key: "old", Type: store.ObjectString, StringValue: "x", ExpireAtUnixMilli: 199000},
		{Key: "live", Type: store.ObjectString, StringValue: "y", ExpireAtUnixMilli: 201000},
	}
	if err := m.RestoreSnapshot(entries); err != nil {
		t.Fatal(err)
	}
	if m.Exists("old") != 0 || m.Exists("live") != 1 {
		t.Fatal("expired entry was resurrected or live entry disappeared")
	}
}

func TestSnapshotDoesNotExposePhysicalEncoding(t *testing.T) {
	m := New(&fakeClock{now: time.Unix(100, 0)})
	for i := 0; i < store.CompactMaxEntries+1; i++ {
		_, _ = m.ListPush("list", false, []string{string(rune('a' + i))})
	}
	obj, ok := m.Get("list")
	if !ok || obj.Encoding != store.EncodingQuickList {
		t.Fatalf("expected quicklist, got %#v", obj)
	}
	entries, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Type != store.ObjectList || len(entries[0].ListValues) != store.CompactMaxEntries+1 {
		t.Fatalf("logical snapshot=%#v", entries)
	}
}
