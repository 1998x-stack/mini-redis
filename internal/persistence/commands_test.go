package persistence

import (
	"reflect"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/store"
)

func TestCommandsUseLogicalTypesAndAbsoluteExpiry(t *testing.T) {
	entries := []store.SnapshotEntry{
		{Key: "z", Type: store.ObjectZSet, ZSetValues: []store.ScoredMember{{Member: "alice", Score: 1.5}}},
		{Key: "a", Type: store.ObjectList, ListValues: []string{"x", "y"}, ExpireAtUnixMilli: 1234},
	}
	got, err := Commands(entries)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"RPUSH", "a", "x", "y"},
		{"PEXPIREAT", "a", "1234"},
		{"ZADD", "z", "1.5", "alice"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
}
