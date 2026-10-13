package rdb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/store"
)

func fixtureEntries() []store.SnapshotEntry {
	return []store.SnapshotEntry{
		{Key: "z", Type: store.ObjectZSet, ZSetValues: []store.ScoredMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}}},
		{Key: "a", Type: store.ObjectString, StringValue: "001", ExpireAtUnixMilli: 123456},
	}
}

func TestEncodeIsDeterministicAndRoundTrips(t *testing.T) {
	entries := fixtureEntries()
	first, err := Encode(entries)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Encode([]store.SnapshotEntry{entries[1], entries[0]})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same logical state encoded differently")
	}
	decoded, err := Decode(first)
	if err != nil {
		t.Fatal(err)
	}
	want := []store.SnapshotEntry{entries[1], entries[0]}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded=%#v want=%#v", decoded, want)
	}
}

func TestChecksumDetectsCorruption(t *testing.T) {
	data, err := Encode(fixtureEntries())
	if err != nil {
		t.Fatal(err)
	}
	data[headerSize] ^= 0xff
	if _, err := Decode(data); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err=%v", err)
	}
}

func TestVersionMismatchIsExplicit(t *testing.T) {
	data, err := Encode(fixtureEntries())
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint16(data[4:6], Version+1)
	if _, err := Decode(data); !errors.Is(err, ErrVersion) {
		t.Fatalf("err=%v", err)
	}
}

func TestSaveLoadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.mrdb")
	if err := SaveFile(path, fixtureEntries()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	entries, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Key != "a" || entries[1].Key != "z" {
		t.Fatalf("entries=%#v", entries)
	}
}
