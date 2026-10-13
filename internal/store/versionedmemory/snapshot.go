package versionedmemory

import (
	"fmt"
	"sort"
	"time"

	"github.com/1998x-stack/mini-redis/internal/dict"
	"github.com/1998x-stack/mini-redis/internal/store"
)

// Snapshot captures one deterministic logical point-in-time view. It removes
// already-expired keys first and never serializes physical encodings.
func (m *Memory) Snapshot() ([]store.SnapshotEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	for _, key := range m.expires.Keys(0) {
		m.deleteIfExpiredLocked(key, now)
	}

	keys := m.data.Keys(0)
	sort.Strings(keys)
	entries := make([]store.SnapshotEntry, 0, len(keys))
	for _, key := range keys {
		obj, ok := m.data.Peek(key)
		if !ok {
			continue
		}
		entry := store.SnapshotEntry{Key: key, Type: obj.Type}
		if deadline, ok := m.expires.Peek(key); ok {
			entry.ExpireAtUnixMilli = deadline.UnixMilli()
		}

		switch obj.Type {
		case store.ObjectString:
			value, ok := obj.StringValue()
			if !ok {
				return nil, fmt.Errorf("snapshot %q: invalid string encoding", key)
			}
			entry.StringValue = value
		case store.ObjectList:
			entry.ListValues = obj.ListRange(0, obj.ListLen()-1)
		case store.ObjectHash:
			entry.HashValues = obj.HashGetAll()
		case store.ObjectSet:
			entry.SetValues = obj.SetMembers()
		case store.ObjectZSet:
			entry.ZSetValues = obj.ZRange(0, obj.ZLen()-1)
		default:
			return nil, fmt.Errorf("snapshot %q: unsupported object type %d", key, obj.Type)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// RestoreSnapshot replaces the whole logical dataset. The reconstruction uses
// public object constructors/mutators, so current encoding policy decides the
// new physical layout. Expired entries are deliberately skipped.
func (m *Memory) RestoreSnapshot(entries []store.SnapshotEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data := dict.New[store.Object](dict.FNV1a)
	expires := dict.New[time.Time](dict.FNV1a)
	now := m.clock.Now()
	seen := make(map[string]struct{}, len(entries))

	for _, entry := range entries {
		if entry.Key == "" {
			return fmt.Errorf("restore snapshot: empty key")
		}
		if _, exists := seen[entry.Key]; exists {
			return fmt.Errorf("restore snapshot: duplicate key %q", entry.Key)
		}
		seen[entry.Key] = struct{}{}

		if entry.ExpireAtUnixMilli != 0 {
			deadline := time.UnixMilli(entry.ExpireAtUnixMilli)
			if !deadline.After(now) {
				continue
			}
			expires.Set(entry.Key, deadline)
		}

		var obj store.Object
		switch entry.Type {
		case store.ObjectString:
			obj = store.NewString(entry.StringValue)
		case store.ObjectList:
			if len(entry.ListValues) == 0 {
				return fmt.Errorf("restore snapshot %q: empty list", entry.Key)
			}
			obj = store.NewList()
			obj.ListPush(false, entry.ListValues)
		case store.ObjectHash:
			if len(entry.HashValues) == 0 {
				return fmt.Errorf("restore snapshot %q: empty hash", entry.Key)
			}
			obj = store.NewHash()
			if added := obj.HashSet(entry.HashValues); added != len(entry.HashValues) {
				return fmt.Errorf("restore snapshot %q: duplicate hash field", entry.Key)
			}
		case store.ObjectSet:
			if len(entry.SetValues) == 0 {
				return fmt.Errorf("restore snapshot %q: empty set", entry.Key)
			}
			obj = store.NewSet()
			if added := obj.SetAdd(entry.SetValues); added != len(entry.SetValues) {
				return fmt.Errorf("restore snapshot %q: duplicate set member", entry.Key)
			}
		case store.ObjectZSet:
			if len(entry.ZSetValues) == 0 {
				return fmt.Errorf("restore snapshot %q: empty zset", entry.Key)
			}
			obj = store.NewZSet()
			if added := obj.ZAdd(entry.ZSetValues); added != len(entry.ZSetValues) {
				return fmt.Errorf("restore snapshot %q: duplicate zset member", entry.Key)
			}
		default:
			return fmt.Errorf("restore snapshot %q: unsupported object type %d", entry.Key, entry.Type)
		}
		data.Set(entry.Key, obj)
	}

	m.data = data
	m.expires = expires
	return nil
}
