package store

import (
	"errors"
	"time"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
)

// ObjectType deliberately exists from v0.1 even though only strings are
// implemented. Future versions can add List/Hash/Set/ZSet without changing
// the command and persistence layers' fundamental contracts.
type ObjectType uint8

const (
	ObjectString ObjectType = iota + 1
)

type Object struct {
	Type ObjectType
	Str  string
}

func NewString(value string) Object {
	return Object{Type: ObjectString, Str: value}
}

// TTLResult mirrors the three states Redis users care about:
//   - key missing
//   - key exists but has no expiration
//   - key exists and has a remaining TTL
type TTLResult struct {
	Exists    bool
	HasExpiry bool
	Remaining time.Duration
}

// Store is the stable data-engine boundary.
//
// v0.1 ships a single in-memory implementation. A later version can add a
// sharded store, snapshot-aware store, or alternate clock without changing
// command handlers.
type Store interface {
	Get(key string) (Object, bool)
	Set(key string, value Object, ttl *time.Duration)
	Delete(keys ...string) int
	Exists(keys ...string) int
	IncrBy(key string, delta int64) (int64, error)
	Expire(key string, ttl time.Duration) bool
	ExpireAt(key string, deadline time.Time) bool
	TTL(key string) TTLResult
	Keys(pattern string) ([]string, error)
	Flush()

	// ExpireDue performs active expiration. limit <= 0 means full scan.
	// It returns the number of keys physically removed.
	ExpireDue(limit int) int
	Len() int
}
