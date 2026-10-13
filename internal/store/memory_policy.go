package store

import (
	"errors"
	"fmt"
	"time"
)

var ErrOOM = errors.New("command not allowed when used memory > 'maxmemory'")

// EvictionPolicy is the v0.6 memory-pressure policy. Expiration is deliberately
// separate: TTL decides whether data is valid; eviction decides what valid data
// to remove when the configured memory budget is exceeded.
type EvictionPolicy string

const (
	PolicyNoEviction    EvictionPolicy = "noeviction"
	PolicyAllKeysRandom EvictionPolicy = "allkeys-random"
	PolicyAllKeysLRU    EvictionPolicy = "allkeys-lru"
	PolicyAllKeysLFU    EvictionPolicy = "allkeys-lfu"
)

// MemoryConfig configures the educational memory-pressure layer. MaxBytes == 0
// disables enforcement. The estimator is deterministic logical accounting, not
// Go heap/RSS accounting.
type MemoryConfig struct {
	MaxBytes   int64
	Policy     EvictionPolicy
	Samples    int
	LFUDecay   time.Duration
	RandomSeed int64
	Clock      Clock
}

func DefaultMemoryConfig() MemoryConfig {
	return MemoryConfig{
		Policy:     PolicyNoEviction,
		Samples:    5,
		LFUDecay:   time.Minute,
		RandomSeed: 1,
		Clock:      RealClock{},
	}
}

func (c MemoryConfig) Validate() error {
	if c.MaxBytes < 0 {
		return errors.New("maxmemory must be non-negative")
	}
	switch c.Policy {
	case PolicyNoEviction, PolicyAllKeysRandom, PolicyAllKeysLRU, PolicyAllKeysLFU:
	default:
		return fmt.Errorf("unknown maxmemory policy %q", c.Policy)
	}
	if c.Samples <= 0 {
		return errors.New("maxmemory-samples must be positive")
	}
	if c.LFUDecay <= 0 {
		return errors.New("lfu-decay must be positive")
	}
	if c.Clock == nil {
		return errors.New("memory policy clock is required")
	}
	return nil
}

type MemoryStats struct {
	UsedBytes          int64
	MaxBytes           int64
	Policy             EvictionPolicy
	Samples            int
	TrackedAccessKeys  int
	EvictedKeys        uint64
	ExpiredForPressure uint64
	CandidatesExamined uint64
}

type PressureResult struct {
	EvictedKeys        []string
	ExpiredKeys        int
	FreedBytes         int64
	UsedBytes          int64
	CandidatesExamined int
}

// MemoryController is optional policy functionality layered above the stable
// Store contract. Persistence code must not depend on these runtime counters.
type MemoryController interface {
	MemoryPolicyConfig() MemoryConfig
	ConfigureMemory(MemoryConfig) error
	MemoryStats() MemoryStats
	KeyMemoryUsage(key string) (int64, bool)
	EnforceMaxMemory() (PressureResult, error)
}

// ApproxSnapshotEntryBytes is a deterministic teaching estimator. It counts
// logical payload bytes plus small fixed metadata constants; it intentionally
// does not pretend to equal allocator bytes, Go heap usage, or process RSS.
func ApproxSnapshotEntryBytes(entry SnapshotEntry) int64 {
	const (
		keyEntryOverhead = int64(48)
		objectOverhead   = int64(32)
		expiryOverhead   = int64(24)
		seqItemOverhead  = int64(8)
		pairOverhead     = int64(16)
		scoreOverhead    = int64(8)
	)

	n := keyEntryOverhead + int64(len(entry.Key)) + objectOverhead
	switch entry.Type {
	case ObjectString:
		n += int64(len(entry.StringValue))
	case ObjectList:
		for _, value := range entry.ListValues {
			n += seqItemOverhead + int64(len(value))
		}
	case ObjectHash:
		for _, pair := range entry.HashValues {
			n += pairOverhead + int64(len(pair.Field)+len(pair.Value))
		}
	case ObjectSet:
		for _, member := range entry.SetValues {
			n += seqItemOverhead + int64(len(member))
		}
	case ObjectZSet:
		for _, member := range entry.ZSetValues {
			n += pairOverhead + scoreOverhead + int64(len(member.Member))
		}
	}
	if entry.ExpireAtUnixMilli != 0 {
		n += expiryOverhead
	}
	return n
}
