package store

import "time"

// Clock makes TTL behavior deterministic in tests and keeps time policy out of
// the storage engine.
type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
