package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/memorypolicy"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type demoClock struct{ now time.Time }

func (c *demoClock) Now() time.Time          { return c.now }
func (c *demoClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newManager(clock *demoClock, policy store.EvictionPolicy, max int64) *memorypolicy.Manager {
	cfg := store.DefaultMemoryConfig()
	cfg.Clock = clock
	cfg.Policy = policy
	cfg.MaxBytes = max
	cfg.Samples = 10
	cfg.LFUDecay = time.Hour
	cfg.RandomSeed = 1
	m, err := memorypolicy.New(versionedmemory.New(clock), cfg)
	if err != nil {
		panic(err)
	}
	return m
}

func main() {
	fmt.Println("== expiration is time invalidation, not eviction ==")
	clock := &demoClock{now: time.Unix(100, 0)}
	m := newManager(clock, store.PolicyAllKeysLRU, 250)
	ttl := time.Second
	m.Set("expired", store.NewString(strings.Repeat("x", 100)), &ttl)
	m.Set("live", store.NewString(strings.Repeat("y", 100)), nil)
	clock.Advance(2 * time.Second)
	pressure, err := m.EnforceMaxMemory()
	if err != nil {
		panic(err)
	}
	fmt.Printf("expired=%d evicted=%v used=%d\n", pressure.ExpiredKeys, pressure.EvictedKeys, pressure.UsedBytes)

	fmt.Println("\n== noeviction keeps valid keys and reports OOM ==")
	clock = &demoClock{now: time.Unix(200, 0)}
	m = newManager(clock, store.PolicyNoEviction, 100)
	m.Set("keep", store.NewString(strings.Repeat("k", 200)), nil)
	_, err = m.EnforceMaxMemory()
	_, stillThere := m.Get("keep")
	fmt.Printf("error=%v key_survives=%v\n", err, stillThere)

	fmt.Println("\n== sampled LRU scores only the candidate sample ==")
	clock = &demoClock{now: time.Unix(300, 0)}
	m = newManager(clock, store.PolicyAllKeysLRU, 1)
	m.Set("cold", store.NewString("c"), nil)
	clock.Advance(time.Second)
	m.Set("warm", store.NewString("w"), nil)
	clock.Advance(time.Second)
	m.Set("hot", store.NewString("h"), nil)
	pressure, err = m.EnforceMaxMemory()
	if err != nil {
		panic(err)
	}
	fmt.Printf("first_victim=%s examined=%d eviction_order=%v\n", pressure.EvictedKeys[0], pressure.CandidatesExamined, pressure.EvictedKeys)

	fmt.Println("\n== sampled LFU protects frequently accessed keys ==")
	clock = &demoClock{now: time.Unix(400, 0)}
	m = newManager(clock, store.PolicyAllKeysLFU, 1)
	m.Set("cold", store.NewString("c"), nil)
	clock.Advance(time.Second)
	m.Set("hot", store.NewString("h"), nil)
	for i := 0; i < 500; i++ {
		_, _ = m.Get("hot")
	}
	pressure, err = m.EnforceMaxMemory()
	if err != nil {
		panic(err)
	}
	fmt.Printf("first_victim=%s eviction_order=%v\n", pressure.EvictedKeys[0], pressure.EvictedKeys)
}
