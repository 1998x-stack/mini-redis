package main

import (
	"fmt"

	"github.com/1998x-stack/mini-redis/internal/dict"
)

// This deliberately forces every key into the same bucket so the demo can
// show that one RehashStep(1) still moves at most one entry.
func collisionHash(string) uint64 { return 0 }

func main() {
	d := dict.NewWithSize[string](4, collisionHash)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key:%d", i)
		d.Set(key, fmt.Sprintf("value:%d", i))
	}

	fmt.Printf("start: %+v\n", d.Stats())
	for step := 1; d.IsRehashing(); step++ {
		used := d.RehashStep(1)
		fmt.Printf("step %d (work=%d): %+v\n", step, used, d.Stats())
	}
}
