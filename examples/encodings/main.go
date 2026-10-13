package main

import (
	"fmt"

	"github.com/1998x-stack/mini-redis/internal/store"
)

func main() {
	list := store.NewList()
	list.ListPush(false, []string{"a", "b", "c", "d", "e", "f", "g", "h"})
	fmt.Printf("list 8 entries  -> %s\n", list.Encoding)
	list.ListPush(false, []string{"i"})
	fmt.Printf("list 9 entries  -> %s\n", list.Encoding)

	hash := store.NewHash()
	for i := 0; i < 9; i++ {
		hash.HashSet([]store.FieldValue{{Field: fmt.Sprintf("f%d", i), Value: "v"}})
		if i == 7 || i == 8 {
			fmt.Printf("hash %d fields  -> %s\n", i+1, hash.Encoding)
		}
	}

	set := store.NewSet()
	set.SetAdd([]string{"1", "2", "3"})
	fmt.Printf("integer set     -> %s\n", set.Encoding)
	set.SetAdd([]string{"01"})
	fmt.Printf("mixed set       -> %s\n", set.Encoding)

	zset := store.NewZSet()
	for i := 0; i < 9; i++ {
		zset.ZAdd([]store.ScoredMember{{Member: fmt.Sprintf("m%d", i), Score: float64(i)}})
		if i == 7 || i == 8 {
			fmt.Printf("zset %d entries -> %s\n", i+1, zset.Encoding)
		}
	}
}
