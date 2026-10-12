package dict

import (
	"strconv"
	"testing"
)

func BenchmarkDictSteadyGet(b *testing.B) {
	d := New[int](FNV1a)
	for i := 0; i < 4096; i++ {
		d.Set(strconv.Itoa(i), i)
	}
	for d.IsRehashing() {
		d.RehashStep(1024)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.Get(strconv.Itoa(i & 4095))
	}
}

func BenchmarkGoMapSteadyGet(b *testing.B) {
	m := make(map[string]int, 4096)
	for i := 0; i < 4096; i++ {
		m[strconv.Itoa(i)] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m[strconv.Itoa(i&4095)]
	}
}

func BenchmarkDictIncrementalGrowth(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		d := NewWithSize[int](8, FNV1a)
		for i := 0; i < 4096; i++ {
			d.Set(strconv.Itoa(i), i)
			d.RehashStep(1)
		}
	}
}
