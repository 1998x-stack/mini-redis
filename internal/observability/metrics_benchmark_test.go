package observability

import (
	"testing"
	"time"
)

func BenchmarkCounterAdd(b *testing.B) {
	var counter Counter
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		counter.Inc()
	}
}

func BenchmarkCounterMapHotLabel(b *testing.B) {
	var counters CounterMap
	counters.Inc("GET") // create label before timing steady-state updates.
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		counters.Inc("GET")
	}
}

func BenchmarkHistogramObserve(b *testing.B) {
	histogram := NewDurationHistogram()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		histogram.ObserveDuration(125 * time.Microsecond)
	}
}
