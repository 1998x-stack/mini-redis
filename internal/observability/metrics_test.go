package observability

import (
	"sync"
	"testing"
	"time"
)

func TestHistogramIsBoundedAndMonotonic(t *testing.T) {
	h := NewHistogram([]time.Duration{time.Microsecond, time.Millisecond})
	for _, duration := range []time.Duration{time.Nanosecond, time.Microsecond, 10 * time.Microsecond, 2 * time.Millisecond} {
		h.ObserveDuration(duration)
	}
	snapshot := h.Snapshot()
	if snapshot.Count != 4 {
		t.Fatalf("count=%d want=4", snapshot.Count)
	}
	if len(snapshot.Buckets) != 3 {
		t.Fatalf("buckets=%d want=3", len(snapshot.Buckets))
	}
	var total uint64
	for _, count := range snapshot.Buckets {
		total += count
	}
	if total != snapshot.Count {
		t.Fatalf("bucket total=%d count=%d", total, snapshot.Count)
	}
	if snapshot.MaxNanos != uint64((2 * time.Millisecond).Nanoseconds()) {
		t.Fatalf("max=%d", snapshot.MaxNanos)
	}
	if len(snapshot.BoundsNanos) != 2 || snapshot.BoundsNanos[0] >= snapshot.BoundsNanos[1] {
		t.Fatalf("unexpected bounds=%v", snapshot.BoundsNanos)
	}
}

func TestMetricsConcurrentUpdates(t *testing.T) {
	metrics := NewMetrics()
	const goroutines = 16
	const iterations = 1000
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				metrics.Commands.Inc("GET")
				metrics.BytesRead.Add("reactor", 3)
				metrics.CommandLatency.ObserveDuration(time.Microsecond)
				metrics.ActiveConnections.Add("reactor", 1)
				metrics.ActiveConnections.Add("reactor", -1)
			}
		}()
	}
	wg.Wait()
	snapshot := metrics.Snapshot(time.Now())
	want := uint64(goroutines * iterations)
	if snapshot.Commands["GET"] != want {
		t.Fatalf("commands=%d want=%d", snapshot.Commands["GET"], want)
	}
	if snapshot.BytesRead["reactor"] != want*3 {
		t.Fatalf("bytes=%d want=%d", snapshot.BytesRead["reactor"], want*3)
	}
	if snapshot.CommandLatency.Count != want {
		t.Fatalf("latency count=%d want=%d", snapshot.CommandLatency.Count, want)
	}
	if snapshot.ActiveConnections["reactor"] != 0 {
		t.Fatalf("active=%d want=0", snapshot.ActiveConnections["reactor"])
	}
}

func TestSnapshotCopiesDynamicMaps(t *testing.T) {
	metrics := NewMetrics()
	metrics.Commands.Inc("GET")
	first := metrics.Snapshot(time.Now())
	first.Commands["GET"] = 999
	second := metrics.Snapshot(time.Now())
	if second.Commands["GET"] != 1 {
		t.Fatalf("snapshot mutation leaked: %d", second.Commands["GET"])
	}
}
