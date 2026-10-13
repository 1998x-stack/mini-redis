package observability

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Counter is a monotonic uint64 metric. It is intentionally tiny so hot-path
// instrumentation does not need to share subsystem locks.
type Counter struct {
	value atomic.Uint64
}

func (c *Counter) Inc()             { c.value.Add(1) }
func (c *Counter) Add(v uint64)     { c.value.Add(v) }
func (c *Counter) Load() uint64     { return c.value.Load() }

// Gauge is a signed point-in-time value, used for active connections and other
// quantities that can move in both directions.
type Gauge struct {
	value atomic.Int64
}

func (g *Gauge) Add(v int64) int64 { return g.value.Add(v) }
func (g *Gauge) Store(v int64)     { g.value.Store(v) }
func (g *Gauge) Load() int64       { return g.value.Load() }

type CounterMap struct {
	mu     sync.RWMutex
	values map[string]*Counter
}

func (m *CounterMap) counter(key string) *Counter {
	m.mu.RLock()
	counter := m.values[key]
	m.mu.RUnlock()
	if counter != nil {
		return counter
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values == nil {
		m.values = make(map[string]*Counter)
	}
	counter = m.values[key]
	if counter == nil {
		counter = &Counter{}
		m.values[key] = counter
	}
	return counter
}

func (m *CounterMap) Inc(key string)         { m.counter(key).Inc() }
func (m *CounterMap) Add(key string, v uint64) { m.counter(key).Add(v) }

func (m *CounterMap) Snapshot() map[string]uint64 {
	m.mu.RLock()
	keys := make([]string, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]uint64, len(keys))
	for _, key := range keys {
		out[key] = m.values[key].Load()
	}
	m.mu.RUnlock()
	return out
}

type GaugeMap struct {
	mu     sync.RWMutex
	values map[string]*Gauge
}

func (m *GaugeMap) gauge(key string) *Gauge {
	m.mu.RLock()
	gauge := m.values[key]
	m.mu.RUnlock()
	if gauge != nil {
		return gauge
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values == nil {
		m.values = make(map[string]*Gauge)
	}
	gauge = m.values[key]
	if gauge == nil {
		gauge = &Gauge{}
		m.values[key] = gauge
	}
	return gauge
}

func (m *GaugeMap) Add(key string, v int64) int64 { return m.gauge(key).Add(v) }
func (m *GaugeMap) Store(key string, v int64)     { m.gauge(key).Store(v) }

func (m *GaugeMap) Snapshot() map[string]int64 {
	m.mu.RLock()
	keys := make([]string, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]int64, len(keys))
	for _, key := range keys {
		out[key] = m.values[key].Load()
	}
	m.mu.RUnlock()
	return out
}

// Histogram keeps one counter per finite upper bound plus one overflow bucket.
// Samples are never retained, so memory stays bounded regardless of runtime.
type Histogram struct {
	boundsNanos []int64
	buckets     []atomic.Uint64
	count       atomic.Uint64
	sumNanos    atomic.Uint64
	maxNanos    atomic.Uint64
}

var defaultDurationBounds = []time.Duration{
	10 * time.Microsecond,
	50 * time.Microsecond,
	100 * time.Microsecond,
	500 * time.Microsecond,
	1 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
}

func NewDurationHistogram() *Histogram {
	return NewHistogram(defaultDurationBounds)
}

func NewHistogram(bounds []time.Duration) *Histogram {
	clean := append([]time.Duration(nil), bounds...)
	sort.Slice(clean, func(i, j int) bool { return clean[i] < clean[j] })
	nanos := make([]int64, 0, len(clean))
	for _, bound := range clean {
		if bound <= 0 {
			continue
		}
		if len(nanos) > 0 && nanos[len(nanos)-1] == int64(bound) {
			continue
		}
		nanos = append(nanos, int64(bound))
	}
	return &Histogram{boundsNanos: nanos, buckets: make([]atomic.Uint64, len(nanos)+1)}
}

func (h *Histogram) ObserveDuration(duration time.Duration) {
	if h == nil {
		return
	}
	nanos := duration.Nanoseconds()
	if nanos < 0 {
		nanos = 0
	}
	h.count.Add(1)
	h.sumNanos.Add(uint64(nanos))
	for {
		current := h.maxNanos.Load()
		if uint64(nanos) <= current || h.maxNanos.CompareAndSwap(current, uint64(nanos)) {
			break
		}
	}
	index := sort.Search(len(h.boundsNanos), func(i int) bool { return nanos <= h.boundsNanos[i] })
	h.buckets[index].Add(1)
}

type HistogramSnapshot struct {
	BoundsNanos []int64  `json:"bounds_nanos"`
	Buckets     []uint64 `json:"buckets"`
	Count       uint64   `json:"count"`
	SumNanos    uint64   `json:"sum_nanos"`
	MaxNanos    uint64   `json:"max_nanos"`
}

func (h *Histogram) Snapshot() HistogramSnapshot {
	if h == nil {
		return HistogramSnapshot{}
	}
	bounds := append([]int64(nil), h.boundsNanos...)
	buckets := make([]uint64, len(h.buckets))
	for i := range h.buckets {
		buckets[i] = h.buckets[i].Load()
	}
	return HistogramSnapshot{
		BoundsNanos: bounds,
		Buckets:     buckets,
		Count:       h.count.Load(),
		SumNanos:    h.sumNanos.Load(),
		MaxNanos:    h.maxNanos.Load(),
	}
}

// Metrics contains process-local observation state for the server data plane.
// It is deliberately not serializable database state.
type Metrics struct {
	startedAt time.Time

	Commands         CounterMap
	CommandErrors    CounterMap
	ProtocolRequests CounterMap
	ProtocolReplies  CounterMap
	Connections      CounterMap
	ActiveConnections GaugeMap
	BytesRead        CounterMap
	BytesWritten     CounterMap
	Backpressure     CounterMap

	CommandLatency  *Histogram
	ReactorPollWait *Histogram
	ReactorPollCalls Counter
	ReactorPollEvents Counter

	ExpiredKeys     Counter
	EvictedKeys     Counter
	MaintenanceRuns Counter
	RehashSteps     Counter

	AOFAppendOps      Counter
	AOFAppendBytes    Counter
	AOFAppendFailures Counter
	AOFAppendLatency  *Histogram
	RDBSaveOps        Counter
	RDBSaveBytes      Counter
	RDBSaveFailures   Counter
	RDBSaveLatency    *Histogram
	AOFRewriteOps      Counter
	AOFRewriteBytes    Counter
	AOFRewriteFailures Counter
	AOFRewriteLatency  *Histogram

	ReplicationLinkFailures Counter
	ReplicationFramesApplied Counter
	ReplicationBytesApplied Counter
	ReplicationFullSyncsApplied Counter
	ReplicationPartialSyncsApplied Counter
}

func NewMetrics() *Metrics {
	return &Metrics{
		startedAt:       time.Now(),
		CommandLatency:  NewDurationHistogram(),
		ReactorPollWait: NewDurationHistogram(),
		AOFAppendLatency: NewDurationHistogram(),
		RDBSaveLatency:   NewDurationHistogram(),
		AOFRewriteLatency: NewDurationHistogram(),
	}
}

type Snapshot struct {
	StartedAtUnixMilli int64 `json:"started_at_unix_ms"`
	UptimeMillis       int64 `json:"uptime_ms"`

	Commands          map[string]uint64 `json:"commands"`
	CommandErrors     map[string]uint64 `json:"command_errors"`
	ProtocolRequests  map[string]uint64 `json:"protocol_requests"`
	ProtocolReplies   map[string]uint64 `json:"protocol_replies"`
	Connections       map[string]uint64 `json:"connections_accepted"`
	ActiveConnections map[string]int64  `json:"connections_active"`
	BytesRead         map[string]uint64 `json:"bytes_read"`
	BytesWritten      map[string]uint64 `json:"bytes_written"`
	Backpressure      map[string]uint64 `json:"backpressure"`

	CommandLatency   HistogramSnapshot `json:"command_latency"`
	ReactorPollWait  HistogramSnapshot `json:"reactor_poll_wait"`
	ReactorPollCalls uint64            `json:"reactor_poll_calls"`
	ReactorPollEvents uint64           `json:"reactor_poll_events"`

	ExpiredKeys     uint64 `json:"expired_keys"`
	EvictedKeys     uint64 `json:"evicted_keys"`
	MaintenanceRuns uint64 `json:"maintenance_runs"`
	RehashSteps     uint64 `json:"rehash_steps"`

	AOFAppendOps      uint64            `json:"aof_append_ops"`
	AOFAppendBytes    uint64            `json:"aof_append_bytes"`
	AOFAppendFailures uint64            `json:"aof_append_failures"`
	AOFAppendLatency  HistogramSnapshot `json:"aof_append_latency"`
	RDBSaveOps        uint64            `json:"rdb_save_ops"`
	RDBSaveBytes      uint64            `json:"rdb_save_bytes"`
	RDBSaveFailures   uint64            `json:"rdb_save_failures"`
	RDBSaveLatency    HistogramSnapshot `json:"rdb_save_latency"`
	AOFRewriteOps      uint64            `json:"aof_rewrite_ops"`
	AOFRewriteBytes    uint64            `json:"aof_rewrite_bytes"`
	AOFRewriteFailures uint64            `json:"aof_rewrite_failures"`
	AOFRewriteLatency  HistogramSnapshot `json:"aof_rewrite_latency"`

	ReplicationLinkFailures      uint64 `json:"replication_link_failures"`
	ReplicationFramesApplied     uint64 `json:"replication_frames_applied"`
	ReplicationBytesApplied      uint64 `json:"replication_bytes_applied"`
	ReplicationFullSyncsApplied  uint64 `json:"replication_full_syncs_applied"`
	ReplicationPartialSyncsApplied uint64 `json:"replication_partial_syncs_applied"`
}

func (m *Metrics) Snapshot(now time.Time) Snapshot {
	if m == nil {
		return Snapshot{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	return Snapshot{
		StartedAtUnixMilli: m.startedAt.UnixMilli(),
		UptimeMillis:       now.Sub(m.startedAt).Milliseconds(),
		Commands:           m.Commands.Snapshot(),
		CommandErrors:      m.CommandErrors.Snapshot(),
		ProtocolRequests:   m.ProtocolRequests.Snapshot(),
		ProtocolReplies:    m.ProtocolReplies.Snapshot(),
		Connections:        m.Connections.Snapshot(),
		ActiveConnections:  m.ActiveConnections.Snapshot(),
		BytesRead:          m.BytesRead.Snapshot(),
		BytesWritten:       m.BytesWritten.Snapshot(),
		Backpressure:       m.Backpressure.Snapshot(),
		CommandLatency:     m.CommandLatency.Snapshot(),
		ReactorPollWait:    m.ReactorPollWait.Snapshot(),
		ReactorPollCalls:   m.ReactorPollCalls.Load(),
		ReactorPollEvents:  m.ReactorPollEvents.Load(),
		ExpiredKeys:        m.ExpiredKeys.Load(),
		EvictedKeys:        m.EvictedKeys.Load(),
		MaintenanceRuns:    m.MaintenanceRuns.Load(),
		RehashSteps:        m.RehashSteps.Load(),
		AOFAppendOps:       m.AOFAppendOps.Load(),
		AOFAppendBytes:     m.AOFAppendBytes.Load(),
		AOFAppendFailures:  m.AOFAppendFailures.Load(),
		AOFAppendLatency:   m.AOFAppendLatency.Snapshot(),
		RDBSaveOps:         m.RDBSaveOps.Load(),
		RDBSaveBytes:       m.RDBSaveBytes.Load(),
		RDBSaveFailures:    m.RDBSaveFailures.Load(),
		RDBSaveLatency:     m.RDBSaveLatency.Snapshot(),
		AOFRewriteOps:      m.AOFRewriteOps.Load(),
		AOFRewriteBytes:    m.AOFRewriteBytes.Load(),
		AOFRewriteFailures: m.AOFRewriteFailures.Load(),
		AOFRewriteLatency:  m.AOFRewriteLatency.Snapshot(),
		ReplicationLinkFailures: m.ReplicationLinkFailures.Load(),
		ReplicationFramesApplied: m.ReplicationFramesApplied.Load(),
		ReplicationBytesApplied: m.ReplicationBytesApplied.Load(),
		ReplicationFullSyncsApplied: m.ReplicationFullSyncsApplied.Load(),
		ReplicationPartialSyncsApplied: m.ReplicationPartialSyncsApplied.Load(),
	}
}
