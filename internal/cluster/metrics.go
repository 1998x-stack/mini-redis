package cluster

import (
	"time"

	"github.com/1998x-stack/mini-redis/internal/observability"
)

type metrics struct {
	routes          observability.Counter
	executes        observability.Counter
	moved           observability.Counter
	ask             observability.Counter
	clusterDown     observability.Counter
	crossSlot       observability.Counter
	gossipPages     observability.Counter
	gossipRecords   observability.Counter
	gossipMerges    observability.Counter
	migrationRuns   observability.Counter
	migrationKeys   observability.Counter
	migrationErrors observability.Counter
	migrationTime   *observability.Histogram
}

func newMetrics() *metrics {
	return &metrics{migrationTime: observability.NewDurationHistogram()}
}

type MetricsSnapshot struct {
	Routes          uint64                          `json:"routes"`
	Executes        uint64                          `json:"executes"`
	Moved           uint64                          `json:"moved"`
	Ask             uint64                          `json:"ask"`
	ClusterDown     uint64                          `json:"cluster_down"`
	CrossSlot       uint64                          `json:"cross_slot"`
	GossipPages     uint64                          `json:"gossip_pages"`
	GossipRecords   uint64                          `json:"gossip_records"`
	GossipMerges    uint64                          `json:"gossip_merges"`
	MigrationRuns   uint64                          `json:"migration_runs"`
	MigrationKeys   uint64                          `json:"migration_keys"`
	MigrationErrors uint64                          `json:"migration_errors"`
	MigrationTime   observability.HistogramSnapshot `json:"migration_time"`
}

func (t *Topology) MetricsSnapshot() MetricsSnapshot {
	if t == nil || t.metrics == nil {
		return MetricsSnapshot{}
	}
	m := t.metrics
	return MetricsSnapshot{
		Routes:          m.routes.Load(),
		Executes:        m.executes.Load(),
		Moved:           m.moved.Load(),
		Ask:             m.ask.Load(),
		ClusterDown:     m.clusterDown.Load(),
		CrossSlot:       m.crossSlot.Load(),
		GossipPages:     m.gossipPages.Load(),
		GossipRecords:   m.gossipRecords.Load(),
		GossipMerges:    m.gossipMerges.Load(),
		MigrationRuns:   m.migrationRuns.Load(),
		MigrationKeys:   m.migrationKeys.Load(),
		MigrationErrors: m.migrationErrors.Load(),
		MigrationTime:   m.migrationTime.Snapshot(),
	}
}

func (t *Topology) RecordCrossSlot() {
	if t != nil && t.metrics != nil {
		t.metrics.crossSlot.Inc()
	}
}

func (t *Topology) observeRoute(action RouteAction) {
	if t == nil || t.metrics == nil {
		return
	}
	t.metrics.routes.Inc()
	switch action {
	case RouteExecute:
		t.metrics.executes.Inc()
	case RouteMoved:
		t.metrics.moved.Inc()
	case RouteAsk:
		t.metrics.ask.Inc()
	case RouteClusterDown:
		t.metrics.clusterDown.Inc()
	}
}

func (t *Topology) observeGossipPage(records int) {
	if t == nil || t.metrics == nil {
		return
	}
	t.metrics.gossipPages.Inc()
	if records > 0 {
		t.metrics.gossipRecords.Add(uint64(records))
	}
}

func (t *Topology) observeGossipMerge(records int) {
	if t == nil || t.metrics == nil {
		return
	}
	t.metrics.gossipMerges.Inc()
	if records > 0 {
		t.metrics.gossipRecords.Add(uint64(records))
	}
}

func (t *Topology) observeMigration(duration time.Duration, keys int, err error) {
	if t == nil || t.metrics == nil {
		return
	}
	t.metrics.migrationRuns.Inc()
	if keys > 0 {
		t.metrics.migrationKeys.Add(uint64(keys))
	}
	if err != nil {
		t.metrics.migrationErrors.Inc()
	}
	t.metrics.migrationTime.ObserveDuration(duration)
}
