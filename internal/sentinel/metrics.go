package sentinel

import (
	"sync"

	"github.com/1998x-stack/mini-redis/internal/observability"
)

type controllerMetrics struct {
	ticks            observability.Counter
	tickErrors       observability.Counter
	probes           observability.Counter
	probeFailures    observability.Counter
	failoverAttempts observability.Counter
	promotions       observability.Counter
	reconfigurations observability.Counter
	tickLatency      *observability.Histogram
	probeLatency     *observability.Histogram
	failoverLatency  *observability.Histogram
}

func newControllerMetrics() *controllerMetrics {
	return &controllerMetrics{
		tickLatency:     observability.NewDurationHistogram(),
		probeLatency:    observability.NewDurationHistogram(),
		failoverLatency: observability.NewDurationHistogram(),
	}
}

var controllerMetricTable sync.Map

func metricsFor(controller *Controller) *controllerMetrics {
	if controller == nil {
		return newControllerMetrics()
	}
	if value, ok := controllerMetricTable.Load(controller); ok {
		return value.(*controllerMetrics)
	}
	created := newControllerMetrics()
	value, _ := controllerMetricTable.LoadOrStore(controller, created)
	return value.(*controllerMetrics)
}

type MetricsSnapshot struct {
	Ticks            uint64                          `json:"ticks"`
	TickErrors       uint64                          `json:"tick_errors"`
	Probes           uint64                          `json:"probes"`
	ProbeFailures    uint64                          `json:"probe_failures"`
	FailoverAttempts uint64                          `json:"failover_attempts"`
	Promotions       uint64                          `json:"promotions"`
	Reconfigurations uint64                          `json:"reconfigurations"`
	TickLatency      observability.HistogramSnapshot `json:"tick_latency"`
	ProbeLatency     observability.HistogramSnapshot `json:"probe_latency"`
	FailoverLatency  observability.HistogramSnapshot `json:"failover_latency"`
	Epoch            uint64                          `json:"epoch"`
	PrimaryID        string                          `json:"primary_id"`
	LastDecision     Decision                        `json:"last_decision"`
}

func (c *Controller) MetricsSnapshot() MetricsSnapshot {
	m := metricsFor(c)
	if c == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		Ticks:            m.ticks.Load(),
		TickErrors:       m.tickErrors.Load(),
		Probes:           m.probes.Load(),
		ProbeFailures:    m.probeFailures.Load(),
		FailoverAttempts: m.failoverAttempts.Load(),
		Promotions:       m.promotions.Load(),
		Reconfigurations: m.reconfigurations.Load(),
		TickLatency:      m.tickLatency.Snapshot(),
		ProbeLatency:     m.probeLatency.Snapshot(),
		FailoverLatency:  m.failoverLatency.Snapshot(),
		Epoch:            c.Epoch(),
		PrimaryID:        c.PrimaryID(),
		LastDecision:     c.LastDecision(),
	}
}
