package server

import (
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/observability"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/version"
)

type ReplicaProgressMetricsSnapshot struct {
	ReplicaID          string `json:"replica_id"`
	RemoteAddr         string `json:"remote_addr"`
	SyncMode           string `json:"sync_mode"`
	Acknowledged       bool   `json:"acknowledged"`
	AcknowledgedOffset int64  `json:"acknowledged_offset"`
	LagBytes           int64  `json:"lag_bytes"`
	AckAgeMillis       int64  `json:"ack_age_ms"`
}

type ReplicationMetricsSnapshot struct {
	Role         string                           `json:"role"`
	ID           string                           `json:"id"`
	Offset       int64                            `json:"offset"`
	BacklogStart int64                            `json:"backlog_start"`
	BacklogBytes int64                            `json:"backlog_bytes"`
	FullSyncs    uint64                           `json:"full_syncs"`
	PartialSyncs uint64                           `json:"partial_syncs"`
	Subscribers  int                              `json:"subscribers"`
	Upstream     string                           `json:"upstream,omitempty"`
	Replicas     []ReplicaProgressMetricsSnapshot `json:"replicas,omitempty"`
}

type ServerMetricsSnapshot struct {
	Runtime     observability.Snapshot     `json:"runtime"`
	Keys        int                        `json:"keys"`
	Memory      store.MemoryStats          `json:"memory"`
	Replication ReplicationMetricsSnapshot `json:"replication"`
}

// MetricsSnapshot returns copies/atomically loaded values only. Callers cannot
// mutate server metric maps, Store state or replication internals through it.
func (s *Server) MetricsSnapshot() ServerMetricsSnapshot {
	now := time.Now()
	snapshot := ServerMetricsSnapshot{Runtime: s.metrics.Snapshot(now)}

	s.persistenceMu.Lock()
	snapshot.Keys = s.store.Len()
	if controller, ok := s.store.(store.MemoryController); ok {
		snapshot.Memory = controller.MemoryStats()
	}
	s.persistenceMu.Unlock()

	role, id, offset := s.ReplicationState()
	repl := ReplicationMetricsSnapshot{
		Role:     string(role),
		ID:       id,
		Offset:   offset,
		Upstream: s.currentReplicationUpstream(),
	}
	if primary := s.currentReplicationPrimary(); primary != nil && s.currentReplicationRole() == role {
		primarySnapshot := primary.Snapshot(now)
		repl.ID = primarySnapshot.ID
		repl.Offset = primarySnapshot.Offset
		repl.BacklogStart = primarySnapshot.BacklogStart
		repl.BacklogBytes = primarySnapshot.Offset - primarySnapshot.BacklogStart
		if repl.BacklogBytes < 0 {
			repl.BacklogBytes = 0
		}
		repl.FullSyncs = primarySnapshot.Stats.FullSyncs
		repl.PartialSyncs = primarySnapshot.Stats.PartialSyncs
		repl.Subscribers = primarySnapshot.Stats.Subscribers
		for _, progress := range primarySnapshot.Replicas {
			ackAgeMillis := int64(-1)
			if progress.Acknowledged {
				ackAgeMillis = progress.AckAge.Milliseconds()
			}
			repl.Replicas = append(repl.Replicas, ReplicaProgressMetricsSnapshot{
				ReplicaID:          progress.ReplicaID,
				RemoteAddr:         progress.RemoteAddr,
				SyncMode:           progress.SyncMode,
				Acknowledged:       progress.Acknowledged,
				AcknowledgedOffset: progress.AcknowledgedOffset,
				LagBytes:           progress.LagBytes,
				AckAgeMillis:       ackAgeMillis,
			})
		}
	}
	snapshot.Replication = repl
	return snapshot
}

func protocolMetricKey(protocol resp.Protocol) string {
	if protocol == resp.Protocol3 {
		return "resp3"
	}
	return "resp2"
}

func repliesContainError(replies []resp.Value) bool {
	for _, reply := range replies {
		if reply.IsError() {
			return true
		}
	}
	return false
}

func (s *Server) observeReply(protocol resp.Protocol, value resp.Value) {
	s.metrics.ProtocolReplies.Inc(protocolMetricKey(protocol))
	if value.Type == resp.PushType {
		s.metrics.ProtocolReplies.Inc("push")
	}
}

// metricConn observes actual bytes accepted by net.Conn Read/Write calls. It
// does not buffer, retry or alter the transport contract.
type metricConn struct {
	net.Conn
	metrics *observability.Metrics
	engine  string
}

func (c *metricConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.metrics != nil {
		c.metrics.BytesRead.Add(c.engine, uint64(n))
	}
	return n, err
}

func (c *metricConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 && c.metrics != nil {
		c.metrics.BytesWritten.Add(c.engine, uint64(n))
	}
	return n, err
}

func encodedCommandBytes(commands [][]string, transaction bool) uint64 {
	var bytes uint64
	if transaction {
		bytes += uint64(len(resp.EncodeCommand([]string{"MULTI"})))
	}
	for _, argv := range commands {
		bytes += uint64(len(resp.EncodeCommand(argv)))
	}
	if transaction {
		bytes += uint64(len(resp.EncodeCommand([]string{"EXEC"})))
	}
	return bytes
}

func (s *Server) appendAOF(commands [][]string, transaction bool) error {
	if s.aof == nil || len(commands) == 0 {
		return nil
	}
	start := time.Now()
	s.metrics.AOFAppendOps.Inc()
	s.metrics.AOFAppendBytes.Add(encodedCommandBytes(commands, transaction))
	var err error
	if transaction {
		err = s.aof.AppendTransaction(commands)
	} else {
		err = s.aof.AppendBatch(commands)
	}
	s.metrics.AOFAppendLatency.ObserveDuration(time.Since(start))
	if err != nil {
		s.metrics.AOFAppendFailures.Inc()
	}
	return err
}

func histogramSummary(value observability.HistogramSnapshot) resp.Value {
	return resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("count"), Value: resp.Integer(int64(value.Count))},
		{Key: resp.BulkString("sum_ns"), Value: resp.Integer(int64(value.SumNanos))},
		{Key: resp.BulkString("max_ns"), Value: resp.Integer(int64(value.MaxNanos))},
	})
}

func uintMapValue(values map[string]uint64) resp.Value {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]resp.MapEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, resp.MapEntry{Key: resp.BulkString(key), Value: resp.Integer(int64(values[key]))})
	}
	return resp.MapValue(entries)
}

func intMapValue(values map[string]int64) resp.Value {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]resp.MapEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, resp.MapEntry{Key: resp.BulkString(key), Value: resp.Integer(values[key])})
	}
	return resp.MapValue(entries)
}

func replicaProgressValue(values []ReplicaProgressMetricsSnapshot) resp.Value {
	items := make([]resp.Value, 0, len(values))
	for _, value := range values {
		items = append(items, resp.MapValue([]resp.MapEntry{
			{Key: resp.BulkString("replica_id"), Value: resp.BulkString(value.ReplicaID)},
			{Key: resp.BulkString("remote_addr"), Value: resp.BulkString(value.RemoteAddr)},
			{Key: resp.BulkString("sync_mode"), Value: resp.BulkString(value.SyncMode)},
			{Key: resp.BulkString("acknowledged"), Value: resp.Boolean(value.Acknowledged)},
			{Key: resp.BulkString("acknowledged_offset"), Value: resp.Integer(value.AcknowledgedOffset)},
			{Key: resp.BulkString("lag_bytes"), Value: resp.Integer(value.LagBytes)},
			{Key: resp.BulkString("ack_age_ms"), Value: resp.Integer(value.AckAgeMillis)},
		}))
	}
	return resp.Array(items)
}

func (s *Server) infoCommand(argv []string) resp.Value {
	if len(argv) > 2 {
		return resp.Error("ERR syntax: INFO [section]")
	}
	section := "all"
	if len(argv) == 2 {
		section = strings.ToLower(argv[1])
	}
	snapshot := s.MetricsSnapshot()

	serverSection := resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("version"), Value: resp.BulkString(version.Version)},
		{Key: resp.BulkString("uptime_ms"), Value: resp.Integer(snapshot.Runtime.UptimeMillis)},
		{Key: resp.BulkString("keys"), Value: resp.Integer(int64(snapshot.Keys))},
	})
	clientsSection := resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("accepted"), Value: uintMapValue(snapshot.Runtime.Connections)},
		{Key: resp.BulkString("active"), Value: intMapValue(snapshot.Runtime.ActiveConnections)},
		{Key: resp.BulkString("bytes_read"), Value: uintMapValue(snapshot.Runtime.BytesRead)},
		{Key: resp.BulkString("bytes_written"), Value: uintMapValue(snapshot.Runtime.BytesWritten)},
		{Key: resp.BulkString("backpressure"), Value: uintMapValue(snapshot.Runtime.Backpressure)},
	})
	commandsSection := resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("calls"), Value: uintMapValue(snapshot.Runtime.Commands)},
		{Key: resp.BulkString("errors"), Value: uintMapValue(snapshot.Runtime.CommandErrors)},
		{Key: resp.BulkString("latency"), Value: histogramSummary(snapshot.Runtime.CommandLatency)},
	})
	protocolSection := resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("requests"), Value: uintMapValue(snapshot.Runtime.ProtocolRequests)},
		{Key: resp.BulkString("replies"), Value: uintMapValue(snapshot.Runtime.ProtocolReplies)},
	})
	persistenceSection := resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("aof_append_ops"), Value: resp.Integer(int64(snapshot.Runtime.AOFAppendOps))},
		{Key: resp.BulkString("aof_append_bytes"), Value: resp.Integer(int64(snapshot.Runtime.AOFAppendBytes))},
		{Key: resp.BulkString("aof_append_failures"), Value: resp.Integer(int64(snapshot.Runtime.AOFAppendFailures))},
		{Key: resp.BulkString("aof_append_latency"), Value: histogramSummary(snapshot.Runtime.AOFAppendLatency)},
		{Key: resp.BulkString("rdb_save_ops"), Value: resp.Integer(int64(snapshot.Runtime.RDBSaveOps))},
		{Key: resp.BulkString("rdb_save_bytes"), Value: resp.Integer(int64(snapshot.Runtime.RDBSaveBytes))},
		{Key: resp.BulkString("rdb_save_failures"), Value: resp.Integer(int64(snapshot.Runtime.RDBSaveFailures))},
		{Key: resp.BulkString("rdb_save_latency"), Value: histogramSummary(snapshot.Runtime.RDBSaveLatency)},
		{Key: resp.BulkString("aof_rewrite_ops"), Value: resp.Integer(int64(snapshot.Runtime.AOFRewriteOps))},
		{Key: resp.BulkString("aof_rewrite_bytes"), Value: resp.Integer(int64(snapshot.Runtime.AOFRewriteBytes))},
		{Key: resp.BulkString("aof_rewrite_failures"), Value: resp.Integer(int64(snapshot.Runtime.AOFRewriteFailures))},
		{Key: resp.BulkString("aof_rewrite_latency"), Value: histogramSummary(snapshot.Runtime.AOFRewriteLatency)},
	})
	replicationSection := resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("role"), Value: resp.BulkString(snapshot.Replication.Role)},
		{Key: resp.BulkString("id"), Value: resp.BulkString(snapshot.Replication.ID)},
		{Key: resp.BulkString("offset"), Value: resp.Integer(snapshot.Replication.Offset)},
		{Key: resp.BulkString("backlog_start"), Value: resp.Integer(snapshot.Replication.BacklogStart)},
		{Key: resp.BulkString("backlog_bytes"), Value: resp.Integer(snapshot.Replication.BacklogBytes)},
		{Key: resp.BulkString("full_syncs"), Value: resp.Integer(int64(snapshot.Replication.FullSyncs))},
		{Key: resp.BulkString("partial_syncs"), Value: resp.Integer(int64(snapshot.Replication.PartialSyncs))},
		{Key: resp.BulkString("subscribers"), Value: resp.Integer(int64(snapshot.Replication.Subscribers))},
		{Key: resp.BulkString("replicas"), Value: replicaProgressValue(snapshot.Replication.Replicas)},
		{Key: resp.BulkString("acks_received"), Value: resp.Integer(int64(snapshot.Runtime.ReplicationAcksReceived))},
		{Key: resp.BulkString("acks_rejected"), Value: resp.Integer(int64(snapshot.Runtime.ReplicationAcksRejected))},
		{Key: resp.BulkString("link_failures"), Value: resp.Integer(int64(snapshot.Runtime.ReplicationLinkFailures))},
		{Key: resp.BulkString("frames_applied"), Value: resp.Integer(int64(snapshot.Runtime.ReplicationFramesApplied))},
		{Key: resp.BulkString("bytes_applied"), Value: resp.Integer(int64(snapshot.Runtime.ReplicationBytesApplied))},
	})
	memorySection := resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("used_bytes"), Value: resp.Integer(snapshot.Memory.UsedBytes)},
		{Key: resp.BulkString("max_bytes"), Value: resp.Integer(snapshot.Memory.MaxBytes)},
		{Key: resp.BulkString("policy"), Value: resp.BulkString(string(snapshot.Memory.Policy))},
		{Key: resp.BulkString("expired_keys"), Value: resp.Integer(int64(snapshot.Runtime.ExpiredKeys))},
		{Key: resp.BulkString("evicted_keys"), Value: resp.Integer(int64(snapshot.Runtime.EvictedKeys))},
	})
	reactorSection := resp.MapValue([]resp.MapEntry{
		{Key: resp.BulkString("poll_calls"), Value: resp.Integer(int64(snapshot.Runtime.ReactorPollCalls))},
		{Key: resp.BulkString("poll_events"), Value: resp.Integer(int64(snapshot.Runtime.ReactorPollEvents))},
		{Key: resp.BulkString("poll_wait"), Value: histogramSummary(snapshot.Runtime.ReactorPollWait)},
	})

	sections := map[string]resp.Value{
		"server":      serverSection,
		"clients":     clientsSection,
		"commands":    commandsSection,
		"protocol":    protocolSection,
		"persistence": persistenceSection,
		"replication": replicationSection,
		"memory":      memorySection,
		"reactor":     reactorSection,
	}
	if section != "all" {
		value, ok := sections[section]
		if !ok {
			return resp.Error("ERR unknown INFO section " + strconv.Quote(section))
		}
		return value
	}
	keys := []string{"server", "clients", "commands", "protocol", "persistence", "replication", "memory", "reactor"}
	entries := make([]resp.MapEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, resp.MapEntry{Key: resp.BulkString(key), Value: sections[key]})
	}
	return resp.MapValue(entries)
}
