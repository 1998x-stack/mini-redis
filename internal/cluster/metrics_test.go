package cluster

import (
	"context"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/store"
)

type metricsTarget struct {
	id       string
	imported []string
}

func (t *metricsTarget) ID() string { return t.id }
func (t *metricsTarget) ImportKey(_ context.Context, entry store.SnapshotEntry) error {
	t.imported = append(t.imported, entry.Key)
	return nil
}

type metricsSource struct {
	id   string
	keys []string
}

func (s *metricsSource) ID() string { return s.id }
func (s *metricsSource) KeysInSlot(_ context.Context, _ int) ([]string, error) {
	return append([]string(nil), s.keys...), nil
}
func (s *metricsSource) TransferKey(ctx context.Context, key string, target MigrationTarget) error {
	if err := target.ImportKey(ctx, store.SnapshotEntry{Key: key}); err != nil {
		return err
	}
	for i, candidate := range s.keys {
		if candidate == key {
			s.keys = append(s.keys[:i], s.keys[i+1:]...)
			break
		}
	}
	return nil
}

func TestTopologyMetricsObserveRealRoutingGossipAndMigration(t *testing.T) {
	topology := NewTopology()
	if err := topology.UpsertNode("source", "source:6379"); err != nil {
		t.Fatal(err)
	}
	if err := topology.UpsertNode("target", "target:6379"); err != nil {
		t.Fatal(err)
	}

	key := "metric:{42}"
	slot := HashSlot(key)
	if err := topology.AssignSlot(slot, "source"); err != nil {
		t.Fatal(err)
	}
	if decision := topology.Route("target", key, false, false); decision.Action != RouteMoved {
		t.Fatalf("stable route=%v want MOVED", decision.Action)
	}
	if err := topology.BeginMigration(slot, "source", "target"); err != nil {
		t.Fatal(err)
	}
	if decision := topology.Route("source", key, false, false); decision.Action != RouteAsk {
		t.Fatalf("migration route=%v want ASK", decision.Action)
	}
	topology.RecordCrossSlot()

	source := &metricsSource{id: "source", keys: []string{key}}
	target := &metricsTarget{id: "target"}
	if err := MigrateSlot(context.Background(), topology, slot, source, target); err != nil {
		t.Fatal(err)
	}
	if len(target.imported) != 1 || target.imported[0] != key {
		t.Fatalf("imported=%v", target.imported)
	}

	page, err := topology.GossipPage(slot, 1)
	if err != nil {
		t.Fatal(err)
	}
	peer := NewTopology()
	if err := peer.MergePage(page); err != nil {
		t.Fatal(err)
	}

	snapshot := topology.MetricsSnapshot()
	if snapshot.Moved != 1 || snapshot.Ask != 1 || snapshot.CrossSlot != 1 {
		t.Fatalf("route metrics=%+v", snapshot)
	}
	if snapshot.MigrationRuns != 1 || snapshot.MigrationKeys != 1 || snapshot.MigrationErrors != 0 || snapshot.MigrationTime.Count != 1 {
		t.Fatalf("migration metrics=%+v", snapshot)
	}
	if snapshot.GossipPages != 1 || snapshot.GossipRecords != 1 {
		t.Fatalf("gossip metrics=%+v", snapshot)
	}
	peerSnapshot := peer.MetricsSnapshot()
	if peerSnapshot.GossipMerges != 1 || peerSnapshot.GossipRecords != 1 {
		t.Fatalf("peer gossip metrics=%+v", peerSnapshot)
	}
}
