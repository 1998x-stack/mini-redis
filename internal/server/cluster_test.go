package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/cluster"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

func clusterTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.NetworkEngine = config.NetworkLegacy
	cfg.AppendOnly = false
	cfg.RDBFilename = ""
	cfg.MaxMemory = 0
	srv, _ := startTestServer(t, cfg)
	return srv
}

func clusterTopologyForServers(t *testing.T, a, b *Server) *cluster.Topology {
	t.Helper()
	topology := cluster.NewTopology()
	if err := topology.UpsertNode("a", a.Addr().String()); err != nil {
		t.Fatal(err)
	}
	if err := topology.UpsertNode("b", b.Addr().String()); err != nil {
		t.Fatal(err)
	}
	if err := topology.AssignRange(0, cluster.SlotCount-1, "a"); err != nil {
		t.Fatal(err)
	}
	return topology
}

func TestClusterExecuteMovedCrossSlotAndKeySlot(t *testing.T) {
	a := clusterTestServer(t)
	b := clusterTestServer(t)
	topology := clusterTopologyForServers(t, a, b)
	sa := &cluster.Session{}
	sb := &cluster.Session{}

	if reply := a.ClusterExecute(topology, "a", sa, []string{"SET", "foo", "1"}); reply.IsError() {
		t.Fatalf("owner SET=%#v", reply)
	}
	moved := b.ClusterExecute(topology, "b", sb, []string{"GET", "foo"})
	if !moved.IsError() || !strings.HasPrefix(moved.Str, "MOVED ") || !strings.Contains(moved.Str, a.Addr().String()) {
		t.Fatalf("wrong-owner reply=%#v", moved)
	}
	cross := a.ClusterExecute(topology, "a", sa, []string{"DEL", "foo", "bar"})
	if !cross.IsError() || !strings.HasPrefix(cross.Str, "CROSSSLOT") {
		t.Fatalf("cross-slot reply=%#v", cross)
	}
	slot := a.ClusterExecute(topology, "a", sa, []string{"CLUSTER", "KEYSLOT", "user:{42}"})
	if slot.Type != resp.IntegerType || slot.Int != 8000 {
		t.Fatalf("keyslot reply=%#v", slot)
	}
}

func TestClusterAskingIsOneRequestGrant(t *testing.T) {
	a := clusterTestServer(t)
	b := clusterTestServer(t)
	topology := clusterTopologyForServers(t, a, b)
	key := "missing:{move}"
	slot := cluster.HashSlot(key)
	if err := topology.BeginMigration(slot, "a", "b"); err != nil {
		t.Fatal(err)
	}
	sa := &cluster.Session{}
	sb := &cluster.Session{}

	ask := a.ClusterExecute(topology, "a", sa, []string{"GET", key})
	if !ask.IsError() || !strings.HasPrefix(ask.Str, "ASK ") || !strings.Contains(ask.Str, b.Addr().String()) {
		t.Fatalf("source missing route=%#v", ask)
	}
	without := b.ClusterExecute(topology, "b", sb, []string{"GET", key})
	if !without.IsError() || !strings.HasPrefix(without.Str, "MOVED ") {
		t.Fatalf("target without ASKING=%#v", without)
	}
	if reply := b.ClusterExecute(topology, "b", sb, []string{"ASKING"}); reply.IsError() {
		t.Fatalf("ASKING=%#v", reply)
	}
	if reply := b.ClusterExecute(topology, "b", sb, []string{"SET", key, "imported"}); reply.IsError() {
		t.Fatalf("ASKING SET=%#v", reply)
	}
	// Grant is consumed by exactly one command; the next request is redirected
	// back to the stable owner until ownership is committed.
	after := b.ClusterExecute(topology, "b", sb, []string{"GET", key})
	if !after.IsError() || !strings.HasPrefix(after.Str, "MOVED ") {
		t.Fatalf("one-shot grant leaked=%#v", after)
	}
}

func TestClusterMigrateSlotMovesLogicalStateAndTTL(t *testing.T) {
	a := clusterTestServer(t)
	b := clusterTestServer(t)
	topology := clusterTopologyForServers(t, a, b)
	key1 := "user:{group}:name"
	key2 := "user:{group}:visits"
	slot := cluster.HashSlot(key1)
	sa := &cluster.Session{}
	sb := &cluster.Session{}

	if reply := a.ClusterExecute(topology, "a", sa, []string{"SET", key1, "alice"}); reply.IsError() {
		t.Fatal(reply.Str)
	}
	if reply := a.ClusterExecute(topology, "a", sa, []string{"SET", key2, "7"}); reply.IsError() {
		t.Fatal(reply.Str)
	}
	if reply := a.ClusterExecute(topology, "a", sa, []string{"EXPIRE", key1, "60"}); reply.IsError() {
		t.Fatal(reply.Str)
	}

	source := &ClusterNode{NodeID: "a", Server: a}
	target := &ClusterNode{NodeID: "b", Server: b}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cluster.MigrateSlot(ctx, topology, slot, source, target); err != nil {
		t.Fatal(err)
	}

	state, _ := topology.Slot(slot)
	if state.OwnerID != "b" || state.MigrationTarget != "" {
		t.Fatalf("slot state=%#v", state)
	}
	if a.store.Exists(key1, key2) != 0 {
		t.Fatal("source retained migrated keys")
	}
	if b.store.Exists(key1, key2) != 2 {
		t.Fatal("target missing migrated keys")
	}
	ttl := b.store.TTL(key1)
	if !ttl.Exists || !ttl.HasExpiry || ttl.Remaining <= 0 || ttl.Remaining > 60*time.Second {
		t.Fatalf("target TTL=%#v", ttl)
	}

	oldOwner := a.ClusterExecute(topology, "a", sa, []string{"GET", key1})
	if !oldOwner.IsError() || !strings.HasPrefix(oldOwner.Str, "MOVED ") {
		t.Fatalf("old owner route=%#v", oldOwner)
	}
	newOwner := b.ClusterExecute(topology, "b", sb, []string{"GET", key1})
	if newOwner.Type != resp.BulkStringType || newOwner.Str != "alice" {
		t.Fatalf("new owner GET=%#v", newOwner)
	}
}
