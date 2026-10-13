package cluster

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/store"
)

func testTopology(t *testing.T) *Topology {
	t.Helper()
	topology := NewTopology()
	for _, node := range []NodeInfo{{ID: "a", Addr: "a:6379"}, {ID: "b", Addr: "b:6379"}, {ID: "c", Addr: "c:6379"}} {
		if err := topology.UpsertNode(node.ID, node.Addr); err != nil {
			t.Fatal(err)
		}
	}
	if err := topology.AssignRange(0, SlotCount-1, "a"); err != nil {
		t.Fatal(err)
	}
	return topology
}

func TestHashSlotAndHashTags(t *testing.T) {
	if got := HashSlot("123456789"); got != 12739 {
		t.Fatalf("slot=%d want=12739", got)
	}
	if got := HashSlot("foo"); got != 12182 {
		t.Fatalf("foo slot=%d want=12182", got)
	}
	if a, b := HashSlot("user:{42}:name"), HashSlot("order:{42}:1"); a != 8000 || b != 8000 {
		t.Fatalf("hash tag slots=%d,%d want=8000,8000", a, b)
	}
	if HashSlot("x:{}:1") == HashSlot("y:{}:2") {
		t.Fatal("empty hash tag must not force unrelated keys into one slot")
	}
}

func TestCommonSlotRejectsCrossSlot(t *testing.T) {
	if slot, err := CommonSlot([]string{"a:{same}", "b:{same}"}); err != nil || slot != HashSlot("{same}") {
		t.Fatalf("slot=%d err=%v", slot, err)
	}
	if _, err := CommonSlot([]string{"foo", "bar"}); !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("err=%v", err)
	}
}

func TestStableMovedAndMigrationAskRouting(t *testing.T) {
	topology := testTopology(t)
	key := "user:{42}"
	slot := HashSlot(key)

	if got := topology.Route("b", key, false, false); got.Action != RouteMoved || got.NodeID != "a" {
		t.Fatalf("stable route=%#v", got)
	}
	if err := topology.BeginMigration(slot, "a", "b"); err != nil {
		t.Fatal(err)
	}
	if got := topology.Route("a", key, true, false); got.Action != RouteExecute {
		t.Fatalf("source existing route=%#v", got)
	}
	if got := topology.Route("a", key, false, false); got.Action != RouteAsk || got.NodeID != "b" {
		t.Fatalf("source missing route=%#v", got)
	}
	if got := topology.Route("b", key, true, false); got.Action != RouteMoved || got.NodeID != "a" {
		t.Fatalf("import target without ASKING=%#v", got)
	}
	if got := topology.Route("b", key, true, true); got.Action != RouteExecute {
		t.Fatalf("import target with ASKING=%#v", got)
	}
	if err := topology.CommitMigration(slot, "a", "b"); err != nil {
		t.Fatal(err)
	}
	if got := topology.Route("a", key, false, false); got.Action != RouteMoved || got.NodeID != "b" {
		t.Fatalf("post-commit source route=%#v", got)
	}
	if got := topology.Route("b", key, true, false); got.Action != RouteExecute {
		t.Fatalf("post-commit target route=%#v", got)
	}
}

func TestGossipPagesAreBoundedAndMergeByEpoch(t *testing.T) {
	source := testTopology(t)
	if err := source.AssignRange(100, 200, "b"); err != nil {
		t.Fatal(err)
	}
	target := NewTopology()
	cursor := 0
	pages := 0
	for cursor >= 0 {
		page, err := source.GossipPage(cursor, 257)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Slots) > 257 {
			t.Fatalf("page size=%d", len(page.Slots))
		}
		if err := target.MergePage(page); err != nil {
			t.Fatal(err)
		}
		cursor = page.Next
		pages++
	}
	if pages <= 1 {
		t.Fatal("expected bounded multi-page gossip")
	}
	for _, slot := range []int{0, 100, 150, 201, SlotCount - 1} {
		want, _ := source.Slot(slot)
		got, _ := target.Slot(slot)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("slot %d got=%#v want=%#v", slot, got, want)
		}
	}
}

type fakeMigrationNode struct {
	id      string
	entries map[string]store.SnapshotEntry
	failKey string
}

func (n *fakeMigrationNode) ID() string { return n.id }
func (n *fakeMigrationNode) KeysInSlot(_ context.Context, slot int) ([]string, error) {
	out := []string{}
	for key := range n.entries {
		if HashSlot(key) == slot {
			out = append(out, key)
		}
	}
	return out, nil
}
func (n *fakeMigrationNode) ImportKey(_ context.Context, entry store.SnapshotEntry) error {
	if entry.Key == n.failKey {
		return errors.New("injected import failure")
	}
	if n.entries == nil {
		n.entries = make(map[string]store.SnapshotEntry)
	}
	n.entries[entry.Key] = entry
	return nil
}
func (n *fakeMigrationNode) TransferKey(ctx context.Context, key string, target MigrationTarget) error {
	entry, ok := n.entries[key]
	if !ok {
		return nil
	}
	if err := target.ImportKey(ctx, entry); err != nil {
		return err
	}
	delete(n.entries, key)
	return nil
}

func TestMigrateSlotDeletesOnlyAfterTargetImportAndCommitsOwner(t *testing.T) {
	topology := testTopology(t)
	key1 := "a:{migrate}"
	key2 := "b:{migrate}"
	slot := HashSlot(key1)
	source := &fakeMigrationNode{id: "a", entries: map[string]store.SnapshotEntry{
		key1: {Key: key1, Type: store.ObjectString, StringValue: "1"},
		key2: {Key: key2, Type: store.ObjectString, StringValue: "2"},
	}}
	target := &fakeMigrationNode{id: "b", entries: map[string]store.SnapshotEntry{}}
	if err := MigrateSlot(context.Background(), topology, slot, source, target); err != nil {
		t.Fatal(err)
	}
	if len(source.entries) != 0 || len(target.entries) != 2 {
		t.Fatalf("source=%v target=%v", source.entries, target.entries)
	}
	state, _ := topology.Slot(slot)
	if state.OwnerID != "b" || state.MigrationTarget != "" {
		t.Fatalf("slot=%#v", state)
	}
}
