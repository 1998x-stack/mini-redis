package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/1998x-stack/mini-redis/internal/cluster"
	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type running struct {
	id     string
	srv    *server.Server
	cancel context.CancelFunc
	sess   *cluster.Session
}

func main() {
	a := start("a")
	defer a.close()
	b := start("b")
	defer b.close()
	c := start("c")
	defer c.close()

	topology := cluster.NewTopology()
	must(topology.UpsertNode("a", a.srv.Addr().String()))
	must(topology.UpsertNode("b", b.srv.Addr().String()))
	must(topology.UpsertNode("c", c.srv.Addr().String()))
	must(topology.AssignRange(0, 5460, "a"))
	must(topology.AssignRange(5461, 10922, "b"))
	must(topology.AssignRange(10923, cluster.SlotCount-1, "c"))

	fmt.Println("== v0.10 Cluster / hash-slot routing ==")
	fmt.Printf("slot(user:{42})=%d; hash-tag keeps related keys together\n", cluster.HashSlot("user:{42}"))

	key := "user:{42}:name" // slot 8000 -> b in this demo
	show("a GET before redirect", exec(a, topology, []string{"GET", key}))
	show("b SET owner", exec(b, topology, []string{"SET", key, "alice"}))
	show("b GET owner", exec(b, topology, []string{"GET", key}))
	show("CROSSSLOT DEL", exec(b, topology, []string{"DEL", "foo", "bar"}))

	slot := cluster.HashSlot(key)
	must(topology.BeginMigration(slot, "b", "c"))
	fmt.Printf("\nslot %d migration: b -> c\n", slot)
	show("b existing key", exec(b, topology, []string{"GET", key}))
	missing := "user:{42}:new"
	show("b missing key", exec(b, topology, []string{"GET", missing}))
	show("c without ASKING", exec(c, topology, []string{"GET", missing}))
	show("c ASKING", exec(c, topology, []string{"ASKING"}))
	show("c one-shot SET", exec(c, topology, []string{"SET", missing, "created-on-importing"}))
	show("c next GET grant consumed", exec(c, topology, []string{"GET", missing}))

	must(cluster.MigrateSlot(context.Background(), topology, slot,
		&server.ClusterNode{NodeID: "b", Server: b.srv},
		&server.ClusterNode{NodeID: "c", Server: c.srv}))
	fmt.Println("\nslot ownership committed to c")
	show("b old owner", exec(b, topology, []string{"GET", key}))
	show("c new owner existing", exec(c, topology, []string{"GET", key}))
	show("c new owner imported", exec(c, topology, []string{"GET", missing}))

	observer := cluster.NewTopology()
	for cursor := 0; cursor >= 0; {
		page, err := topology.GossipPage(cursor, 256)
		must(err)
		must(observer.MergePage(page))
		cursor = page.Next
	}
	state, _ := observer.Slot(slot)
	fmt.Printf("\nbounded gossip observer: slot=%d owner=%s topology-epoch=%d\n", slot, state.OwnerID, observer.Epoch())
	fmt.Println("note: v0.10 models routing/migration epochs; it does not claim Redis Cluster consensus or automatic failover")
}

func start(id string) *running {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.NetworkEngine = config.NetworkLegacy
	cfg.AppendOnly = false
	cfg.RDBFilename = ""
	cfg.MaxMemory = 0
	registry := command.NewRegistry()
	command.RegisterCore(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := server.New(cfg, versionedmemory.New(store.RealClock{}), registry, logger)
	must(err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			panic(err)
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if srv.Addr() == nil {
		panic("server did not start: " + id)
	}
	return &running{id: id, srv: srv, cancel: cancel, sess: &cluster.Session{}}
}

func (r *running) close() {
	r.cancel()
	_ = r.srv.Close()
}

func exec(r *running, topology *cluster.Topology, argv []string) resp.Value {
	return r.srv.ClusterExecute(topology, r.id, r.sess, argv)
}

func show(label string, value resp.Value) {
	switch value.Type {
	case resp.ErrorType:
		fmt.Printf("%-28s -> -%s\n", label, value.Str)
	case resp.SimpleStringType, resp.BulkStringType:
		fmt.Printf("%-28s -> %s\n", label, value.Str)
	case resp.NullBulkStringType:
		fmt.Printf("%-28s -> (nil)\n", label)
	case resp.IntegerType:
		fmt.Printf("%-28s -> %d\n", label, value.Int)
	default:
		fmt.Printf("%-28s -> %#v\n", label, value)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
