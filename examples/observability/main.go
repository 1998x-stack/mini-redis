package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/1998x-stack/mini-redis/internal/cluster"
	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/sentinel"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type wireReader struct {
	conn net.Conn
	buf  []byte
}

func (r *wireReader) next() resp.Value {
	for {
		value, consumed, complete, err := resp.ParseValue(r.buf)
		must(err)
		if complete {
			r.buf = append(r.buf[:0], r.buf[consumed:]...)
			return value
		}
		var chunk [4096]byte
		_ = r.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := r.conn.Read(chunk[:])
		must(err)
		r.buf = append(r.buf, chunk[:n]...)
	}
}

type demoNode struct {
	id     string
	status sentinel.NodeStatus
}

func (n *demoNode) ID() string { return n.id }
func (n *demoNode) Probe(context.Context) (sentinel.NodeStatus, error) {
	return n.status, nil
}
func (n *demoNode) Promote(context.Context) error {
	n.status.Role = sentinel.RolePrimary
	n.status.Upstream = ""
	if n.status.ReplicationAddr == "" {
		n.status.ReplicationAddr = n.id + ":6380"
	}
	return nil
}
func (n *demoNode) Follow(_ context.Context, upstream string) error {
	n.status.Role = sentinel.RoleReplica
	n.status.Upstream = upstream
	return nil
}

func main() {
	fmt.Println("== mini-redis v0.12 observability ==")
	serverDemo()
	clusterDemo()
	sentinelDemo()
	fmt.Println("mental model: metrics observe mechanisms; mechanisms never consult metrics")
}

func serverDemo() {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.NetworkEngine = config.NetworkLegacy
	cfg.AppendOnly = false
	cfg.RDBFilename = ""

	registry := command.NewRegistry()
	command.RegisterCore(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := server.New(cfg, versionedmemory.New(store.RealClock{}), registry, logger)
	must(err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.ListenAndServe(ctx) }()
	for srv.Addr() == nil {
		time.Sleep(time.Millisecond)
	}
	defer srv.Close()

	conn, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
	must(err)
	defer conn.Close()
	reader := &wireReader{conn: conn}
	for _, argv := range [][]string{
		{"SET", "observed", "1"},
		{"PING"},
		{"HELLO", "3"},
		{"INFO", "commands"},
	} {
		_, err := conn.Write(resp.EncodeCommand(argv))
		must(err)
		if reply := reader.next(); reply.IsError() {
			panic(reply.Str)
		}
	}

	snapshot := srv.MetricsSnapshot()
	fmt.Printf("server: SET=%d PING=%d HELLO=%d INFO=%d latency_samples=%d\n",
		snapshot.Runtime.Commands["SET"], snapshot.Runtime.Commands["PING"],
		snapshot.Runtime.Commands["HELLO"], snapshot.Runtime.Commands["INFO"],
		snapshot.Runtime.CommandLatency.Count)
	fmt.Printf("protocol: resp2_requests=%d resp3_requests=%d resp3_replies=%d\n",
		snapshot.Runtime.ProtocolRequests["resp2"], snapshot.Runtime.ProtocolRequests["resp3"],
		snapshot.Runtime.ProtocolReplies["resp3"])
	fmt.Printf("network: legacy_accepted=%d active=%d bytes_read_nonzero=%t bytes_written_nonzero=%t\n",
		snapshot.Runtime.Connections["legacy"], snapshot.Runtime.ActiveConnections["legacy"],
		snapshot.Runtime.BytesRead["legacy"] > 0, snapshot.Runtime.BytesWritten["legacy"] > 0)
}

func clusterDemo() {
	topology := cluster.NewTopology()
	must(topology.UpsertNode("a", "a:6379"))
	must(topology.UpsertNode("b", "b:6379"))
	key := "observed:{42}"
	slot := cluster.HashSlot(key)
	must(topology.AssignSlot(slot, "a"))
	_ = topology.Route("b", key, false, false) // MOVED
	must(topology.BeginMigration(slot, "a", "b"))
	_ = topology.Route("a", key, false, false) // ASK
	topology.RecordCrossSlot()
	_, err := topology.GossipPage(slot, 1)
	must(err)

	snapshot := topology.MetricsSnapshot()
	fmt.Printf("cluster: MOVED=%d ASK=%d CROSSSLOT=%d gossip_pages=%d routes=%d\n",
		snapshot.Moved, snapshot.Ask, snapshot.CrossSlot, snapshot.GossipPages, snapshot.Routes)
}

func sentinelDemo() {
	primary := &demoNode{id: "p", status: sentinel.NodeStatus{
		ID: "p", Role: sentinel.RolePrimary, ClientAddr: "p:6379", ReplicationAddr: "p:6380", Offset: 20,
	}}
	replica := &demoNode{id: "r", status: sentinel.NodeStatus{
		ID: "r", Role: sentinel.RoleReplica, ClientAddr: "r:6379", ReplicationAddr: "r:6380", Upstream: "p:6380", Offset: 18,
	}}
	probe := func(ctx context.Context, _ string, node sentinel.Node) (sentinel.NodeStatus, error) {
		if node.ID() == "p" {
			return sentinel.NodeStatus{}, errors.New("primary unreachable")
		}
		return node.Probe(ctx)
	}
	controller, err := sentinel.NewController(sentinel.Config{
		MonitorIDs: []string{"s1"},
		Quorum:     1,
		DownAfter:  time.Millisecond,
	}, "p", []sentinel.Node{primary, replica}, probe)
	must(err)
	base := time.Unix(100, 0)
	_, err = controller.Tick(context.Background(), base)
	must(err)
	_, err = controller.Tick(context.Background(), base.Add(2*time.Millisecond))
	must(err)

	snapshot := controller.MetricsSnapshot()
	fmt.Printf("sentinel: ticks=%d probes=%d probe_failures=%d failovers=%d promotions=%d primary=%s\n",
		snapshot.Ticks, snapshot.Probes, snapshot.ProbeFailures,
		snapshot.FailoverAttempts, snapshot.Promotions, snapshot.PrimaryID)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
