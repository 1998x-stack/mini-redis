package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/cluster"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

func startClusterWire(t *testing.T, srv *Server, topology *cluster.Topology, localID string) net.Addr {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ServeCluster(ctx, listener, topology, localID) }()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("ServeCluster: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("ServeCluster did not stop")
		}
	})
	return listener.Addr()
}

func TestClusterWireMovedAndAskingOneShot(t *testing.T) {
	a := clusterTestServer(t)
	b := clusterTestServer(t)
	topology := cluster.NewTopology()
	if err := topology.UpsertNode("a", "a-cluster:6379"); err != nil {
		t.Fatal(err)
	}
	if err := topology.UpsertNode("b", "b-cluster:6379"); err != nil {
		t.Fatal(err)
	}
	if err := topology.AssignRange(0, cluster.SlotCount-1, "a"); err != nil {
		t.Fatal(err)
	}
	bAddr := startClusterWire(t, b, topology, "b")

	key := "wire:{42}"
	slot := cluster.HashSlot(key)
	wantMoved := fmt.Sprintf("-MOVED %d a-cluster:6379\r\n", slot)
	roundTrip(t, bAddr, resp.EncodeCommand([]string{"GET", key}), wantMoved)

	if err := topology.BeginMigration(slot, "a", "b"); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", bAddr.String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	payload := append(resp.EncodeCommand([]string{"ASKING"}), resp.EncodeCommand([]string{"SET", key, "v"})...)
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("+OK\r\n+OK\r\n"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if got := string(buf); got != "+OK\r\n+OK\r\n" {
		t.Fatalf("asking/set replies=%q", got)
	}
	if _, err := conn.Write(resp.EncodeCommand([]string{"GET", key})); err != nil {
		t.Fatal(err)
	}
	buf = make([]byte, len(wantMoved))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if got := string(buf); got != wantMoved {
		t.Fatalf("ASKING leaked to next request: got=%q want=%q", got, wantMoved)
	}

	_ = a // source server intentionally need not be network-reached by this wire test.
}

func TestClusterWireRESP3NegotiationDoesNotBypassRouting(t *testing.T) {
	b := clusterTestServer(t)
	topology := cluster.NewTopology()
	if err := topology.UpsertNode("a", "a-cluster:6379"); err != nil {
		t.Fatal(err)
	}
	if err := topology.UpsertNode("b", "b-cluster:6379"); err != nil {
		t.Fatal(err)
	}
	if err := topology.AssignRange(0, cluster.SlotCount-1, "a"); err != nil {
		t.Fatal(err)
	}
	addr := startClusterWire(t, b, topology, "b")
	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	writeCommand(t, conn, "HELLO", "3")
	hello := readProtocolValue(t, conn)
	if hello.Type != resp.MapType {
		t.Fatalf("HELLO 3=%#v", hello)
	}
	mode, ok := mapLookup(hello, "mode")
	if !ok || mode.Str != "cluster" {
		t.Fatalf("mode=%#v ok=%v", mode, ok)
	}

	key := "cluster-resp3:{42}"
	slot := cluster.HashSlot(key)
	writeCommand(t, conn, "GET", key)
	moved := readProtocolValue(t, conn)
	want := fmt.Sprintf("MOVED %d a-cluster:6379", slot)
	if moved.Type != resp.ErrorType || moved.Str != want {
		t.Fatalf("routing bypassed: got=%#v want=%q", moved, want)
	}

	if err := topology.AssignSlot(slot, "b"); err != nil {
		t.Fatal(err)
	}
	writeCommand(t, conn, "HSET", key, "field", "value")
	_ = readProtocolValue(t, conn)
	writeCommand(t, conn, "HGETALL", key)
	if got := readProtocolValue(t, conn); got.Type != resp.MapType || len(got.Map) != 1 {
		t.Fatalf("cluster RESP3 HGETALL=%#v", got)
	}
}
