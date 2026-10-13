package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/sentinel"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type runningNode struct {
	node   *sentinel.ServerNode
	cancel context.CancelFunc
}

type faultMatrix struct {
	mu   sync.RWMutex
	down map[string]map[string]bool
}

func main() {
	primaryCfg := demoConfig(config.ReplicationPrimary)
	p := startNode("p", primaryCfg)
	defer p.close()
	wait("primary replication listener", func() bool { return p.node.Server.ReplicationAddr() != nil })
	mustSet(p.node.Server, "seed", "1")

	r1Cfg := demoConfig(config.ReplicationReplica)
	r1Cfg.ReplicaOf = p.node.Server.ReplicationAddr().String()
	r1 := startNode("r1", r1Cfg)
	defer r1.close()
	r2Cfg := demoConfig(config.ReplicationReplica)
	r2Cfg.ReplicaOf = p.node.Server.ReplicationAddr().String()
	r2 := startNode("r2", r2Cfg)
	defer r2.close()
	wait("replicas converge", func() bool {
		v1, ok1 := get(r1.node.Server, "seed")
		v2, ok2 := get(r2.node.Server, "seed")
		return ok1 && ok2 && v1 == "1" && v2 == "1"
	})

	faults := &faultMatrix{down: make(map[string]map[string]bool)}
	controller, err := sentinel.NewController(sentinel.Config{
		MonitorIDs: []string{"s1", "s2", "s3"},
		Quorum:     2,
		DownAfter:  50 * time.Millisecond,
	}, "p", []sentinel.Node{p.node, r1.node, r2.node}, faults.probe)
	must(err)

	base := time.Unix(1_700_000_000, 0)
	fmt.Println("== v0.9 Sentinel-inspired failover ==")
	printTopology("initial", p.node, r1.node, r2.node)

	// One observer loses the primary. This is only subjective-down.
	faults.set("s1", "p", true)
	_, _ = controller.Tick(context.Background(), base)
	events, err := controller.Tick(context.Background(), base.Add(51*time.Millisecond))
	must(err)
	fmt.Println("\n1) only s1 cannot reach p")
	printEvents(events)
	fmt.Printf("   ODOWN=%v epoch=%d primary=%s\n", controller.IsObjectiveDown("p"), controller.Epoch(), controller.PrimaryID())

	// A quorum now loses the primary. After down-after elapses the controller
	// enters one failover epoch and promotes the freshest deterministic replica.
	faults.set("s2", "p", true)
	faults.set("s3", "p", true)
	_, _ = controller.Tick(context.Background(), base.Add(60*time.Millisecond))
	events, err = controller.Tick(context.Background(), base.Add(111*time.Millisecond))
	must(err)
	fmt.Println("\n2) quorum cannot reach p")
	printEvents(events)
	decision := controller.LastDecision()
	fmt.Printf("   decision: epoch=%d leader=%s old=%s new=%s\n", decision.Epoch, decision.LeaderID, decision.OldPrimaryID, decision.NewPrimaryID)
	printTopology("after promotion", p.node, r1.node, r2.node)

	mustSet(r1.node.Server, "after-failover", "2")
	wait("surviving replica follows new primary", func() bool {
		value, ok := get(r2.node.Server, "after-failover")
		return ok && value == "2"
	})
	fmt.Println("   write on promoted r1 replicated to r2")

	// The old primary becomes reachable again. Sentinel does not merge divergent
	// histories; it reconfigures the returning node to follow the chosen primary.
	faults.set("s1", "p", false)
	faults.set("s2", "p", false)
	faults.set("s3", "p", false)
	events, err = controller.Tick(context.Background(), base.Add(170*time.Millisecond))
	must(err)
	wait("old primary rejoin", func() bool {
		topology := p.node.Server.ReplicationTopology()
		value, ok := get(p.node.Server, "after-failover")
		return topology.Role == config.ReplicationReplica && ok && value == "2"
	})
	fmt.Println("\n3) old p becomes reachable again")
	printEvents(events)
	printTopology("repaired", p.node, r1.node, r2.node)

	mustSet(r1.node.Server, "post-repair", "3")
	wait("all replicas converge", func() bool {
		vp, okp := get(p.node.Server, "post-repair")
		v2, ok2 := get(r2.node.Server, "post-repair")
		return okp && ok2 && vp == "3" && v2 == "3"
	})
	fmt.Println("\n4) post-repair write converged to both replicas")
	fmt.Println("note: this teaching Sentinel does not fence an unreachable old primary; split-brain prevention is not claimed")
}

func demoConfig(role config.ReplicationRole) config.Config {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.NetworkEngine = config.NetworkLegacy
	cfg.AppendOnly = false
	cfg.RDBFilename = ""
	cfg.MaxMemory = 0
	cfg.ReplicationRole = role
	cfg.ReplicationAddr = "127.0.0.1:0"
	cfg.ReplicationBacklogBytes = 1 << 20
	cfg.ReplicationSubscriberFrames = 64
	cfg.ReplicationReconnectDelay = 25 * time.Millisecond
	return cfg
}

func startNode(id string, cfg config.Config) *runningNode {
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
	wait("client listener "+id, func() bool { return srv.Addr() != nil })
	return &runningNode{
		node:   &sentinel.ServerNode{NodeID: id, Server: srv, ProbeTimeout: 200 * time.Millisecond},
		cancel: cancel,
	}
}

func (r *runningNode) close() {
	r.cancel()
	_ = r.node.Server.Close()
}

func (f *faultMatrix) set(monitorID, nodeID string, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[monitorID] == nil {
		f.down[monitorID] = make(map[string]bool)
	}
	f.down[monitorID][nodeID] = down
}

func (f *faultMatrix) probe(ctx context.Context, monitorID string, node sentinel.Node) (sentinel.NodeStatus, error) {
	f.mu.RLock()
	down := f.down[monitorID][node.ID()]
	f.mu.RUnlock()
	if down {
		return sentinel.NodeStatus{}, errors.New("simulated observer partition")
	}
	return node.Probe(ctx)
}

func printEvents(events []sentinel.Event) {
	if len(events) == 0 {
		fmt.Println("   (no state transition)")
		return
	}
	for _, event := range events {
		fmt.Printf("   %-12s monitor=%-2s node=%-2s epoch=%d %s\n", event.Kind, event.MonitorID, event.NodeID, event.Epoch, event.Detail)
	}
}

func printTopology(label string, nodes ...*sentinel.ServerNode) {
	fmt.Printf("   topology[%s]:\n", label)
	for _, node := range nodes {
		status, err := node.Probe(context.Background())
		if err != nil {
			fmt.Printf("     %s probe-error=%v\n", node.ID(), err)
			continue
		}
		fmt.Printf("     %s role=%-7s offset=%-4d upstream=%s\n", status.ID, status.Role, status.Offset, status.Upstream)
	}
}

func wait(name string, fn func() bool) {
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("timeout waiting for " + name)
}

func mustSet(srv *server.Server, key, value string) {
	reply, err := send(srv.Addr().String(), []string{"SET", key, value})
	must(err)
	if reply != "OK" {
		panic("SET reply: " + reply)
	}
}

func get(srv *server.Server, key string) (string, bool) {
	if srv.Addr() == nil {
		return "", false
	}
	value, err := send(srv.Addr().String(), []string{"GET", key})
	return value, err == nil
}

func send(addr string, argv []string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := conn.Write(resp.EncodeCommand(argv)); err != nil {
		return "", err
	}
	reader := bufio.NewReader(conn)
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	readLine := func() (string, error) {
		line, err := reader.ReadString('\n')
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		return line, err
	}
	switch prefix {
	case '+', ':':
		return readLine()
	case '-':
		line, _ := readLine()
		return "", errors.New(line)
	case '$':
		line, err := readLine()
		if err != nil {
			return "", err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "", nil
		}
		payload := make([]byte, n+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return "", err
		}
		return string(payload[:n]), nil
	default:
		return "", fmt.Errorf("unexpected RESP prefix %q", prefix)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
