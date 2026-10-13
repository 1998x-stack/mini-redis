package sentinel

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type runningNode struct {
	node   *ServerNode
	cancel context.CancelFunc
	errCh  chan error
}

func sentinelServerConfig(role config.ReplicationRole) config.Config {
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

func startSentinelNode(t *testing.T, id string, cfg config.Config) *runningNode {
	t.Helper()
	registry := command.NewRegistry()
	command.RegisterCore(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := server.New(cfg, versionedmemory.New(store.RealClock{}), registry, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	waitSentinel(t, 2*time.Second, "client listener", func() bool { return srv.Addr() != nil })

	running := &runningNode{
		node:   &ServerNode{NodeID: id, Server: srv, ProbeTimeout: 200 * time.Millisecond},
		cancel: cancel,
		errCh:  errCh,
	}
	t.Cleanup(func() {
		cancel()
		_ = srv.Close()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("ListenAndServe(%s): %v", id, err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("server %s did not stop", id)
		}
	})
	return running
}

func waitSentinel(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func sendTestCommand(addr string, argv []string) (string, error) {
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
		if payload[n] != '\r' || payload[n+1] != '\n' {
			return "", errors.New("malformed bulk reply")
		}
		return string(payload[:n]), nil
	default:
		return "", fmt.Errorf("unexpected RESP prefix %q", prefix)
	}
}

func directSet(t *testing.T, srv *server.Server, key, value string) {
	t.Helper()
	got, err := sendTestCommand(srv.Addr().String(), []string{"SET", key, value})
	if err != nil {
		t.Fatal(err)
	}
	if got != "OK" {
		t.Fatalf("SET reply=%q", got)
	}
}

func tryGet(srv *server.Server, key string) (string, bool) {
	if srv.Addr() == nil {
		return "", false
	}
	value, err := sendTestCommand(srv.Addr().String(), []string{"GET", key})
	return value, err == nil
}

func TestSentinelFailoverReusesReplicationAndRepairsOldPrimary(t *testing.T) {
	primaryCfg := sentinelServerConfig(config.ReplicationPrimary)
	p := startSentinelNode(t, "p", primaryCfg)
	waitSentinel(t, time.Second, "primary replication listener", func() bool {
		return p.node.Server.ReplicationAddr() != nil
	})
	directSet(t, p.node.Server, "seed", "1")

	r1Cfg := sentinelServerConfig(config.ReplicationReplica)
	r1Cfg.ReplicaOf = p.node.Server.ReplicationAddr().String()
	r1 := startSentinelNode(t, "r1", r1Cfg)
	r2Cfg := sentinelServerConfig(config.ReplicationReplica)
	r2Cfg.ReplicaOf = p.node.Server.ReplicationAddr().String()
	r2 := startSentinelNode(t, "r2", r2Cfg)

	waitSentinel(t, 3*time.Second, "initial replica convergence", func() bool {
		v1, ok1 := tryGet(r1.node.Server, "seed")
		v2, ok2 := tryGet(r2.node.Server, "seed")
		return ok1 && ok2 && v1 == "1" && v2 == "1"
	})
	waitSentinel(t, 2*time.Second, "replication offsets converge", func() bool {
		_, _, pOffset := p.node.Server.ReplicationState()
		_, _, r1Offset := r1.node.Server.ReplicationState()
		_, _, r2Offset := r2.node.Server.ReplicationState()
		return pOffset == r1Offset && pOffset == r2Offset
	})

	var isolateOldPrimary atomic.Bool
	isolateOldPrimary.Store(true)
	probe := func(ctx context.Context, _ string, node Node) (NodeStatus, error) {
		if node.ID() == "p" && isolateOldPrimary.Load() {
			return NodeStatus{}, errors.New("simulated primary partition")
		}
		return node.Probe(ctx)
	}
	controller, err := NewController(Config{
		MonitorIDs: []string{"s1", "s2", "s3"},
		Quorum:     2,
		DownAfter:  20 * time.Millisecond,
	}, "p", []Node{p.node, r1.node, r2.node}, probe)
	if err != nil {
		t.Fatal(err)
	}

	t0 := time.Unix(1_700_000_000, 0)
	if _, err := controller.Tick(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Tick(context.Background(), t0.Add(21*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if controller.PrimaryID() != "r1" || controller.Epoch() != 1 {
		t.Fatalf("primary=%s epoch=%d decision=%#v", controller.PrimaryID(), controller.Epoch(), controller.LastDecision())
	}
	waitSentinel(t, time.Second, "r1 promotion", func() bool {
		return r1.node.Server.ReplicationTopology().Role == config.ReplicationPrimary && r1.node.Server.ReplicationAddr() != nil
	})
	newUpstream := r1.node.Server.ReplicationAddr().String()
	waitSentinel(t, 3*time.Second, "r2 follows promoted primary", func() bool {
		topology := r2.node.Server.ReplicationTopology()
		return topology.Role == config.ReplicationReplica && topology.Upstream == newUpstream
	})

	directSet(t, r1.node.Server, "after-failover", "2")
	waitSentinel(t, 3*time.Second, "surviving replica converges", func() bool {
		value, ok := tryGet(r2.node.Server, "after-failover")
		return ok && value == "2"
	})

	isolateOldPrimary.Store(false)
	if _, err := controller.Tick(context.Background(), t0.Add(42*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	waitSentinel(t, 3*time.Second, "old primary rejoins new topology", func() bool {
		topology := p.node.Server.ReplicationTopology()
		value, ok := tryGet(p.node.Server, "after-failover")
		return topology.Role == config.ReplicationReplica && topology.Upstream == newUpstream && ok && value == "2"
	})

	directSet(t, r1.node.Server, "post-repair", "3")
	waitSentinel(t, 3*time.Second, "all replicas converge after repair", func() bool {
		vp, okp := tryGet(p.node.Server, "post-repair")
		v2, ok2 := tryGet(r2.node.Server, "post-repair")
		return okp && ok2 && vp == "3" && v2 == "3"
	})
}
