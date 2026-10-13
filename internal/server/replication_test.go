package server

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func replicationConfig(role config.ReplicationRole) config.Config {
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
	cfg.ReplicationReconnectDelay = 50 * time.Millisecond
	return cfg
}

func waitForCondition(t *testing.T, timeout time.Duration, what string, fn func() bool) {
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

func stringValue(s *Server, key string) (string, bool) {
	obj, ok := s.store.Get(key)
	if !ok {
		return "", false
	}
	value, ok := obj.StringValue()
	return value, ok
}

func TestReplicationFullSyncLivePropagationAndReplicaReadOnly(t *testing.T) {
	primaryCfg := replicationConfig(config.ReplicationPrimary)
	primary, _ := startTestServer(t, primaryCfg)
	waitForCondition(t, time.Second, "primary replication listener", func() bool {
		return primary.ReplicationAddr() != nil
	})

	if reply := primary.executeCommand([]string{"SET", "before", "1"}); reply.IsError() {
		t.Fatalf("primary SET before failed: %#v", reply)
	}

	replicaCfg := replicationConfig(config.ReplicationReplica)
	replicaCfg.ReplicaOf = primary.ReplicationAddr().String()
	replica, _ := startTestServer(t, replicaCfg)

	waitForCondition(t, 2*time.Second, "full sync convergence", func() bool {
		value, ok := stringValue(replica, "before")
		return ok && value == "1"
	})
	if stats := primary.ReplicationStats(); stats.FullSyncs < 1 {
		t.Fatalf("full sync counter=%#v", stats)
	}

	if reply := primary.executeCommand([]string{"SET", "live", "2"}); reply.IsError() {
		t.Fatalf("primary live SET failed: %#v", reply)
	}
	waitForCondition(t, 2*time.Second, "live replication", func() bool {
		value, ok := stringValue(replica, "live")
		return ok && value == "2"
	})

	blocked := replica.executeCommand([]string{"SET", "local", "x"})
	if !blocked.IsError() || !strings.Contains(blocked.Str, "READONLY") {
		t.Fatalf("replica write reply=%#v", blocked)
	}
	if _, ok := stringValue(replica, "local"); ok {
		t.Fatal("rejected replica write changed state")
	}

	_, primaryID, primaryOffset := primary.ReplicationState()
	_, replicaID, replicaOffset := replica.ReplicationState()
	if primaryID == "" || primaryID != replicaID || primaryOffset != replicaOffset {
		t.Fatalf("replication state primary=(%q,%d) replica=(%q,%d)", primaryID, primaryOffset, replicaID, replicaOffset)
	}
}

func TestReplicationPartialResyncAfterDisconnect(t *testing.T) {
	primaryCfg := replicationConfig(config.ReplicationPrimary)
	primary, _ := startTestServer(t, primaryCfg)
	waitForCondition(t, time.Second, "primary replication listener", func() bool { return primary.ReplicationAddr() != nil })

	replicaCfg := replicationConfig(config.ReplicationReplica)
	replicaCfg.ReplicaOf = primary.ReplicationAddr().String()
	replicaCfg.ReplicationReconnectDelay = 150 * time.Millisecond
	replica, _ := startTestServer(t, replicaCfg)
	waitForCondition(t, 2*time.Second, "initial full sync", func() bool {
		return primary.ReplicationStats().FullSyncs >= 1
	})

	_, idBefore, offsetBefore := replica.ReplicationState()
	if idBefore == "" {
		t.Fatal("replica identity missing after full sync")
	}
	if err := replica.DisconnectReplicationLink(); err != nil {
		t.Fatal(err)
	}

	primary.executeCommand([]string{"SET", "gap:1", "a"})
	primary.executeCommand([]string{"SET", "gap:2", "b"})

	waitForCondition(t, 3*time.Second, "partial resync", func() bool {
		return primary.ReplicationStats().PartialSyncs >= 1
	})
	waitForCondition(t, 2*time.Second, "partial catch-up", func() bool {
		v1, ok1 := stringValue(replica, "gap:1")
		v2, ok2 := stringValue(replica, "gap:2")
		return ok1 && ok2 && v1 == "a" && v2 == "b"
	})
	_, idAfter, offsetAfter := replica.ReplicationState()
	if idAfter != idBefore || offsetAfter <= offsetBefore {
		t.Fatalf("replica state before=(%q,%d) after=(%q,%d)", idBefore, offsetBefore, idAfter, offsetAfter)
	}
	if stats := primary.ReplicationStats(); stats.FullSyncs != 1 {
		t.Fatalf("partial reconnect unexpectedly full-synced: %#v", stats)
	}
}

func TestReplicationBacklogWrapFallsBackToFullSync(t *testing.T) {
	primaryCfg := replicationConfig(config.ReplicationPrimary)
	primaryCfg.ReplicationBacklogBytes = 64
	primary, _ := startTestServer(t, primaryCfg)
	waitForCondition(t, time.Second, "primary replication listener", func() bool { return primary.ReplicationAddr() != nil })

	replicaCfg := replicationConfig(config.ReplicationReplica)
	replicaCfg.ReplicaOf = primary.ReplicationAddr().String()
	replicaCfg.ReplicationReconnectDelay = 250 * time.Millisecond
	replica, _ := startTestServer(t, replicaCfg)
	waitForCondition(t, 2*time.Second, "initial full sync", func() bool {
		return primary.ReplicationStats().FullSyncs >= 1
	})

	if err := replica.DisconnectReplicationLink(); err != nil {
		t.Fatal(err)
	}
	large := strings.Repeat("x", 256)
	primary.executeCommand([]string{"SET", "large", large})

	waitForCondition(t, 3*time.Second, "fallback full sync", func() bool {
		return primary.ReplicationStats().FullSyncs >= 2
	})
	waitForCondition(t, 2*time.Second, "full-sync fallback convergence", func() bool {
		value, ok := stringValue(replica, "large")
		return ok && value == large
	})
}

func TestReplicationPreservesTransactionFrame(t *testing.T) {
	primaryCfg := replicationConfig(config.ReplicationPrimary)
	primary, _ := startTestServer(t, primaryCfg)
	waitForCondition(t, time.Second, "primary replication listener", func() bool { return primary.ReplicationAddr() != nil })

	replicaCfg := replicationConfig(config.ReplicationReplica)
	replicaCfg.ReplicaOf = primary.ReplicationAddr().String()
	replica, _ := startTestServer(t, replicaCfg)
	waitForCondition(t, 2*time.Second, "initial full sync", func() bool {
		return primary.ReplicationStats().FullSyncs >= 1
	})

	session := primary.newClientSession(nil)
	defer primary.closeClientSession(session)
	primary.executeClientCommand(session, []string{"MULTI"})
	primary.executeClientCommand(session, []string{"SET", "tx", "1"})
	primary.executeClientCommand(session, []string{"INCR", "tx"})
	replies := primary.executeClientCommand(session, []string{"EXEC"})
	if len(replies) != 1 || replies[0].IsError() {
		t.Fatalf("EXEC reply=%#v", replies)
	}

	waitForCondition(t, 2*time.Second, "transaction replication", func() bool {
		value, ok := stringValue(replica, "tx")
		return ok && value == "2"
	})
	_, _, primaryOffset := primary.ReplicationState()
	_, _, replicaOffset := replica.ReplicationState()
	waitForCondition(t, time.Second, "replication offsets converge", func() bool {
		_, _, replicaOffset = replica.ReplicationState()
		return replicaOffset == primaryOffset
	})
}

// Compile-time reminder that replication tests intentionally stay in-package so
// they can inspect the same logical store boundary used by transaction tests.
var _ = slog.New
var _ = io.Discard
var _ = command.NewRegistry
var _ = store.RealClock{}
var _ = versionedmemory.New
