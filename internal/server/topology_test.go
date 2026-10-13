package server

import (
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/config"
)

func TestRuntimePromotionAndOldPrimaryRejoin(t *testing.T) {
	primaryCfg := replicationConfig(config.ReplicationPrimary)
	oldPrimary, _ := startTestServer(t, primaryCfg)
	waitForCondition(t, time.Second, "old primary replication listener", func() bool {
		return oldPrimary.ReplicationAddr() != nil
	})
	if reply := oldPrimary.executeCommand([]string{"SET", "seed", "1"}); reply.IsError() {
		t.Fatalf("seed write failed: %#v", reply)
	}

	replicaCfg := replicationConfig(config.ReplicationReplica)
	replicaCfg.ReplicaOf = oldPrimary.ReplicationAddr().String()
	promoted, _ := startTestServer(t, replicaCfg)
	waitForCondition(t, 2*time.Second, "initial replica convergence", func() bool {
		value, ok := replicatedString(promoted, "seed")
		return ok && value == "1"
	})
	_, oldReplicationID, _ := promoted.ReplicationState()
	if oldReplicationID == "" {
		t.Fatal("replica should know old primary replication id")
	}

	if err := promoted.PromoteToPrimary(); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, time.Second, "promoted replication listener", func() bool {
		return promoted.ReplicationAddr() != nil
	})
	role, newReplicationID, newOffset := promoted.ReplicationState()
	if role != config.ReplicationPrimary || newReplicationID == "" || newReplicationID == oldReplicationID || newOffset != 0 {
		t.Fatalf("promoted state role=%s id=%q old=%q offset=%d", role, newReplicationID, oldReplicationID, newOffset)
	}
	if reply := promoted.executeCommand([]string{"SET", "after-promotion", "2"}); reply.IsError() {
		t.Fatalf("promoted primary rejected write: %#v", reply)
	}

	newUpstream := promoted.ReplicationAddr().String()
	if err := oldPrimary.FollowPrimary(newUpstream); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 2*time.Second, "old primary rejoin", func() bool {
		topology := oldPrimary.ReplicationTopology()
		value, ok := replicatedString(oldPrimary, "after-promotion")
		return topology.Role == config.ReplicationReplica && topology.Upstream == newUpstream && ok && value == "2"
	})
	blocked := oldPrimary.executeCommand([]string{"SET", "old-primary-write", "blocked"})
	if !blocked.IsError() {
		t.Fatalf("rejoined old primary remained writable: %#v", blocked)
	}

	if reply := promoted.executeCommand([]string{"SET", "post-repair", "3"}); reply.IsError() {
		t.Fatalf("new primary write failed: %#v", reply)
	}
	waitForCondition(t, 2*time.Second, "post-repair replication", func() bool {
		value, ok := replicatedString(oldPrimary, "post-repair")
		return ok && value == "3"
	})
}
