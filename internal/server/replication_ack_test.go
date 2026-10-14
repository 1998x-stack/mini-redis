package server

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/observability"
	"github.com/1998x-stack/mini-redis/internal/replication"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

func TestReplicationACKTracksRealReplicaProgress(t *testing.T) {
	primaryCfg := replicationConfig(config.ReplicationPrimary)
	primary, _ := startTestServer(t, primaryCfg)
	waitForCondition(t, time.Second, "primary replication listener", func() bool { return primary.ReplicationAddr() != nil })

	replicaCfg := replicationConfig(config.ReplicationReplica)
	replicaCfg.ReplicaOf = primary.ReplicationAddr().String()
	replicaCfg.ReplicationReconnectDelay = 500 * time.Millisecond
	replica, _ := startTestServer(t, replicaCfg)

	waitForCondition(t, 2*time.Second, "initial replica ACK", func() bool {
		snapshot := primary.MetricsSnapshot()
		return len(snapshot.Replication.Replicas) == 1 &&
			snapshot.Replication.Replicas[0].Acknowledged &&
			snapshot.Replication.Replicas[0].LagBytes == 0
	})

	if reply := primary.executeCommand([]string{"SET", "ack:live", "1"}); reply.IsError() {
		t.Fatalf("primary SET failed: %#v", reply)
	}
	waitForCondition(t, 2*time.Second, "replica applies and ACKs live frame", func() bool {
		snapshot := primary.MetricsSnapshot()
		if len(snapshot.Replication.Replicas) != 1 {
			return false
		}
		progress := snapshot.Replication.Replicas[0]
		return progress.LagBytes == 0 && progress.AcknowledgedOffset == snapshot.Replication.Offset
	})
	if primary.MetricsSnapshot().Runtime.ReplicationAcksReceived < 2 {
		t.Fatalf("ACK counter=%d want at least snapshot+live ACK", primary.MetricsSnapshot().Runtime.ReplicationAcksReceived)
	}

	if err := replica.DisconnectReplicationLink(); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 300*time.Millisecond, "disconnect removes live progress", func() bool {
		return len(primary.MetricsSnapshot().Replication.Replicas) == 0
	})
}

func TestManualReplicaMakesLagVisibleUntilACK(t *testing.T) {
	primaryCfg := replicationConfig(config.ReplicationPrimary)
	primary, _ := startTestServer(t, primaryCfg)
	waitForCondition(t, time.Second, "primary replication listener", func() bool { return primary.ReplicationAddr() != nil })

	conn, err := net.DialTimeout("tcp", primary.ReplicationAddr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeReplicationCommand(conn, []string{"PSYNC", "?", "-1"}); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	header, err := readReplicationSimpleString(reader)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Fields(header)
	if len(parts) != 4 || parts[0] != "FULLRESYNC" {
		t.Fatalf("header=%q", header)
	}
	primaryReplID := parts[1]
	baseOffset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	replicaID := parts[3]
	if _, err := readReplicationBulk(reader); err != nil {
		t.Fatal(err)
	}
	if err := writeReplicationACK(conn, replicaID, baseOffset); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, time.Second, "manual replica initial ACK", func() bool {
		snapshot := primary.MetricsSnapshot()
		return len(snapshot.Replication.Replicas) == 1 && snapshot.Replication.Replicas[0].LagBytes == 0
	})

	beforeOffset := primary.MetricsSnapshot().Replication.Offset
	if reply := primary.executeCommand([]string{"SET", "manual:lag", "value"}); reply.IsError() {
		t.Fatalf("SET failed: %#v", reply)
	}
	waitForCondition(t, time.Second, "positive authoritative lag", func() bool {
		snapshot := primary.MetricsSnapshot()
		return len(snapshot.Replication.Replicas) == 1 && snapshot.Replication.Replicas[0].LagBytes > 0
	})

	payload, err := readReplicationBulk(reader)
	if err != nil {
		t.Fatal(err)
	}
	commands, err := decodeReplicationCommands(payload)
	if err != nil || len(commands) != 1 || strings.ToUpper(commands[0][0]) != "SET" {
		t.Fatalf("payload commands=%#v err=%v", commands, err)
	}
	ackOffset := baseOffset + int64(len(payload))
	if err := writeReplicationACK(conn, replicaID, ackOffset); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, time.Second, "manual ACK catches up", func() bool {
		snapshot := primary.MetricsSnapshot()
		return len(snapshot.Replication.Replicas) == 1 &&
			snapshot.Replication.Replicas[0].LagBytes == 0 &&
			snapshot.Replication.Replicas[0].AcknowledgedOffset == snapshot.Replication.Offset
	})
	if after := primary.MetricsSnapshot().Replication.Offset; after != ackOffset || after <= beforeOffset {
		t.Fatalf("primary offset after ACK=%d want=%d before=%d", after, ackOffset, beforeOffset)
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, time.Second, "manual disconnect clears live progress", func() bool {
		return len(primary.MetricsSnapshot().Replication.Replicas) == 0
	})

	reconnected, err := net.DialTimeout("tcp", primary.ReplicationAddr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reconnected.Close()
	if err := writeReplicationCommand(reconnected, []string{"PSYNC", primaryReplID, strconv.FormatInt(ackOffset, 10)}); err != nil {
		t.Fatal(err)
	}
	reconnectedReader := bufio.NewReader(reconnected)
	continueHeader, err := readReplicationSimpleString(reconnectedReader)
	if err != nil {
		t.Fatal(err)
	}
	continueParts := strings.Fields(continueHeader)
	if len(continueParts) != 4 || continueParts[0] != "CONTINUE" {
		t.Fatalf("continue header=%q", continueHeader)
	}
	if continueParts[1] != primaryReplID {
		t.Fatalf("CONTINUE repl-id=%q want=%q", continueParts[1], primaryReplID)
	}
	newReplicaID := continueParts[3]
	if newReplicaID == replicaID {
		t.Fatalf("reconnect reused connection-local replica id %q", replicaID)
	}
	if err := writeReplicationACK(reconnected, newReplicaID, ackOffset); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, time.Second, "reconnected ACK establishes new live progress", func() bool {
		snapshot := primary.MetricsSnapshot()
		return len(snapshot.Replication.Replicas) == 1 &&
			snapshot.Replication.Replicas[0].ReplicaID == newReplicaID &&
			snapshot.Replication.Replicas[0].LagBytes == 0
	})
}

func TestReplicationACKReaderRejectsWrongConnectionIdentity(t *testing.T) {
	primary, err := replication.NewPrimaryWithID("primary-A", 1024, 8)
	if err != nil {
		t.Fatal(err)
	}
	primary.Append([]byte("payload"))
	_, _, sub := primary.SubscribeAtCurrent()
	defer primary.Unsubscribe(sub)

	server := &Server{metrics: observability.NewMetrics()}
	wire := resp.EncodeCommand([]string{"REPLCONF", "ACK", "other-replica", strconv.FormatInt(primary.Offset(), 10)})
	err = server.readReplicaAcks(resp.NewReader(bytes.NewReader(wire)), primary, sub.ReplicaID)
	if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("readReplicaAcks error=%v", err)
	}
	if got := server.metrics.Snapshot(time.Now()).ReplicationAcksRejected; got != 1 {
		t.Fatalf("rejected ACK metric=%d want=1", got)
	}
	progress := primary.ReplicaProgressSnapshot(time.Now())
	if len(progress) != 1 || progress[0].Acknowledged {
		t.Fatalf("wrong-id ACK changed progress: %#v", progress)
	}
}
