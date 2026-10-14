package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type runningServer struct {
	srv    *server.Server
	cancel context.CancelFunc
}

type manualReplica struct {
	conn      net.Conn
	reader    *bufio.Reader
	replID    string
	replicaID string
	offset    int64
}

func main() {
	primary := startPrimary()
	defer primary.close()
	wait("primary replication listener", func() bool { return primary.srv.ReplicationAddr() != nil })

	fmt.Println("== mini-redis v0.13 replication ACK / authoritative lag ==")

	replica := fullSync(primary.srv)
	printProgress("FULLRESYNC installed + ACK", primary.srv)

	mustClientCommand(primary.srv.Addr().String(), "SET", "lag-demo", "value")
	wait("positive lag before ACK", func() bool {
		progress, ok := oneReplica(primary.srv)
		return ok && progress.LagBytes > 0
	})
	printProgress("primary advanced, replica has not ACKed", primary.srv)

	payload := replica.readFrame()
	replica.offset += int64(len(payload))
	replica.ack()
	wait("lag returns to zero", func() bool {
		progress, ok := oneReplica(primary.srv)
		return ok && progress.LagBytes == 0 && progress.AcknowledgedOffset == replica.offset
	})
	printProgress("frame applied + ACK", primary.srv)

	oldReplicaID := replica.replicaID
	replID := replica.replID
	resumeOffset := replica.offset
	replica.close()
	wait("live progress removed on disconnect", func() bool {
		return len(primary.srv.MetricsSnapshot().Replication.Replicas) == 0
	})
	fmt.Println("disconnect: live replica progress removed")

	reconnected := partialSync(primary.srv, replID, resumeOffset)
	defer reconnected.close()
	if reconnected.replicaID == oldReplicaID {
		panic("reconnect unexpectedly reused connection-local replica id")
	}
	reconnected.ack()
	wait("reconnected ACK", func() bool {
		progress, ok := oneReplica(primary.srv)
		return ok && progress.ReplicaID == reconnected.replicaID && progress.LagBytes == 0
	})
	printProgress("CONTINUE + fresh connection-local replica id", primary.srv)

	snapshot := primary.srv.MetricsSnapshot()
	fmt.Printf("ACK counters: received=%d rejected=%d\n",
		snapshot.Runtime.ReplicationAcksReceived,
		snapshot.Runtime.ReplicationAcksRejected,
	)
	fmt.Println("mental model: primary offset is sent work; replica ACK is applied progress; their byte difference is authoritative protocol lag")
}

func startPrimary() *runningServer {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.NetworkEngine = config.NetworkLegacy
	cfg.AppendOnly = false
	cfg.RDBFilename = ""
	cfg.MaxMemory = 0
	cfg.ReplicationRole = config.ReplicationPrimary
	cfg.ReplicationAddr = "127.0.0.1:0"
	cfg.ReplicationBacklogBytes = 1 << 20
	cfg.ReplicationSubscriberFrames = 64

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
	wait("client listener", func() bool { return srv.Addr() != nil })
	return &runningServer{srv: srv, cancel: cancel}
}

func (r *runningServer) close() {
	r.cancel()
	_ = r.srv.Close()
}

func fullSync(primary *server.Server) *manualReplica {
	conn := dial(primary.ReplicationAddr().String())
	writeCommand(conn, "PSYNC", "?", "-1")
	reader := bufio.NewReader(conn)
	header := readSimpleString(reader)
	parts := strings.Fields(header)
	if len(parts) != 4 || parts[0] != "FULLRESYNC" {
		panic("unexpected FULLRESYNC header: " + header)
	}
	offset, err := strconv.ParseInt(parts[2], 10, 64)
	must(err)
	_ = readBulk(reader) // logical snapshot; this teaching replica only demonstrates ACK timing.

	replica := &manualReplica{
		conn:      conn,
		reader:    reader,
		replID:    parts[1],
		replicaID: parts[3],
		offset:    offset,
	}
	replica.ack()
	wait("initial ACK visible", func() bool {
		progress, ok := oneReplica(primary)
		return ok && progress.ReplicaID == replica.replicaID && progress.LagBytes == 0
	})
	return replica
}

func partialSync(primary *server.Server, replID string, offset int64) *manualReplica {
	conn := dial(primary.ReplicationAddr().String())
	writeCommand(conn, "PSYNC", replID, strconv.FormatInt(offset, 10))
	reader := bufio.NewReader(conn)
	header := readSimpleString(reader)
	parts := strings.Fields(header)
	if len(parts) != 4 || parts[0] != "CONTINUE" {
		panic("unexpected CONTINUE header: " + header)
	}
	if parts[1] != replID {
		panic("CONTINUE replication identity changed")
	}
	return &manualReplica{
		conn:      conn,
		reader:    reader,
		replID:    parts[1],
		replicaID: parts[3],
		offset:    offset,
	}
}

func (r *manualReplica) readFrame() []byte {
	return readBulk(r.reader)
}

func (r *manualReplica) ack() {
	writeCommand(r.conn, "REPLCONF", "ACK", r.replicaID, strconv.FormatInt(r.offset, 10))
}

func (r *manualReplica) close() {
	_ = r.conn.Close()
}

func oneReplica(primary *server.Server) (server.ReplicaProgressMetricsSnapshot, bool) {
	replicas := primary.MetricsSnapshot().Replication.Replicas
	if len(replicas) != 1 {
		return server.ReplicaProgressMetricsSnapshot{}, false
	}
	return replicas[0], true
}

func printProgress(label string, primary *server.Server) {
	snapshot := primary.MetricsSnapshot()
	progress, ok := oneReplica(primary)
	if !ok {
		panic("expected exactly one live replica")
	}
	fmt.Printf("%s:\n", label)
	fmt.Printf("  primary_offset=%d\n", snapshot.Replication.Offset)
	fmt.Printf("  replica_id=%s sync=%s ack_offset=%d lag_bytes=%d ack_age_ms=%d\n",
		progress.ReplicaID,
		progress.SyncMode,
		progress.AcknowledgedOffset,
		progress.LagBytes,
		progress.AckAgeMillis,
	)
}

func mustClientCommand(addr string, argv ...string) {
	conn := dial(addr)
	defer conn.Close()
	writeCommand(conn, argv...)
	reader := bufio.NewReader(conn)
	prefix, err := reader.ReadByte()
	must(err)
	line, err := reader.ReadString('\n')
	must(err)
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if prefix == '-' {
		panic(line)
	}
}

func dial(addr string) net.Conn {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	must(err)
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	return conn
}

func writeCommand(writer io.Writer, argv ...string) {
	payload := resp.EncodeCommand(argv)
	for len(payload) > 0 {
		n, err := writer.Write(payload)
		must(err)
		if n <= 0 {
			panic(io.ErrShortWrite)
		}
		payload = payload[n:]
	}
}

func readSimpleString(reader *bufio.Reader) string {
	prefix, err := reader.ReadByte()
	must(err)
	line, err := reader.ReadString('\n')
	must(err)
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if prefix == '-' {
		panic(line)
	}
	if prefix != '+' {
		panic(fmt.Sprintf("expected simple string, got %q", prefix))
	}
	return line
}

func readBulk(reader *bufio.Reader) []byte {
	prefix, err := reader.ReadByte()
	must(err)
	if prefix != '$' {
		panic(fmt.Sprintf("expected bulk string, got %q", prefix))
	}
	line, err := reader.ReadString('\n')
	must(err)
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	n, err := strconv.Atoi(line)
	must(err)
	if n < 0 {
		panic("unexpected null bulk")
	}
	payload := make([]byte, n)
	_, err = io.ReadFull(reader, payload)
	must(err)
	var crlf [2]byte
	_, err = io.ReadFull(reader, crlf[:])
	must(err)
	if crlf != [2]byte{'\r', '\n'} {
		panic("bulk payload missing CRLF")
	}
	return payload
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

func must(err error) {
	if err != nil {
		panic(err)
	}
}
