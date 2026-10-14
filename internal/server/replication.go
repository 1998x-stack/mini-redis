package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/replication"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
)

func (s *Server) startReplication(ctx context.Context) error {
	s.replControlMu.Lock()
	s.replRootCtx = ctx
	role := s.replRole
	upstream := s.replUpstream
	s.replControlMu.Unlock()

	switch role {
	case config.ReplicationStandalone:
		return nil
	case config.ReplicationPrimary:
		return s.activatePrimary(ctx)
	case config.ReplicationReplica:
		return s.activateReplica(ctx, upstream)
	default:
		return fmt.Errorf("unsupported replication role %q", role)
	}
}

func (s *Server) ReplicationAddr() net.Addr {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.replAddr
}

func (s *Server) ReplicationState() (role config.ReplicationRole, id string, offset int64) {
	role = s.currentReplicationRole()
	if role == config.ReplicationPrimary {
		if primary := s.currentReplicationPrimary(); primary != nil {
			return role, primary.ID(), primary.Offset()
		}
	}
	s.replStateMu.Lock()
	defer s.replStateMu.Unlock()
	return role, s.replicaID, s.replicaOffset
}

func (s *Server) ReplicationStats() replication.Stats {
	primary := s.currentReplicationPrimary()
	if primary == nil || s.currentReplicationRole() != config.ReplicationPrimary {
		return replication.Stats{}
	}
	return primary.Stats()
}

// DisconnectReplicationLink is an educational diagnostic hook used by the
// deterministic demo/tests to force the reconnect path without stopping the
// replica process (and therefore without losing its in-memory repl-id/offset).
func (s *Server) DisconnectReplicationLink() error {
	s.stateMu.RLock()
	conn := s.replicaConn
	s.stateMu.RUnlock()
	if conn == nil {
		return errors.New("replication link is not connected")
	}
	return conn.Close()
}

func (s *Server) serveReplicationPrimary(ctx context.Context, listener net.Listener, primary *replication.Primary) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Warn("replication accept failed", "error", err)
			continue
		}
		go s.handleReplicaConnection(ctx, conn, primary)
	}
}

func (s *Server) handleReplicaConnection(ctx context.Context, conn net.Conn, primary *replication.Primary) {
	defer conn.Close()
	closed := make(chan struct{})
	defer close(closed)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-closed:
		}
	}()

	reader := resp.NewReader(conn)
	argv, err := reader.ReadCommand()
	if err != nil {
		return
	}
	if len(argv) != 3 || strings.ToUpper(argv[0]) != "PSYNC" {
		_ = resp.NewWriter(conn).WriteValue(resp.Error("ERR expected PSYNC <repl-id> <offset>"))
		return
	}
	requestedOffset, err := strconv.ParseInt(argv[2], 10, 64)
	if err != nil {
		_ = resp.NewWriter(conn).WriteValue(resp.Error("ERR invalid replication offset"))
		return
	}

	writer := resp.NewWriter(conn)
	remoteAddr := ""
	if conn.RemoteAddr() != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	if frames, current, sub, ok := primary.SubscribeFrom(argv[1], requestedOffset); ok {
		defer primary.Unsubscribe(sub)
		if !primary.SetReplicaInfo(sub, remoteAddr, "partial") {
			return
		}
		ackErr := s.startReplicaAckReader(conn, reader, primary, sub.ReplicaID)
		primary.RecordPartialSync()
		if err := writer.WriteValue(resp.SimpleString(fmt.Sprintf("CONTINUE %s %d %s", primary.ID(), current, sub.ReplicaID))); err != nil {
			return
		}
		for _, frame := range frames {
			if err := writer.WriteValue(resp.BulkString(string(frame.Data))); err != nil {
				return
			}
		}
		s.streamReplicationFrames(ctx, writer, sub, ackErr)
		return
	}

	snapshotter, err := s.snapshotter()
	if err != nil {
		_ = writer.WriteValue(resp.Error("ERR snapshot unavailable"))
		return
	}

	// Establish snapshot + stream boundary while ordinary writes/EXEC are blocked.
	s.persistenceMu.Lock()
	entries, snapshotErr := snapshotter.Snapshot()
	id, baseOffset, sub := primary.SubscribeAtCurrent()
	s.persistenceMu.Unlock()
	if snapshotErr != nil {
		primary.Unsubscribe(sub)
		_ = writer.WriteValue(resp.Error("ERR snapshot failed"))
		return
	}
	defer primary.Unsubscribe(sub)
	if !primary.SetReplicaInfo(sub, remoteAddr, "full") {
		return
	}
	ackErr := s.startReplicaAckReader(conn, reader, primary, sub.ReplicaID)

	payload, err := json.Marshal(entries)
	if err != nil {
		return
	}
	primary.RecordFullSync()
	if err := writer.WriteValue(resp.SimpleString(fmt.Sprintf("FULLRESYNC %s %d %s", id, baseOffset, sub.ReplicaID))); err != nil {
		return
	}
	if err := writer.WriteValue(resp.BulkString(string(payload))); err != nil {
		return
	}
	s.streamReplicationFrames(ctx, writer, sub, ackErr)
}

func (s *Server) startReplicaAckReader(conn net.Conn, reader *resp.Reader, primary *replication.Primary, replicaID string) <-chan error {
	result := make(chan error, 1)
	go func() {
		err := s.readReplicaAcks(reader, primary, replicaID)
		if err != nil {
			_ = conn.Close()
		}
		result <- err
		close(result)
	}()
	return result
}

func (s *Server) readReplicaAcks(reader *resp.Reader, primary *replication.Primary, replicaID string) error {
	for {
		argv, err := reader.ReadCommand()
		if err != nil {
			return err
		}
		if len(argv) != 4 || strings.ToUpper(argv[0]) != "REPLCONF" || strings.ToUpper(argv[1]) != "ACK" {
			if s.metrics != nil {
				s.metrics.ReplicationAcksRejected.Inc()
			}
			return fmt.Errorf("invalid replication ACK frame %q", strings.Join(argv, " "))
		}
		if argv[2] != replicaID {
			if s.metrics != nil {
				s.metrics.ReplicationAcksRejected.Inc()
			}
			return fmt.Errorf("replication ACK identity mismatch: got %q want %q", argv[2], replicaID)
		}
		offset, err := strconv.ParseInt(argv[3], 10, 64)
		if err != nil {
			if s.metrics != nil {
				s.metrics.ReplicationAcksRejected.Inc()
			}
			return fmt.Errorf("invalid replication ACK offset %q", argv[3])
		}
		if err := primary.Acknowledge(replicaID, offset, time.Now()); err != nil {
			if s.metrics != nil {
				s.metrics.ReplicationAcksRejected.Inc()
			}
			return fmt.Errorf("reject replication ACK: %w", err)
		}
		if s.metrics != nil {
			s.metrics.ReplicationAcksReceived.Inc()
		}
	}
}

func (s *Server) streamReplicationFrames(ctx context.Context, writer *resp.Writer, sub *replication.Subscription, ackErr <-chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ackErr:
			return
		case frame, ok := <-sub.C:
			if !ok {
				return
			}
			if err := writer.WriteValue(resp.BulkString(string(frame.Data))); err != nil {
				return
			}
		}
	}
}

func (s *Server) runReplicaLoop(ctx context.Context, upstream string) {
	for ctx.Err() == nil {
		if err := s.followPrimaryOnce(ctx, upstream); err != nil && ctx.Err() == nil {
			if s.metrics != nil {
				s.metrics.ReplicationLinkFailures.Inc()
			}
			s.logger.Warn("replication link lost", "upstream", upstream, "error", err)
		}
		timer := time.NewTimer(s.cfg.ReplicationReconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Server) followPrimaryOnce(ctx context.Context, upstream string) error {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", upstream)
	if err != nil {
		return err
	}
	defer conn.Close()

	s.stateMu.Lock()
	s.replicaConn = conn
	s.stateMu.Unlock()
	defer func() {
		s.stateMu.Lock()
		if s.replicaConn == conn {
			s.replicaConn = nil
		}
		s.stateMu.Unlock()
	}()

	s.replStateMu.Lock()
	id := s.replicaID
	offset := s.replicaOffset
	s.replStateMu.Unlock()
	if id == "" {
		id = "?"
		offset = -1
	}
	if err := writeReplicationCommand(conn, []string{"PSYNC", id, strconv.FormatInt(offset, 10)}); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	header, err := readReplicationSimpleString(reader)
	if err != nil {
		return err
	}
	parts := strings.Fields(header)
	if len(parts) < 1 {
		return errors.New("empty replication handshake")
	}

	var connectionReplicaID string
	switch parts[0] {
	case "FULLRESYNC":
		if len(parts) != 4 {
			return fmt.Errorf("invalid FULLRESYNC header %q", header)
		}
		baseOffset, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || baseOffset < 0 {
			return fmt.Errorf("invalid FULLRESYNC offset %q", parts[2])
		}
		connectionReplicaID = parts[3]
		if connectionReplicaID == "" {
			return errors.New("FULLRESYNC missing replica id")
		}
		snapshotPayload, err := readReplicationBulk(reader)
		if err != nil {
			return err
		}
		var entries []store.SnapshotEntry
		if err := json.Unmarshal(snapshotPayload, &entries); err != nil {
			return fmt.Errorf("decode full sync snapshot: %w", err)
		}
		if err := s.installReplicaSnapshot(entries, parts[1], baseOffset); err != nil {
			return err
		}
		if s.metrics != nil {
			s.metrics.ReplicationFullSyncsApplied.Inc()
		}
		if err := writeReplicationACK(conn, connectionReplicaID, baseOffset); err != nil {
			return err
		}
	case "CONTINUE":
		if len(parts) != 4 {
			return fmt.Errorf("invalid CONTINUE header %q", header)
		}
		if parts[1] != id {
			return fmt.Errorf("CONTINUE identity mismatch: got %q want %q", parts[1], id)
		}
		if _, err := strconv.ParseInt(parts[2], 10, 64); err != nil {
			return fmt.Errorf("invalid CONTINUE offset %q", parts[2])
		}
		connectionReplicaID = parts[3]
		if connectionReplicaID == "" {
			return errors.New("CONTINUE missing replica id")
		}
		if s.metrics != nil {
			s.metrics.ReplicationPartialSyncsApplied.Inc()
		}
		// The PSYNC request offset is already the replica's locally applied
		// boundary. ACK it explicitly before catch-up frames arrive.
		if err := writeReplicationACK(conn, connectionReplicaID, offset); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected replication handshake %q", header)
	}

	for {
		payload, err := readReplicationBulk(reader)
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			continue
		}
		if err := s.applyReplicationPayload(payload); err != nil {
			return err
		}
		if err := writeReplicationACK(conn, connectionReplicaID, s.replicaAppliedOffset()); err != nil {
			return err
		}
	}
}

func writeReplicationACK(writer io.Writer, replicaID string, offset int64) error {
	return writeReplicationCommand(writer, []string{"REPLCONF", "ACK", replicaID, strconv.FormatInt(offset, 10)})
}

func writeReplicationCommand(writer io.Writer, argv []string) error {
	payload := resp.EncodeCommand(argv)
	for len(payload) > 0 {
		n, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func (s *Server) replicaAppliedOffset() int64 {
	s.replStateMu.Lock()
	defer s.replStateMu.Unlock()
	return s.replicaOffset
}

func (s *Server) installReplicaSnapshot(entries []store.SnapshotEntry, id string, offset int64) error {
	snapshotter, err := s.snapshotter()
	if err != nil {
		return err
	}
	s.persistenceMu.Lock()
	defer s.persistenceMu.Unlock()
	if err := snapshotter.RestoreSnapshot(entries); err != nil {
		return fmt.Errorf("restore replication snapshot: %w", err)
	}
	s.versions.bumpAll()
	s.replStateMu.Lock()
	s.replicaID = id
	s.replicaOffset = offset
	s.replStateMu.Unlock()
	return nil
}

func (s *Server) applyReplicationPayload(payload []byte) error {
	commands, err := decodeReplicationCommands(payload)
	if err != nil {
		return err
	}
	if len(commands) == 0 {
		return nil
	}

	s.persistenceMu.Lock()
	defer s.persistenceMu.Unlock()
	if s.currentReplicationRole() != config.ReplicationReplica {
		return errors.New("replication payload received after node stopped being a replica")
	}

	transaction := strings.ToUpper(commands[0][0]) == "MULTI"
	if transaction {
		if len(commands) < 2 || strings.ToUpper(commands[len(commands)-1][0]) != "EXEC" {
			return errors.New("replication transaction frame missing EXEC")
		}
		commands = commands[1 : len(commands)-1]
	}
	for _, argv := range commands {
		name := strings.ToUpper(argv[0])
		if name == "MULTI" || name == "EXEC" {
			return errors.New("nested replication transaction marker")
		}
		result := s.registry.Execute(&command.Context{Store: s.store}, argv)
		if !result.Runnable || result.Reply.IsError() {
			return fmt.Errorf("apply replication command %q failed", strings.Join(argv, " "))
		}
		if result.Spec.Mutating {
			s.noteAcceptedMutation(argv, nil)
		}
	}

	s.replStateMu.Lock()
	s.replicaOffset += int64(len(payload))
	s.replStateMu.Unlock()
	if s.metrics != nil {
		s.metrics.ReplicationFramesApplied.Inc()
		s.metrics.ReplicationBytesApplied.Add(uint64(len(payload)))
	}
	return nil
}

func decodeReplicationCommands(payload []byte) ([][]string, error) {
	reader := resp.NewReader(bytes.NewReader(payload))
	var commands [][]string
	for {
		argv, err := reader.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode replication frame: %w", err)
		}
		if len(argv) == 0 {
			return nil, errors.New("empty replication command")
		}
		commands = append(commands, argv)
	}
	return commands, nil
}

func (s *Server) replicateCommandsLocked(commands [][]string, transaction bool) {
	if s.currentReplicationRole() != config.ReplicationPrimary || len(commands) == 0 {
		return
	}
	primary := s.currentReplicationPrimary()
	if primary == nil {
		return
	}
	var payload []byte
	if transaction {
		payload = append(payload, resp.EncodeCommand([]string{"MULTI"})...)
	}
	for _, argv := range commands {
		payload = append(payload, resp.EncodeCommand(argv)...)
	}
	if transaction {
		payload = append(payload, resp.EncodeCommand([]string{"EXEC"})...)
	}
	primary.Append(payload)
}

func readReplicationSimpleString(reader *bufio.Reader) (string, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if prefix == '-' {
		return "", errors.New(line)
	}
	if prefix != '+' {
		return "", fmt.Errorf("expected simple string, got prefix %q", prefix)
	}
	return line, nil
}

func readReplicationBulk(reader *bufio.Reader) ([]byte, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '$' {
		return nil, fmt.Errorf("expected replication bulk frame, got prefix %q", prefix)
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	n, err := strconv.Atoi(line)
	if err != nil || n < 0 {
		return nil, fmt.Errorf("invalid replication bulk length %q", line)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	var crlf [2]byte
	if _, err := io.ReadFull(reader, crlf[:]); err != nil {
		return nil, err
	}
	if crlf != [2]byte{'\r', '\n'} {
		return nil, errors.New("replication bulk frame missing CRLF")
	}
	return payload, nil
}
