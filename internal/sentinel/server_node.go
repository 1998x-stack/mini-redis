package sentinel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/server"
)

// ServerNode adapts a mini-redis Server to the Sentinel control-plane contract.
// Health is proved through the normal client TCP plane with PING; topology is
// then read from the server's explicit replication state.
type ServerNode struct {
	NodeID       string
	Server       *server.Server
	ProbeTimeout time.Duration
}

func (n *ServerNode) ID() string { return n.NodeID }

func (n *ServerNode) Probe(ctx context.Context) (NodeStatus, error) {
	if n == nil || n.Server == nil || n.NodeID == "" {
		return NodeStatus{}, errors.New("sentinel: invalid server node")
	}
	addr := n.Server.Addr()
	if addr == nil {
		return NodeStatus{}, errors.New("sentinel: server is not listening")
	}

	timeout := n.ProbeTimeout
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr.String())
	if err != nil {
		return NodeStatus{}, err
	}
	defer conn.Close()
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
	if _, err := conn.Write(resp.EncodeCommand([]string{"PING"})); err != nil {
		return NodeStatus{}, err
	}
	pong := make([]byte, len("+PONG\r\n"))
	if _, err := io.ReadFull(conn, pong); err != nil {
		return NodeStatus{}, err
	}
	if string(pong) != "+PONG\r\n" {
		return NodeStatus{}, fmt.Errorf("sentinel: unexpected PING reply %q", pong)
	}

	topology := n.Server.ReplicationTopology()
	return NodeStatus{
		ID:              n.NodeID,
		Role:            sentinelRole(topology.Role),
		ClientAddr:      addr.String(),
		ReplicationAddr: topology.ReplicationAddr,
		Upstream:        topology.Upstream,
		ReplicationID:   topology.ReplicationID,
		Offset:          topology.Offset,
	}, nil
}

func (n *ServerNode) Promote(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return n.Server.PromoteToPrimary()
}

func (n *ServerNode) Follow(ctx context.Context, upstream string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return n.Server.FollowPrimary(upstream)
}

func sentinelRole(role config.ReplicationRole) Role {
	switch role {
	case config.ReplicationPrimary:
		return RolePrimary
	case config.ReplicationReplica:
		return RoleReplica
	default:
		return RoleStandalone
	}
}
