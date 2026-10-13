package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/replication"
)

// ReplicationTopology is the observable runtime topology used by the v0.9
// Sentinel teaching subsystem. It intentionally reports logical topology state,
// not private transport internals.
type ReplicationTopology struct {
	Role            config.ReplicationRole
	Upstream        string
	ReplicationAddr string
	ReplicationID   string
	Offset          int64
}

func (s *Server) currentReplicationRole() config.ReplicationRole {
	s.replControlMu.RLock()
	defer s.replControlMu.RUnlock()
	return s.replRole
}

func (s *Server) currentReplicationPrimary() *replication.Primary {
	s.replControlMu.RLock()
	defer s.replControlMu.RUnlock()
	return s.replPrimary
}

func (s *Server) currentReplicationUpstream() string {
	s.replControlMu.RLock()
	defer s.replControlMu.RUnlock()
	return s.replUpstream
}

func (s *Server) replicationRootContext() (context.Context, error) {
	s.replControlMu.RLock()
	defer s.replControlMu.RUnlock()
	if s.replRootCtx == nil {
		return nil, errors.New("replication runtime has not started")
	}
	return s.replRootCtx, nil
}

func (s *Server) ReplicationTopology() ReplicationTopology {
	role := s.currentReplicationRole()
	upstream := s.currentReplicationUpstream()
	_, id, offset := s.ReplicationState()

	s.stateMu.RLock()
	var addr string
	if s.replAddr != nil {
		addr = s.replAddr.String()
	}
	s.stateMu.RUnlock()

	return ReplicationTopology{
		Role:            role,
		Upstream:        upstream,
		ReplicationAddr: addr,
		ReplicationID:   id,
		Offset:          offset,
	}
}

// PromoteToPrimary turns a running replica into a writable primary. The data
// itself is not copied here: the node already owns its replicated v0.8 state.
// Promotion creates a fresh replication identity/backlog and starts a new
// replication listener for surviving replicas to follow.
func (s *Server) PromoteToPrimary() error {
	root, err := s.replicationRootContext()
	if err != nil {
		return err
	}
	if s.currentReplicationRole() == config.ReplicationPrimary {
		return nil
	}
	return s.activatePrimary(root)
}

// FollowPrimary reconfigures a running node as a read-only replica of upstream.
// Explicit topology changes reset the remembered replication identity/offset so
// a different promoted primary starts with FULLRESYNC and cannot accidentally
// reuse an offset from the previous primary identity.
func (s *Server) FollowPrimary(upstream string) error {
	if upstream == "" {
		return errors.New("upstream replication address is required")
	}
	// v0.8 replica semantics deliberately disable local AOF and local eviction.
	// Dynamic demotion must preserve the same invariant: otherwise an old primary
	// could retain a stale AOF or independently evict keys after becoming replica.
	if s.aof != nil {
		return errors.New("v0.9 dynamic demotion requires appendonly=false")
	}
	if s.cfg.MaxMemory > 0 {
		return errors.New("v0.9 dynamic demotion requires maxmemory=0")
	}
	root, err := s.replicationRootContext()
	if err != nil {
		return err
	}
	if s.currentReplicationRole() == config.ReplicationReplica && s.currentReplicationUpstream() == upstream {
		return nil
	}
	return s.activateReplica(root, upstream)
}

func (s *Server) activatePrimary(root context.Context) error {
	primary, err := replication.NewPrimary(s.cfg.ReplicationBacklogBytes, s.cfg.ReplicationSubscriberFrames)
	if err != nil {
		return fmt.Errorf("configure replication primary: %w", err)
	}
	listener, err := net.Listen("tcp", s.cfg.ReplicationAddr)
	if err != nil {
		return fmt.Errorf("listen replication: %w", err)
	}
	roleCtx, roleCancel := context.WithCancel(root)

	s.persistenceMu.Lock()
	s.replControlMu.Lock()
	oldCancel := s.replRoleCancel
	if oldCancel != nil {
		oldCancel()
	}

	s.stateMu.Lock()
	oldListener := s.replListener
	oldReplicaConn := s.replicaConn
	s.replListener = listener
	s.replAddr = listener.Addr()
	s.replicaConn = nil
	s.stateMu.Unlock()
	if oldListener != nil && oldListener != listener {
		_ = oldListener.Close()
	}
	if oldReplicaConn != nil {
		_ = oldReplicaConn.Close()
	}

	s.replRole = config.ReplicationPrimary
	s.replUpstream = ""
	s.replPrimary = primary
	s.replRoleCancel = roleCancel
	s.replStateMu.Lock()
	s.replicaID = ""
	s.replicaOffset = 0
	s.replStateMu.Unlock()
	s.replControlMu.Unlock()
	s.persistenceMu.Unlock()

	go s.serveReplicationPrimary(roleCtx, listener, primary)
	return nil
}

func (s *Server) activateReplica(root context.Context, upstream string) error {
	roleCtx, roleCancel := context.WithCancel(root)

	s.persistenceMu.Lock()
	s.replControlMu.Lock()
	oldCancel := s.replRoleCancel
	if oldCancel != nil {
		oldCancel()
	}

	s.stateMu.Lock()
	oldListener := s.replListener
	oldReplicaConn := s.replicaConn
	s.replListener = nil
	s.replAddr = nil
	s.replicaConn = nil
	s.stateMu.Unlock()
	if oldListener != nil {
		_ = oldListener.Close()
	}
	if oldReplicaConn != nil {
		_ = oldReplicaConn.Close()
	}

	s.replRole = config.ReplicationReplica
	s.replUpstream = upstream
	s.replPrimary = nil
	s.replRoleCancel = roleCancel
	s.replStateMu.Lock()
	s.replicaID = ""
	s.replicaOffset = 0
	s.replStateMu.Unlock()
	s.replControlMu.Unlock()
	s.persistenceMu.Unlock()

	go s.runReplicaLoop(roleCtx, upstream)
	return nil
}
