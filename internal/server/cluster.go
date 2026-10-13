package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/1998x-stack/mini-redis/internal/cluster"
	"github.com/1998x-stack/mini-redis/internal/persistence"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
)

// ClusterExecute is the v0.10 cluster-aware command gate. It deliberately sits
// above the existing server command path: slot ownership is checked before the
// command reaches Store, while successful execution still uses the same
// persistence/replication machinery established by earlier versions.
func (s *Server) ClusterExecute(topology *cluster.Topology, localID string, session *cluster.Session, argv []string) resp.Value {
	if topology == nil || session == nil {
		return resp.Error("ERR cluster topology and session are required")
	}
	if len(argv) == 0 {
		return resp.Error("ERR empty command")
	}
	name := strings.ToUpper(argv[0])
	if name == "ASKING" {
		if len(argv) != 1 {
			return resp.Error("ERR wrong number of arguments for 'asking' command")
		}
		session.GrantAsking()
		return resp.SimpleString("OK")
	}
	asking := session.ConsumeAsking()

	if name == "CLUSTER" {
		if len(argv) == 3 && strings.EqualFold(argv[1], "KEYSLOT") {
			return resp.Integer(int64(cluster.HashSlot(argv[2])))
		}
		return resp.Error("ERR supported CLUSTER subcommand: KEYSLOT <key>")
	}
	// v0.10 keeps transaction state out of the cluster facade. Multi-key command
	// routing is supported and CROSSSLOT-checked; cross-command MULTI routing is
	// intentionally deferred rather than pretending partial support is atomic.
	switch name {
	case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH":
		return resp.Error("ERR transactions are not supported by mini-redis cluster facade v0.10")
	}

	if _, err := s.registry.Validate(argv); err != nil {
		return resp.Error(err.Error())
	}
	keys, routed, err := cluster.CommandKeys(argv)
	if err != nil {
		return resp.Error(err.Error())
	}
	if !routed || len(keys) == 0 {
		return s.executeCommand(argv)
	}
	if _, err := cluster.CommonSlot(keys); err != nil {
		return resp.Error(err.Error())
	}

	// Existence and execution share persistenceMu. This matters on a migrating
	// source: a concurrent delete must not happen after we decide "serve local"
	// but before the command itself runs.
	s.persistenceMu.Lock()
	exists := false
	for _, key := range keys {
		if s.store.Exists(key) > 0 {
			exists = true
			break
		}
	}
	decision := topology.Route(localID, keys[0], exists, asking)
	if decision.Action != cluster.RouteExecute {
		s.persistenceMu.Unlock()
		return resp.Error(cluster.FormatRedirect(decision))
	}
	reply := s.executeCommandLocked(argv)
	s.persistenceMu.Unlock()
	return reply
}

// ClusterNode adapts a real Server to the cluster slot-migration interface.
// It exports/imports SnapshotEntry, keeping physical Object encodings private.
type ClusterNode struct {
	NodeID string
	Server *Server
}

func (n *ClusterNode) ID() string {
	if n == nil {
		return ""
	}
	return n.NodeID
}

func (n *ClusterNode) KeysInSlot(ctx context.Context, slot int) ([]string, error) {
	if n == nil || n.Server == nil {
		return nil, errors.New("cluster node server is nil")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s := n.Server
	s.persistenceMu.Lock()
	keys, err := s.store.Keys("*")
	s.persistenceMu.Unlock()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0)
	for _, key := range keys {
		if cluster.HashSlot(key) == slot {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

func snapshotEntryForKey(snapshotter store.Snapshotter, key string) (store.SnapshotEntry, bool, error) {
	entries, err := snapshotter.Snapshot()
	if err != nil {
		return store.SnapshotEntry{}, false, err
	}
	for _, entry := range entries {
		if entry.Key == key {
			return entry, true, nil
		}
	}
	return store.SnapshotEntry{}, false, nil
}

// ImportKey installs one logical key atomically with respect to ordinary server
// mutations. DEL + reconstruction commands are framed as one local durability /
// replication transaction so an existing target value cannot leak fields or
// collection members into the imported value.
func (n *ClusterNode) ImportKey(ctx context.Context, entry store.SnapshotEntry) error {
	if n == nil || n.Server == nil {
		return errors.New("cluster node server is nil")
	}
	if entry.Key == "" {
		return errors.New("cluster import requires key")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s := n.Server
	commands, err := persistence.Commands([]store.SnapshotEntry{entry})
	if err != nil {
		return err
	}
	snapshotter, err := s.snapshotter()
	if err != nil {
		return err
	}

	s.persistenceMu.Lock()
	rollback, err := snapshotter.Snapshot()
	if err != nil {
		s.persistenceMu.Unlock()
		return err
	}
	sequence := make([][]string, 0, len(commands)+1)
	sequence = append(sequence, []string{"DEL", entry.Key})
	sequence = append(sequence, commands...)
	var persist [][]string
	for _, argv := range sequence {
		reply, batch := s.executeCommandDeferredLocked(argv)
		if reply.IsError() {
			_ = snapshotter.RestoreSnapshot(rollback)
			s.versions.bumpAll()
			s.persistenceMu.Unlock()
			return fmt.Errorf("cluster import %q failed: %s", entry.Key, reply.Str)
		}
		persist = append(persist, batch...)
	}
	if s.aof != nil && len(persist) > 0 {
		if err := s.aof.AppendTransaction(persist); err != nil {
			_ = snapshotter.RestoreSnapshot(rollback)
			s.versions.bumpAll()
			s.persistenceMu.Unlock()
			return fmt.Errorf("cluster import AOF: %w", err)
		}
	}
	if len(persist) > 0 {
		s.replicateCommandsLocked(persist, true)
	}
	s.persistenceMu.Unlock()
	return nil
}

// TransferKey holds the source mutation-order lock from snapshot capture until
// target acknowledgement and source deletion. The educational coordinator only
// runs one directed slot migration at a time, giving a clear source→target lock
// order and preventing a write from slipping between export and delete.
func (n *ClusterNode) TransferKey(ctx context.Context, key string, target cluster.MigrationTarget) error {
	if n == nil || n.Server == nil || target == nil {
		return errors.New("cluster transfer requires source and target")
	}
	s := n.Server
	snapshotter, err := s.snapshotter()
	if err != nil {
		return err
	}
	s.persistenceMu.Lock()
	entry, ok, err := snapshotEntryForKey(snapshotter, key)
	if err != nil {
		s.persistenceMu.Unlock()
		return err
	}
	if !ok {
		s.persistenceMu.Unlock()
		return nil
	}
	if err := target.ImportKey(ctx, entry); err != nil {
		s.persistenceMu.Unlock()
		return err
	}
	reply := s.executeCommandLocked([]string{"DEL", key})
	s.persistenceMu.Unlock()
	if reply.IsError() {
		return fmt.Errorf("cluster source delete %q failed: %s", key, reply.Str)
	}
	return nil
}
