package cluster

import (
	"context"
	"fmt"

	"github.com/1998x-stack/mini-redis/internal/store"
)

// MigrationTarget receives one stable logical SnapshotEntry. Import is allowed
// to be implementation-specific, but must return success only after the target
// owns a complete logical value including its absolute expiry deadline.
type MigrationTarget interface {
	ID() string
	ImportKey(context.Context, store.SnapshotEntry) error
}

// MigrationSource owns the source-side atomicity boundary for one key transfer.
// TransferKey must not delete the source copy until target import succeeds.
type MigrationSource interface {
	ID() string
	KeysInSlot(context.Context, int) ([]string, error)
	TransferKey(context.Context, string, MigrationTarget) error
}

// MigrateSlot makes migration state visible before any key moves. If the same
// source→target migration is already active, the operation resumes from the
// source's remaining keys. The stable owner changes only after every remaining
// source key transfers successfully.
func MigrateSlot(ctx context.Context, topology *Topology, slot int, source MigrationSource, target MigrationTarget) error {
	if topology == nil || source == nil || target == nil {
		return fmt.Errorf("cluster: topology/source/target are required")
	}
	state, ok := topology.Slot(slot)
	if !ok {
		return fmt.Errorf("cluster: slot %d is unassigned", slot)
	}
	if state.OwnerID != source.ID() {
		return fmt.Errorf("cluster: slot %d owner is %q, not %q", slot, state.OwnerID, source.ID())
	}
	if state.MigrationTarget == "" {
		if err := topology.BeginMigration(slot, source.ID(), target.ID()); err != nil {
			return err
		}
	} else if state.MigrationTarget != target.ID() {
		return fmt.Errorf("cluster: slot %d already migrates to %q", slot, state.MigrationTarget)
	}

	keys, err := source.KeysInSlot(ctx, slot)
	if err != nil {
		return fmt.Errorf("cluster: list slot %d keys: %w", slot, err)
	}
	for _, key := range keys {
		if HashSlot(key) != slot {
			return fmt.Errorf("cluster: source returned key %q outside slot %d", key, slot)
		}
		if err := source.TransferKey(ctx, key, target); err != nil {
			// Keep the migration marker on partial transfer. Some keys may already
			// live at the target, so pretending the slot returned to a pristine
			// stable state would hide reality. A retry resumes from remaining keys.
			return fmt.Errorf("cluster: transfer key %q: %w", key, err)
		}
	}
	if err := topology.CommitMigration(slot, source.ID(), target.ID()); err != nil {
		return fmt.Errorf("cluster: commit slot %d migration: %w", slot, err)
	}
	return nil
}
