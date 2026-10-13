package cluster

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

type NodeInfo struct {
	ID    string `json:"id"`
	Addr  string `json:"addr"`
	Epoch uint64 `json:"epoch"`
}

type SlotState struct {
	OwnerID         string `json:"owner_id"`
	MigrationTarget string `json:"migration_target,omitempty"`
	Epoch           uint64 `json:"epoch"`
}

type SlotRecord struct {
	Slot  int       `json:"slot"`
	State SlotState `json:"state"`
}

type GossipPage struct {
	Nodes []NodeInfo   `json:"nodes,omitempty"`
	Slots []SlotRecord `json:"slots"`
	Next  int          `json:"next"`
}

type RouteAction uint8

const (
	RouteExecute RouteAction = iota + 1
	RouteMoved
	RouteAsk
	RouteClusterDown
)

type RouteDecision struct {
	Action RouteAction
	Slot   int
	NodeID string
	Addr   string
}

type Topology struct {
	mu    sync.RWMutex
	epoch uint64
	nodes map[string]NodeInfo
	slots [SlotCount]SlotState
}

func NewTopology() *Topology {
	return &Topology{nodes: make(map[string]NodeInfo)}
}

func (t *Topology) nextEpochLocked() uint64 {
	t.epoch++
	return t.epoch
}

func (t *Topology) Epoch() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.epoch
}

func (t *Topology) UpsertNode(id, addr string) error {
	if id == "" || addr == "" {
		return errors.New("cluster: node id and address are required")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	epoch := t.nextEpochLocked()
	t.nodes[id] = NodeInfo{ID: id, Addr: addr, Epoch: epoch}
	return nil
}

func (t *Topology) Node(id string) (NodeInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	return n, ok
}

func validateSlot(slot int) error {
	if slot < 0 || slot >= SlotCount {
		return fmt.Errorf("cluster: slot %d out of range", slot)
	}
	return nil
}

func (t *Topology) AssignSlot(slot int, ownerID string) error {
	return t.AssignRange(slot, slot, ownerID)
}

func (t *Topology) AssignRange(start, end int, ownerID string) error {
	if err := validateSlot(start); err != nil {
		return err
	}
	if err := validateSlot(end); err != nil {
		return err
	}
	if start > end {
		return errors.New("cluster: invalid slot range")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.nodes[ownerID]; !ok {
		return fmt.Errorf("cluster: unknown owner %q", ownerID)
	}
	epoch := t.nextEpochLocked()
	for slot := start; slot <= end; slot++ {
		t.slots[slot] = SlotState{OwnerID: ownerID, Epoch: epoch}
	}
	return nil
}

func (t *Topology) Slot(slot int) (SlotState, bool) {
	if validateSlot(slot) != nil {
		return SlotState{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	state := t.slots[slot]
	return state, state.OwnerID != ""
}

func (t *Topology) BeginMigration(slot int, sourceID, targetID string) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if sourceID == targetID {
		return errors.New("cluster: migration source and target must differ")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.slots[slot]
	if state.OwnerID != sourceID {
		return fmt.Errorf("cluster: slot %d owner is %q, not %q", slot, state.OwnerID, sourceID)
	}
	if _, ok := t.nodes[targetID]; !ok {
		return fmt.Errorf("cluster: unknown target %q", targetID)
	}
	state.MigrationTarget = targetID
	state.Epoch = t.nextEpochLocked()
	t.slots[slot] = state
	return nil
}

func (t *Topology) AbortMigration(slot int, sourceID, targetID string) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.slots[slot]
	if state.OwnerID != sourceID || state.MigrationTarget != targetID {
		return fmt.Errorf("cluster: slot %d is not migrating %s -> %s", slot, sourceID, targetID)
	}
	state.MigrationTarget = ""
	state.Epoch = t.nextEpochLocked()
	t.slots[slot] = state
	return nil
}

func (t *Topology) CommitMigration(slot int, sourceID, targetID string) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.slots[slot]
	if state.OwnerID != sourceID || state.MigrationTarget != targetID {
		return fmt.Errorf("cluster: slot %d is not migrating %s -> %s", slot, sourceID, targetID)
	}
	state.OwnerID = targetID
	state.MigrationTarget = ""
	state.Epoch = t.nextEpochLocked()
	t.slots[slot] = state
	return nil
}

// Route evaluates one key from one node's point of view. During migration the
// source keeps serving keys it still owns physically, while a missing key gets
// a temporary ASK to the importing target. The target may execute only after a
// one-request ASKING grant.
func (t *Topology) Route(localID, key string, keyExists, asking bool) RouteDecision {
	slot := HashSlot(key)
	t.mu.RLock()
	state := t.slots[slot]
	owner := t.nodes[state.OwnerID]
	target := t.nodes[state.MigrationTarget]
	t.mu.RUnlock()

	if state.OwnerID == "" {
		return RouteDecision{Action: RouteClusterDown, Slot: slot}
	}
	if state.MigrationTarget != "" {
		if localID == state.OwnerID {
			if keyExists {
				return RouteDecision{Action: RouteExecute, Slot: slot, NodeID: localID}
			}
			return RouteDecision{Action: RouteAsk, Slot: slot, NodeID: target.ID, Addr: target.Addr}
		}
		if localID == state.MigrationTarget {
			if asking {
				return RouteDecision{Action: RouteExecute, Slot: slot, NodeID: localID}
			}
			return RouteDecision{Action: RouteMoved, Slot: slot, NodeID: owner.ID, Addr: owner.Addr}
		}
	}
	if localID == state.OwnerID {
		return RouteDecision{Action: RouteExecute, Slot: slot, NodeID: localID}
	}
	return RouteDecision{Action: RouteMoved, Slot: slot, NodeID: owner.ID, Addr: owner.Addr}
}

// GossipPage returns a bounded deterministic slice of the slot table. Repeated
// pages make the control-plane work explicit instead of hiding an unbounded
// 16384-slot transfer behind one call.
func (t *Topology) GossipPage(start, limit int) (GossipPage, error) {
	if start < 0 || start > SlotCount {
		return GossipPage{}, errors.New("cluster: invalid gossip cursor")
	}
	if limit <= 0 {
		return GossipPage{}, errors.New("cluster: gossip limit must be positive")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	end := start + limit
	if end > SlotCount {
		end = SlotCount
	}
	page := GossipPage{Next: -1}
	if end < SlotCount {
		page.Next = end
	}
	page.Slots = make([]SlotRecord, 0, end-start)
	for slot := start; slot < end; slot++ {
		page.Slots = append(page.Slots, SlotRecord{Slot: slot, State: t.slots[slot]})
	}
	ids := make([]string, 0, len(t.nodes))
	for id := range t.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		page.Nodes = append(page.Nodes, t.nodes[id])
	}
	return page, nil
}

func slotTieKey(state SlotState) string {
	return state.OwnerID + "\x00" + state.MigrationTarget
}

// MergePage is last-writer-wins by explicit topology epoch. Equal epochs use
// stable owner+migration-target tuples as a deterministic tie break, which is a
// teaching mechanism, not a distributed-consensus claim.
func (t *Topology) MergePage(page GossipPage) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, n := range page.Nodes {
		if n.ID == "" || n.Addr == "" {
			return errors.New("cluster: invalid gossip node")
		}
		current, ok := t.nodes[n.ID]
		if !ok || n.Epoch > current.Epoch || (n.Epoch == current.Epoch && n.Addr > current.Addr) {
			t.nodes[n.ID] = n
		}
		if n.Epoch > t.epoch {
			t.epoch = n.Epoch
		}
	}
	for _, record := range page.Slots {
		if err := validateSlot(record.Slot); err != nil {
			return err
		}
		current := t.slots[record.Slot]
		incoming := record.State
		if incoming.Epoch > current.Epoch || (incoming.Epoch == current.Epoch && slotTieKey(incoming) > slotTieKey(current)) {
			t.slots[record.Slot] = incoming
		}
		if incoming.Epoch > t.epoch {
			t.epoch = incoming.Epoch
		}
	}
	return nil
}
