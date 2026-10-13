package sentinel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeNode struct {
	mu       sync.Mutex
	id       string
	status   NodeStatus
	promotes int
	follows  int
}

func newFakeNode(id string, role Role, offset int64) *fakeNode {
	status := NodeStatus{ID: id, Role: role, Offset: offset}
	if role == RolePrimary {
		status.ReplicationAddr = id + ":repl"
	}
	return &fakeNode{id: id, status: status}
}

func (n *fakeNode) ID() string { return n.id }

func (n *fakeNode) Probe(context.Context) (NodeStatus, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.status, nil
}

func (n *fakeNode) Promote(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.promotes++
	n.status.Role = RolePrimary
	n.status.Upstream = ""
	n.status.ReplicationAddr = n.id + ":repl"
	n.status.ReplicationID = "promoted-" + n.id
	n.status.Offset = 0
	return nil
}

func (n *fakeNode) Follow(_ context.Context, upstream string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.follows++
	n.status.Role = RoleReplica
	n.status.Upstream = upstream
	n.status.ReplicationAddr = ""
	n.status.ReplicationID = ""
	n.status.Offset = 0
	return nil
}

func (n *fakeNode) snapshot() NodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.status
}

type faultMatrix struct {
	mu   sync.RWMutex
	down map[string]map[string]bool
}

func newFaultMatrix() *faultMatrix {
	return &faultMatrix{down: make(map[string]map[string]bool)}
}

func (f *faultMatrix) set(monitorID, nodeID string, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[monitorID] == nil {
		f.down[monitorID] = make(map[string]bool)
	}
	f.down[monitorID][nodeID] = down
}

func (f *faultMatrix) probe(ctx context.Context, monitorID string, node Node) (NodeStatus, error) {
	f.mu.RLock()
	down := f.down[monitorID][node.ID()]
	f.mu.RUnlock()
	if down {
		return NodeStatus{}, errors.New("simulated network failure")
	}
	return node.Probe(ctx)
}

func testController(t *testing.T, nodes []Node, faults *faultMatrix) *Controller {
	t.Helper()
	controller, err := NewController(Config{
		MonitorIDs: []string{"m1", "m2", "m3"},
		Quorum:     2,
		DownAfter:  100 * time.Millisecond,
	}, "p", nodes, faults.probe)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func TestSubjectiveDownAloneCannotCreateObjectiveDown(t *testing.T) {
	p := newFakeNode("p", RolePrimary, 120)
	r1 := newFakeNode("r1", RoleReplica, 120)
	r2 := newFakeNode("r2", RoleReplica, 110)
	faults := newFaultMatrix()
	faults.set("m1", "p", true)
	controller := testController(t, []Node{p, r1, r2}, faults)

	t0 := time.Unix(1_700_000_000, 0)
	if _, err := controller.Tick(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	events, err := controller.Tick(context.Background(), t0.Add(101*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	state, ok := controller.MonitorState("m1", "p")
	if !ok || !state.SubjectiveDown {
		t.Fatalf("m1 state=%#v", state)
	}
	if controller.IsObjectiveDown("p") {
		t.Fatal("one subjective-down vote must not create ODOWN")
	}
	if controller.Epoch() != 0 || controller.PrimaryID() != "p" {
		t.Fatalf("epoch=%d primary=%s", controller.Epoch(), controller.PrimaryID())
	}
	for _, event := range events {
		if event.Kind == EventObjectiveDown || event.Kind == EventPromotion {
			t.Fatalf("unexpected event %#v", event)
		}
	}
}

func TestQuorumFailoverChoosesFreshestReplicaOnceAndRepairsOldPrimary(t *testing.T) {
	p := newFakeNode("p", RolePrimary, 120)
	r1 := newFakeNode("r1", RoleReplica, 120)
	r2 := newFakeNode("r2", RoleReplica, 80)
	faults := newFaultMatrix()
	for _, monitorID := range []string{"m1", "m2", "m3"} {
		faults.set(monitorID, "p", true)
	}
	controller := testController(t, []Node{p, r1, r2}, faults)

	t0 := time.Unix(1_700_000_000, 0)
	if _, err := controller.Tick(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	events, err := controller.Tick(context.Background(), t0.Add(101*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	decision := controller.LastDecision()
	if decision.Epoch != 1 || decision.LeaderID != "m1" || decision.OldPrimaryID != "p" || decision.NewPrimaryID != "r1" {
		t.Fatalf("decision=%#v", decision)
	}
	if controller.PrimaryID() != "r1" || controller.Epoch() != 1 {
		t.Fatalf("primary=%s epoch=%d", controller.PrimaryID(), controller.Epoch())
	}
	if r1.snapshot().Role != RolePrimary {
		t.Fatalf("r1=%#v", r1.snapshot())
	}
	if got := r2.snapshot(); got.Role != RoleReplica || got.Upstream != "r1:repl" {
		t.Fatalf("r2=%#v", got)
	}
	if p.snapshot().Role != RolePrimary {
		t.Fatal("unreachable old primary must not be silently reconfigured")
	}
	if len(events) == 0 {
		t.Fatal("expected observable failover events")
	}

	if _, err := controller.Tick(context.Background(), t0.Add(202*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if controller.Epoch() != 1 {
		t.Fatalf("repeated tick triggered another epoch: %d", controller.Epoch())
	}

	for _, monitorID := range []string{"m1", "m2", "m3"} {
		faults.set(monitorID, "p", false)
	}
	events, err = controller.Tick(context.Background(), t0.Add(303*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if got := p.snapshot(); got.Role != RoleReplica || got.Upstream != "r1:repl" {
		t.Fatalf("returned old primary=%#v", got)
	}
	foundRejoin := false
	for _, event := range events {
		if event.Kind == EventRejoined && event.NodeID == "p" {
			foundRejoin = true
		}
	}
	if !foundRejoin {
		t.Fatalf("events=%#v", events)
	}
}

func TestCandidateRequiresQuorumReachabilityEvenWhenFreshest(t *testing.T) {
	p := newFakeNode("p", RolePrimary, 200)
	r1 := newFakeNode("r1", RoleReplica, 190)
	r2 := newFakeNode("r2", RoleReplica, 150)
	faults := newFaultMatrix()
	for _, monitorID := range []string{"m1", "m2", "m3"} {
		faults.set(monitorID, "p", true)
	}
	faults.set("m2", "r1", true)
	faults.set("m3", "r1", true)
	controller := testController(t, []Node{p, r1, r2}, faults)

	t0 := time.Unix(1_700_000_000, 0)
	_, _ = controller.Tick(context.Background(), t0)
	if _, err := controller.Tick(context.Background(), t0.Add(101*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if decision := controller.LastDecision(); decision.NewPrimaryID != "r2" {
		t.Fatalf("decision=%#v", decision)
	}
}
