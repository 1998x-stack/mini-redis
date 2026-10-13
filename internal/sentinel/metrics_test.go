package sentinel

import (
	"context"
	"errors"
	"testing"
	"time"
)

type metricsNode struct {
	id     string
	status NodeStatus
}

func (n *metricsNode) ID() string { return n.id }
func (n *metricsNode) Probe(context.Context) (NodeStatus, error) {
	return n.status, nil
}
func (n *metricsNode) Promote(context.Context) error {
	n.status.Role = RolePrimary
	n.status.Upstream = ""
	if n.status.ReplicationAddr == "" {
		n.status.ReplicationAddr = n.id + ":6380"
	}
	return nil
}
func (n *metricsNode) Follow(_ context.Context, upstream string) error {
	n.status.Role = RoleReplica
	n.status.Upstream = upstream
	return nil
}

func TestControllerMetricsObserveRealFailover(t *testing.T) {
	primary := &metricsNode{id: "p", status: NodeStatus{
		ID: "p", Role: RolePrimary, ClientAddr: "p:6379", ReplicationAddr: "p:6380", Offset: 20,
	}}
	replica := &metricsNode{id: "r", status: NodeStatus{
		ID: "r", Role: RoleReplica, ClientAddr: "r:6379", ReplicationAddr: "r:6380", Upstream: "p:6380", Offset: 18,
	}}
	probe := func(ctx context.Context, _ string, node Node) (NodeStatus, error) {
		if node.ID() == "p" {
			return NodeStatus{}, errors.New("primary unreachable")
		}
		return node.Probe(ctx)
	}
	controller, err := NewController(Config{
		MonitorIDs: []string{"s1"},
		Quorum:     1,
		DownAfter:  time.Millisecond,
	}, "p", []Node{primary, replica}, probe)
	if err != nil {
		t.Fatal(err)
	}

	base := time.Unix(100, 0)
	if events, err := controller.Tick(context.Background(), base); err != nil {
		t.Fatal(err)
	} else if controller.PrimaryID() != "p" || len(events) != 0 {
		t.Fatalf("first failure should only start down-after: primary=%s events=%v", controller.PrimaryID(), events)
	}
	if events, err := controller.Tick(context.Background(), base.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	} else if controller.PrimaryID() != "r" {
		t.Fatalf("failover did not promote replica: events=%v", events)
	}

	snapshot := controller.MetricsSnapshot()
	if snapshot.Ticks != 2 || snapshot.TickErrors != 0 || snapshot.TickLatency.Count != 2 {
		t.Fatalf("tick metrics=%+v", snapshot)
	}
	if snapshot.Probes != 5 || snapshot.ProbeFailures != 2 || snapshot.ProbeLatency.Count != 5 {
		t.Fatalf("probe metrics=%+v", snapshot)
	}
	if snapshot.FailoverAttempts != 1 || snapshot.Promotions != 1 || snapshot.FailoverLatency.Count != 1 {
		t.Fatalf("failover metrics=%+v", snapshot)
	}
	if snapshot.Epoch != 1 || snapshot.PrimaryID != "r" || snapshot.LastDecision.NewPrimaryID != "r" {
		t.Fatalf("controller snapshot=%+v", snapshot)
	}
}
