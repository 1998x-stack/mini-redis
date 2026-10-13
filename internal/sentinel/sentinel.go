package sentinel

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Role string

const (
	RoleStandalone Role = "standalone"
	RolePrimary    Role = "primary"
	RoleReplica    Role = "replica"
)

type NodeStatus struct {
	ID              string
	Role            Role
	ClientAddr      string
	ReplicationAddr string
	Upstream        string
	ReplicationID   string
	Offset          int64
}

// Node is the control-plane contract. Sentinel asks a node for health/topology
// state and may only change replication role; data copy remains the v0.8
// replication mechanism behind Promote and Follow.
type Node interface {
	ID() string
	Probe(context.Context) (NodeStatus, error)
	Promote(context.Context) error
	Follow(context.Context, string) error
}

type ProbeFunc func(context.Context, string, Node) (NodeStatus, error)

type Config struct {
	MonitorIDs []string
	Quorum     int
	DownAfter  time.Duration
}

type EventKind string

const (
	EventSubjectiveDown EventKind = "sdown"
	EventRecovered      EventKind = "recovered"
	EventObjectiveDown  EventKind = "odown"
	EventElection       EventKind = "election"
	EventPromotion      EventKind = "promotion"
	EventReconfigured   EventKind = "reconfigured"
	EventRejoined       EventKind = "rejoined"
)

type Event struct {
	Kind      EventKind
	MonitorID string
	NodeID    string
	Epoch     uint64
	Detail    string
}

type Decision struct {
	Epoch        uint64
	LeaderID     string
	OldPrimaryID string
	NewPrimaryID string
}

type MonitorState struct {
	MonitorID      string
	NodeID         string
	LastSuccess    time.Time
	LastFailure    time.Time
	FailureSince   time.Time
	SubjectiveDown bool
	LastStatus     NodeStatus
}

type monitor struct {
	id        string
	downAfter time.Duration
	mu        sync.RWMutex
	state     map[string]MonitorState
}

func newMonitor(id string, downAfter time.Duration) *monitor {
	return &monitor{id: id, downAfter: downAfter, state: make(map[string]MonitorState)}
}

func (m *monitor) observe(now time.Time, nodeID string, status NodeStatus, err error) (MonitorState, MonitorState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	before := m.state[nodeID]
	after := before
	after.MonitorID = m.id
	after.NodeID = nodeID
	if err == nil {
		after.LastSuccess = now
		after.FailureSince = time.Time{}
		after.SubjectiveDown = false
		after.LastStatus = status
	} else {
		after.LastFailure = now
		if after.FailureSince.IsZero() {
			after.FailureSince = now
		}
		if !now.Before(after.FailureSince.Add(m.downAfter)) {
			after.SubjectiveDown = true
		}
	}
	m.state[nodeID] = after
	return before, after
}

func (m *monitor) snapshot(nodeID string) MonitorState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state[nodeID]
}

type probeResult struct {
	status NodeStatus
	err    error
}

type candidateScore struct {
	nodeID  string
	upVotes int
	offset  int64
}

type Controller struct {
	tickMu sync.Mutex
	mu     sync.RWMutex

	cfg           Config
	monitorIDs    []string
	monitors      map[string]*monitor
	nodeIDs       []string
	nodes         map[string]Node
	probe         ProbeFunc
	primaryID     string
	epoch         uint64
	objectiveDown map[string]bool
	lastDecision  Decision
}

var ErrNoEligibleReplica = errors.New("sentinel: no eligible replica for failover")

func NewController(cfg Config, primaryID string, nodes []Node, probe ProbeFunc) (*Controller, error) {
	if len(cfg.MonitorIDs) == 0 {
		return nil, errors.New("sentinel: at least one monitor is required")
	}
	if cfg.Quorum <= 0 || cfg.Quorum > len(cfg.MonitorIDs) {
		return nil, fmt.Errorf("sentinel: quorum %d outside monitor set", cfg.Quorum)
	}
	if cfg.DownAfter <= 0 {
		return nil, errors.New("sentinel: down-after must be positive")
	}
	if primaryID == "" {
		return nil, errors.New("sentinel: primary id is required")
	}

	monitorIDs := append([]string(nil), cfg.MonitorIDs...)
	sort.Strings(monitorIDs)
	monitors := make(map[string]*monitor, len(monitorIDs))
	for i, id := range monitorIDs {
		if id == "" {
			return nil, errors.New("sentinel: monitor id cannot be empty")
		}
		if i > 0 && id == monitorIDs[i-1] {
			return nil, fmt.Errorf("sentinel: duplicate monitor id %q", id)
		}
		monitors[id] = newMonitor(id, cfg.DownAfter)
	}

	nodeMap := make(map[string]Node, len(nodes))
	nodeIDs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node == nil || node.ID() == "" {
			return nil, errors.New("sentinel: every node needs a non-empty id")
		}
		if _, exists := nodeMap[node.ID()]; exists {
			return nil, fmt.Errorf("sentinel: duplicate node id %q", node.ID())
		}
		nodeMap[node.ID()] = node
		nodeIDs = append(nodeIDs, node.ID())
	}
	if _, ok := nodeMap[primaryID]; !ok {
		return nil, fmt.Errorf("sentinel: primary %q is not in node set", primaryID)
	}
	sort.Strings(nodeIDs)
	if probe == nil {
		probe = func(ctx context.Context, _ string, node Node) (NodeStatus, error) {
			return node.Probe(ctx)
		}
	}

	return &Controller{
		cfg:           cfg,
		monitorIDs:    monitorIDs,
		monitors:      monitors,
		nodeIDs:       nodeIDs,
		nodes:         nodeMap,
		probe:         probe,
		primaryID:     primaryID,
		objectiveDown: make(map[string]bool),
	}, nil
}

func (c *Controller) PrimaryID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.primaryID
}

func (c *Controller) Epoch() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.epoch
}

func (c *Controller) LastDecision() Decision {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastDecision
}

func (c *Controller) IsObjectiveDown(nodeID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.objectiveDown[nodeID]
}

func (c *Controller) MonitorState(monitorID, nodeID string) (MonitorState, bool) {
	c.mu.RLock()
	monitor := c.monitors[monitorID]
	c.mu.RUnlock()
	if monitor == nil {
		return MonitorState{}, false
	}
	state := monitor.snapshot(nodeID)
	return state, state.NodeID != ""
}

func (c *Controller) Tick(ctx context.Context, now time.Time) ([]Event, error) {
	c.tickMu.Lock()
	defer c.tickMu.Unlock()
	if now.IsZero() {
		now = time.Now()
	}

	results := make(map[string]map[string]probeResult, len(c.monitorIDs))
	events := make([]Event, 0)
	for _, monitorID := range c.monitorIDs {
		monitor := c.monitors[monitorID]
		results[monitorID] = make(map[string]probeResult, len(c.nodeIDs))
		for _, nodeID := range c.nodeIDs {
			status, err := c.probe(ctx, monitorID, c.nodes[nodeID])
			results[monitorID][nodeID] = probeResult{status: status, err: err}
			before, after := monitor.observe(now, nodeID, status, err)
			if !before.SubjectiveDown && after.SubjectiveDown {
				events = append(events, Event{Kind: EventSubjectiveDown, MonitorID: monitorID, NodeID: nodeID})
			}
			if before.SubjectiveDown && !after.SubjectiveDown {
				events = append(events, Event{Kind: EventRecovered, MonitorID: monitorID, NodeID: nodeID})
			}
		}
	}

	primaryID := c.PrimaryID()
	downVoters := c.subjectiveDownVoters(primaryID)
	if len(downVoters) < c.cfg.Quorum {
		c.setObjectiveDown(primaryID, false)
		repairEvents, err := c.repairTopology(ctx, results, primaryID)
		events = append(events, repairEvents...)
		return events, err
	}

	if c.setObjectiveDown(primaryID, true) {
		events = append(events, Event{
			Kind:   EventObjectiveDown,
			NodeID: primaryID,
			Detail: fmt.Sprintf("%d/%d monitors report SDOWN", len(downVoters), len(c.monitorIDs)),
		})
	}

	candidateID, err := c.chooseCandidate(results, primaryID)
	if err != nil {
		return events, err
	}
	leaderID := downVoters[0]
	epoch := c.nextEpoch()
	events = append(events, Event{Kind: EventElection, MonitorID: leaderID, NodeID: candidateID, Epoch: epoch})

	candidate := c.nodes[candidateID]
	if err := candidate.Promote(ctx); err != nil {
		return events, fmt.Errorf("sentinel: epoch %d promotion of %s failed: %w", epoch, candidateID, err)
	}
	promoted, err := candidate.Probe(ctx)
	if err != nil {
		return events, fmt.Errorf("sentinel: promoted node %s could not be probed: %w", candidateID, err)
	}
	if promoted.Role != RolePrimary || promoted.ReplicationAddr == "" {
		return events, fmt.Errorf("sentinel: promoted node %s did not expose primary replication endpoint", candidateID)
	}
	events = append(events, Event{Kind: EventPromotion, MonitorID: leaderID, NodeID: candidateID, Epoch: epoch})

	var reconfigureErrs []error
	for _, nodeID := range c.nodeIDs {
		if nodeID == candidateID || nodeID == primaryID {
			continue
		}
		if c.currentUpVotes(results, nodeID) < c.cfg.Quorum {
			continue
		}
		if err := c.nodes[nodeID].Follow(ctx, promoted.ReplicationAddr); err != nil {
			reconfigureErrs = append(reconfigureErrs, fmt.Errorf("%s follow %s: %w", nodeID, candidateID, err))
			continue
		}
		events = append(events, Event{Kind: EventReconfigured, NodeID: nodeID, Epoch: epoch, Detail: "follow " + candidateID})
	}

	c.mu.Lock()
	c.primaryID = candidateID
	c.objectiveDown[candidateID] = false
	c.lastDecision = Decision{Epoch: epoch, LeaderID: leaderID, OldPrimaryID: primaryID, NewPrimaryID: candidateID}
	c.mu.Unlock()
	if len(reconfigureErrs) > 0 {
		return events, errors.Join(reconfigureErrs...)
	}
	return events, nil
}

func (c *Controller) subjectiveDownVoters(nodeID string) []string {
	voters := make([]string, 0, len(c.monitorIDs))
	for _, monitorID := range c.monitorIDs {
		if c.monitors[monitorID].snapshot(nodeID).SubjectiveDown {
			voters = append(voters, monitorID)
		}
	}
	return voters
}

func (c *Controller) setObjectiveDown(nodeID string, value bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := c.objectiveDown[nodeID] != value
	c.objectiveDown[nodeID] = value
	return changed && value
}

func (c *Controller) nextEpoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	return c.epoch
}

func (c *Controller) currentUpVotes(results map[string]map[string]probeResult, nodeID string) int {
	votes := 0
	for _, monitorID := range c.monitorIDs {
		if result, ok := results[monitorID][nodeID]; ok && result.err == nil {
			votes++
		}
	}
	return votes
}

func (c *Controller) chooseCandidate(results map[string]map[string]probeResult, oldPrimaryID string) (string, error) {
	scores := make([]candidateScore, 0)
	for _, nodeID := range c.nodeIDs {
		if nodeID == oldPrimaryID {
			continue
		}
		upVotes := 0
		replicaVotes := 0
		var freshest int64 = -1
		for _, monitorID := range c.monitorIDs {
			result := results[monitorID][nodeID]
			if result.err != nil {
				continue
			}
			upVotes++
			if result.status.Role == RoleReplica {
				replicaVotes++
				if result.status.Offset > freshest {
					freshest = result.status.Offset
				}
			}
		}
		if upVotes < c.cfg.Quorum || replicaVotes < c.cfg.Quorum {
			continue
		}
		scores = append(scores, candidateScore{nodeID: nodeID, upVotes: upVotes, offset: freshest})
	}
	if len(scores) == 0 {
		return "", ErrNoEligibleReplica
	}
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].upVotes != scores[j].upVotes {
			return scores[i].upVotes > scores[j].upVotes
		}
		if scores[i].offset != scores[j].offset {
			return scores[i].offset > scores[j].offset
		}
		return scores[i].nodeID < scores[j].nodeID
	})
	return scores[0].nodeID, nil
}

func (c *Controller) repairTopology(ctx context.Context, results map[string]map[string]probeResult, primaryID string) ([]Event, error) {
	if c.currentUpVotes(results, primaryID) < c.cfg.Quorum {
		return nil, nil
	}
	primaryStatus, ok := c.bestStatus(results, primaryID)
	if !ok || primaryStatus.Role != RolePrimary || primaryStatus.ReplicationAddr == "" {
		return nil, nil
	}

	events := make([]Event, 0)
	var errs []error
	for _, nodeID := range c.nodeIDs {
		if nodeID == primaryID || c.currentUpVotes(results, nodeID) < c.cfg.Quorum {
			continue
		}
		status, ok := c.bestStatus(results, nodeID)
		if !ok {
			continue
		}
		if status.Role == RoleReplica && status.Upstream == primaryStatus.ReplicationAddr {
			continue
		}
		kind := EventReconfigured
		if status.Role == RolePrimary {
			kind = EventRejoined
		}
		if err := c.nodes[nodeID].Follow(ctx, primaryStatus.ReplicationAddr); err != nil {
			errs = append(errs, fmt.Errorf("repair %s follow %s: %w", nodeID, primaryID, err))
			continue
		}
		events = append(events, Event{Kind: kind, NodeID: nodeID, Epoch: c.Epoch(), Detail: "follow " + primaryID})
	}
	if len(errs) > 0 {
		return events, errors.Join(errs...)
	}
	return events, nil
}

func (c *Controller) bestStatus(results map[string]map[string]probeResult, nodeID string) (NodeStatus, bool) {
	var best NodeStatus
	found := false
	for _, monitorID := range c.monitorIDs {
		result := results[monitorID][nodeID]
		if result.err != nil {
			continue
		}
		if !found || result.status.Offset > best.Offset {
			best = result.status
			found = true
		}
	}
	return best, found
}
