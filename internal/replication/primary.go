package replication

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrInvalidBacklogSize = errors.New("replication backlog size must be positive")
	ErrUnknownReplica     = errors.New("replication ACK references unknown replica")
	ErrInvalidAckOffset   = errors.New("replication ACK offset must be non-negative")
	ErrAckRegression      = errors.New("replication ACK offset regressed")
	ErrAckAhead           = errors.New("replication ACK offset is ahead of primary")
)

type Subscription struct {
	id        uint64
	ReplicaID string
	C         <-chan Frame
}

type subscriber struct {
	ch          chan Frame
	replicaID   string
	remoteAddr  string
	syncMode    string
	acked       bool
	ackOffset   int64
	lastAckTime time.Time
}

type Stats struct {
	FullSyncs    uint64
	PartialSyncs uint64
	Subscribers  int
}

// ReplicaProgress is an immutable point-in-time description of one live
// replication connection. LagBytes/AckAge are -1 until the first ACK arrives.
type ReplicaProgress struct {
	ReplicaID          string
	RemoteAddr         string
	SyncMode           string
	Acknowledged       bool
	AcknowledgedOffset int64
	LagBytes           int64
	AckAge             time.Duration
}

// PrimarySnapshot captures canonical offset/backlog state and all live replica
// progress under one Primary.mu boundary so INFO cannot combine different
// replication instants during a concurrent append.
type PrimarySnapshot struct {
	ID           string
	Offset       int64
	BacklogStart int64
	Stats        Stats
	Replicas     []ReplicaProgress
}

// Primary owns one replication identity, canonical byte offset, retained
// backlog, live replica fan-out and v0.13 applied-offset acknowledgements.
// ACK metadata is runtime connection state; it never enters the backlog.
type Primary struct {
	mu           sync.Mutex
	id           string
	backlog      *Backlog
	nextSub      uint64
	subs         map[uint64]*subscriber
	replicaIndex map[string]uint64
	buffered     int
	fullSyncs    uint64
	partialSyncs uint64
}

func NewPrimary(backlogBytes, subscriberFrames int) (*Primary, error) {
	idBytes := make([]byte, 20)
	if _, err := rand.Read(idBytes); err != nil {
		return nil, err
	}
	return NewPrimaryWithID(hex.EncodeToString(idBytes), backlogBytes, subscriberFrames)
}

func NewPrimaryWithID(id string, backlogBytes, subscriberFrames int) (*Primary, error) {
	if backlogBytes <= 0 {
		return nil, ErrInvalidBacklogSize
	}
	if subscriberFrames <= 0 {
		subscriberFrames = 128
	}
	return &Primary{
		id:           id,
		backlog:      NewBacklog(backlogBytes),
		subs:         make(map[uint64]*subscriber),
		replicaIndex: make(map[string]uint64),
		buffered:     subscriberFrames,
	}, nil
}

func (p *Primary) ID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.id
}

func (p *Primary) Offset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.backlog.EndOffset()
}

func (p *Primary) BacklogStartOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.backlog.StartOffset()
}

func (p *Primary) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{FullSyncs: p.fullSyncs, PartialSyncs: p.partialSyncs, Subscribers: len(p.subs)}
}

// Snapshot returns one self-consistent primary/replica observation.
func (p *Primary) Snapshot(now time.Time) PrimarySnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	if now.IsZero() {
		now = time.Now()
	}
	end := p.backlog.EndOffset()
	return PrimarySnapshot{
		ID:           p.id,
		Offset:       end,
		BacklogStart: p.backlog.StartOffset(),
		Stats: Stats{
			FullSyncs:    p.fullSyncs,
			PartialSyncs: p.partialSyncs,
			Subscribers:  len(p.subs),
		},
		Replicas: p.replicaProgressSnapshotLocked(now, end),
	}
}

func (p *Primary) RecordFullSync() {
	p.mu.Lock()
	p.fullSyncs++
	p.mu.Unlock()
}

func (p *Primary) RecordPartialSync() {
	p.mu.Lock()
	p.partialSyncs++
	p.mu.Unlock()
}

func (p *Primary) Append(payload []byte) Frame {
	p.mu.Lock()
	defer p.mu.Unlock()
	frame := p.backlog.Append(payload)
	if len(payload) == 0 {
		return frame
	}
	for id, sub := range p.subs {
		copyFrame := Frame{Start: frame.Start, End: frame.End, Data: append([]byte(nil), frame.Data...)}
		select {
		case sub.ch <- copyFrame:
		default:
			p.removeSubscriberLocked(id)
		}
	}
	return frame
}

func (p *Primary) SubscribeAtCurrent() (string, int64, *Subscription) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.id, p.backlog.EndOffset(), p.addSubscriberLocked()
}

// SubscribeFrom atomically snapshots retained complete frames after offset and
// attaches the live subscriber. No append can fall between catch-up and live.
func (p *Primary) SubscribeFrom(id string, offset int64) ([]Frame, int64, *Subscription, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id != p.id {
		return nil, p.backlog.EndOffset(), nil, false
	}
	frames, ok := p.backlog.FramesAfter(offset)
	if !ok {
		return nil, p.backlog.EndOffset(), nil, false
	}
	return frames, p.backlog.EndOffset(), p.addSubscriberLocked(), true
}

func (p *Primary) addSubscriberLocked() *Subscription {
	p.nextSub++
	id := p.nextSub
	replicaID := fmt.Sprintf("replica-%d", id)
	ch := make(chan Frame, p.buffered)
	p.subs[id] = &subscriber{ch: ch, replicaID: replicaID, ackOffset: -1}
	p.replicaIndex[replicaID] = id
	return &Subscription{id: id, ReplicaID: replicaID, C: ch}
}

// SetReplicaInfo attaches transport metadata to an already-created live
// subscriber. It does not affect replication eligibility or stream ordering.
func (p *Primary) SetReplicaInfo(sub *Subscription, remoteAddr, syncMode string) bool {
	if sub == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.subs[sub.id]
	if !ok || current.replicaID != sub.ReplicaID {
		return false
	}
	current.remoteAddr = remoteAddr
	current.syncMode = syncMode
	return true
}

// Acknowledge records applied progress reported by one live replica. Equal ACKs
// are accepted so a replica can refresh liveness without inventing progress.
func (p *Primary) Acknowledge(replicaID string, offset int64, now time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	subID, ok := p.replicaIndex[replicaID]
	if !ok {
		return ErrUnknownReplica
	}
	sub, ok := p.subs[subID]
	if !ok || sub.replicaID != replicaID {
		return ErrUnknownReplica
	}
	if offset < 0 {
		return ErrInvalidAckOffset
	}
	if offset > p.backlog.EndOffset() {
		return ErrAckAhead
	}
	if sub.acked && offset < sub.ackOffset {
		return ErrAckRegression
	}
	if now.IsZero() {
		now = time.Now()
	}
	sub.acked = true
	sub.ackOffset = offset
	sub.lastAckTime = now
	return nil
}

// ReplicaProgressSnapshot returns copies of live ACK state sorted by replica ID.
func (p *Primary) ReplicaProgressSnapshot(now time.Time) []ReplicaProgress {
	p.mu.Lock()
	defer p.mu.Unlock()
	if now.IsZero() {
		now = time.Now()
	}
	return p.replicaProgressSnapshotLocked(now, p.backlog.EndOffset())
}

func (p *Primary) replicaProgressSnapshotLocked(now time.Time, end int64) []ReplicaProgress {
	out := make([]ReplicaProgress, 0, len(p.subs))
	for _, sub := range p.subs {
		progress := ReplicaProgress{
			ReplicaID:          sub.replicaID,
			RemoteAddr:         sub.remoteAddr,
			SyncMode:           sub.syncMode,
			Acknowledged:       sub.acked,
			AcknowledgedOffset: -1,
			LagBytes:           -1,
			AckAge:             -1,
		}
		if sub.acked {
			progress.AcknowledgedOffset = sub.ackOffset
			progress.LagBytes = end - sub.ackOffset
			if progress.LagBytes < 0 {
				progress.LagBytes = 0
			}
			age := now.Sub(sub.lastAckTime)
			if age < 0 {
				age = 0
			}
			progress.AckAge = age
		}
		out = append(out, progress)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReplicaID < out[j].ReplicaID })
	return out
}

func (p *Primary) Unsubscribe(sub *Subscription) {
	if sub == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removeSubscriberLocked(sub.id)
}

func (p *Primary) removeSubscriberLocked(id uint64) {
	current, ok := p.subs[id]
	if !ok {
		return
	}
	close(current.ch)
	delete(p.replicaIndex, current.replicaID)
	delete(p.subs, id)
}
