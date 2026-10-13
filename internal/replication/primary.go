package replication

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
)

var ErrInvalidBacklogSize = errors.New("replication backlog size must be positive")

type Subscription struct {
	id uint64
	C  <-chan Frame
}

type subscriber struct {
	ch chan Frame
}

type Stats struct {
	FullSyncs    uint64
	PartialSyncs uint64
	Subscribers  int
}

// Primary owns one replication identity, canonical byte offset, retained
// backlog and live replica fan-out. It is transport-agnostic: the server layer
// decides how snapshot/handshake data is moved over TCP.
type Primary struct {
	mu           sync.Mutex
	id           string
	backlog      *Backlog
	nextSub      uint64
	subs         map[uint64]*subscriber
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
		id:       id,
		backlog:  NewBacklog(backlogBytes),
		subs:     make(map[uint64]*subscriber),
		buffered: subscriberFrames,
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

// Append publishes one already-ordered canonical mutation frame. Slow live
// subscribers are disconnected instead of blocking primary command execution.
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
			close(sub.ch)
			delete(p.subs, id)
		}
	}
	return frame
}

// SubscribeAtCurrent attaches a live subscriber at exactly the current stream
// end. Callers use this while holding the server mutation-ordering lock during
// full-sync snapshot capture; later writes therefore cannot fall into a gap.
func (p *Primary) SubscribeAtCurrent() (string, int64, *Subscription) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.id, p.backlog.EndOffset(), p.addSubscriberLocked()
}

// SubscribeFrom atomically captures the retained suffix and attaches the live
// subscriber. No append can occur between suffix capture and subscription.
func (p *Primary) SubscribeFrom(id string, offset int64) ([]byte, int64, *Subscription, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id != p.id {
		return nil, p.backlog.EndOffset(), nil, false
	}
	suffix, ok := p.backlog.SuffixAfter(offset)
	if !ok {
		return nil, p.backlog.EndOffset(), nil, false
	}
	return suffix, p.backlog.EndOffset(), p.addSubscriberLocked(), true
}

func (p *Primary) addSubscriberLocked() *Subscription {
	p.nextSub++
	id := p.nextSub
	ch := make(chan Frame, p.buffered)
	p.subs[id] = &subscriber{ch: ch}
	return &Subscription{id: id, C: ch}
}

func (p *Primary) Unsubscribe(sub *Subscription) {
	if sub == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.subs[sub.id]
	if !ok {
		return
	}
	close(current.ch)
	delete(p.subs, sub.id)
}
