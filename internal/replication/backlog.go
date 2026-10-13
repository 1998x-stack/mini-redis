package replication

import "sync"

// Frame is one canonical replication unit. Offsets are 1-based byte offsets
// into the logical RESP mutation stream. Start/End include every byte in Data.
type Frame struct {
	Start int64
	End   int64
	Data  []byte
}

// Backlog retains complete replication frames up to a byte budget. Unlike a
// raw byte ring, it never evicts a partial RESP frame, so resumable offsets are
// always valid logical boundaries.
type Backlog struct {
	mu       sync.RWMutex
	capacity int
	used     int
	end      int64
	frames   []Frame
}

func NewBacklog(capacity int) *Backlog {
	if capacity < 0 {
		capacity = 0
	}
	return &Backlog{capacity: capacity}
}

func (b *Backlog) Capacity() int { return b.capacity }

func (b *Backlog) EndOffset() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.end
}

func (b *Backlog) StartOffset() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.frames) == 0 {
		return b.end + 1
	}
	return b.frames[0].Start
}

func (b *Backlog) Append(payload []byte) Frame {
	b.mu.Lock()
	defer b.mu.Unlock()

	copyData := append([]byte(nil), payload...)
	frame := Frame{Start: b.end + 1, End: b.end + int64(len(copyData)), Data: copyData}
	b.end = frame.End

	// Empty frames carry no stream position and are intentionally not retained.
	if len(copyData) == 0 || b.capacity == 0 {
		return frame
	}
	b.frames = append(b.frames, frame)
	b.used += len(copyData)

	// Evict complete oldest frames. A single frame larger than capacity is not
	// partially retained; after eviction partial resync before b.end is impossible.
	for b.used > b.capacity && len(b.frames) > 0 {
		b.used -= len(b.frames[0].Data)
		b.frames[0].Data = nil
		b.frames = b.frames[1:]
	}
	return frame
}

// SuffixAfter returns every retained stream byte after offset. ok is true only
// when offset is exactly the current end or a retained frame boundary. Replica
// offsets are advanced only after whole-frame apply, so accepting arbitrary
// interior byte positions would weaken the protocol invariant.
func (b *Backlog) SuffixAfter(offset int64) ([]byte, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if offset == b.end {
		return nil, true
	}
	if offset < 0 || offset > b.end || len(b.frames) == 0 {
		return nil, false
	}

	start := -1
	for i, frame := range b.frames {
		if frame.Start == offset+1 {
			start = i
			break
		}
		if frame.End == offset {
			start = i + 1
			break
		}
	}
	if start < 0 || start > len(b.frames) {
		return nil, false
	}

	var size int
	for _, frame := range b.frames[start:] {
		size += len(frame.Data)
	}
	out := make([]byte, 0, size)
	for _, frame := range b.frames[start:] {
		out = append(out, frame.Data...)
	}
	return out, true
}

func (b *Backlog) SnapshotFrames() []Frame {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Frame, len(b.frames))
	for i, frame := range b.frames {
		out[i] = Frame{Start: frame.Start, End: frame.End, Data: append([]byte(nil), frame.Data...)}
	}
	return out
}
