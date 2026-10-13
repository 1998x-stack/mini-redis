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

	if len(copyData) == 0 || b.capacity == 0 {
		return frame
	}
	b.frames = append(b.frames, frame)
	b.used += len(copyData)

	for b.used > b.capacity && len(b.frames) > 0 {
		b.used -= len(b.frames[0].Data)
		b.frames[0].Data = nil
		b.frames = b.frames[1:]
	}
	return frame
}

// FramesAfter returns retained complete frames strictly after offset. ok is
// true only when offset is the current end or a retained frame boundary.
func (b *Backlog) FramesAfter(offset int64) ([]Frame, bool) {
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

	out := make([]Frame, len(b.frames[start:]))
	for i, frame := range b.frames[start:] {
		out[i] = Frame{Start: frame.Start, End: frame.End, Data: append([]byte(nil), frame.Data...)}
	}
	return out, true
}

// SuffixAfter is a convenience view used by focused backlog tests. Transport
// code should prefer FramesAfter so transaction boundaries remain explicit.
func (b *Backlog) SuffixAfter(offset int64) ([]byte, bool) {
	frames, ok := b.FramesAfter(offset)
	if !ok {
		return nil, false
	}
	var size int
	for _, frame := range frames {
		size += len(frame.Data)
	}
	out := make([]byte, 0, size)
	for _, frame := range frames {
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
