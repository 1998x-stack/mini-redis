package replication

import (
	"testing"
	"time"
)

func TestPrimaryPartialResyncAndLiveHandoff(t *testing.T) {
	p, err := NewPrimaryWithID("primary-A", 1024, 8)
	if err != nil {
		t.Fatal(err)
	}
	first := p.Append([]byte("frame-one"))
	second := p.Append([]byte("frame-two"))

	frames, current, sub, ok := p.SubscribeFrom("primary-A", first.End)
	if !ok {
		t.Fatal("expected partial resync")
	}
	defer p.Unsubscribe(sub)
	if current != second.End || len(frames) != 1 || string(frames[0].Data) != "frame-two" {
		t.Fatalf("current=%d frames=%#v", current, frames)
	}

	third := p.Append([]byte("frame-three"))
	select {
	case live := <-sub.C:
		if live.Start != third.Start || live.End != third.End || string(live.Data) != "frame-three" {
			t.Fatalf("live=%#v", live)
		}
	case <-time.After(time.Second):
		t.Fatal("live frame not delivered")
	}
}

func TestPrimaryRejectsStaleIdentityAndWrappedOffset(t *testing.T) {
	p, err := NewPrimaryWithID("primary-A", 5, 8)
	if err != nil {
		t.Fatal(err)
	}
	f1 := p.Append([]byte("111"))
	p.Append([]byte("222"))

	if _, _, sub, ok := p.SubscribeFrom("old-primary", f1.End); ok || sub != nil {
		t.Fatal("stale replication id must force full sync")
	}
	if _, _, sub, ok := p.SubscribeFrom("primary-A", 0); ok || sub != nil {
		t.Fatal("wrapped backlog must force full sync")
	}
}

func TestSubscribeAtCurrentBuffersLaterFrames(t *testing.T) {
	p, err := NewPrimaryWithID("primary-A", 1024, 8)
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("base"))
	id, offset, sub := p.SubscribeAtCurrent()
	defer p.Unsubscribe(sub)
	if id != "primary-A" || offset != 4 {
		t.Fatalf("id=%q offset=%d", id, offset)
	}
	p.Append([]byte("after"))
	select {
	case frame := <-sub.C:
		if string(frame.Data) != "after" || frame.Start != 5 {
			t.Fatalf("frame=%#v", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("post-boundary frame missing")
	}
}
