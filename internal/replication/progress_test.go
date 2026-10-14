package replication

import (
	"errors"
	"testing"
	"time"
)

func TestPrimaryAcknowledgesMonotonicAppliedProgress(t *testing.T) {
	p, err := NewPrimaryWithID("primary-A", 1024, 8)
	if err != nil {
		t.Fatal(err)
	}
	first := p.Append([]byte("frame-one"))
	_, current, sub, ok := p.SubscribeFrom("primary-A", 0)
	if !ok {
		t.Fatal("expected partial subscription")
	}
	defer p.Unsubscribe(sub)
	if current != first.End {
		t.Fatalf("current=%d want=%d", current, first.End)
	}
	if !p.SetReplicaInfo(sub, "127.0.0.1:12345", "partial") {
		t.Fatal("set replica info failed")
	}

	now := time.Unix(100, 0)
	if err := p.Acknowledge(sub.ReplicaID, first.End, now); err != nil {
		t.Fatal(err)
	}
	if got := p.Offset(); got != first.End {
		t.Fatalf("ACK changed canonical offset: got=%d want=%d", got, first.End)
	}

	progress := p.ReplicaProgressSnapshot(now.Add(25 * time.Millisecond))
	if len(progress) != 1 {
		t.Fatalf("progress=%#v", progress)
	}
	got := progress[0]
	if !got.Acknowledged || got.AcknowledgedOffset != first.End || got.LagBytes != 0 {
		t.Fatalf("progress=%#v", got)
	}
	if got.AckAge != 25*time.Millisecond || got.RemoteAddr != "127.0.0.1:12345" || got.SyncMode != "partial" {
		t.Fatalf("metadata=%#v", got)
	}

	// Equal ACKs are legal and refresh liveness without changing progress.
	later := now.Add(time.Second)
	if err := p.Acknowledge(sub.ReplicaID, first.End, later); err != nil {
		t.Fatal(err)
	}
	progress = p.ReplicaProgressSnapshot(later)
	if progress[0].AckAge != 0 {
		t.Fatalf("equal ACK did not refresh age: %#v", progress[0])
	}
}

func TestPrimaryRejectsInvalidAcknowledgements(t *testing.T) {
	p, err := NewPrimaryWithID("primary-A", 1024, 8)
	if err != nil {
		t.Fatal(err)
	}
	frame := p.Append([]byte("payload"))
	_, _, sub := p.SubscribeAtCurrent()
	defer p.Unsubscribe(sub)

	if err := p.Acknowledge("other", frame.End, time.Now()); !errors.Is(err, ErrUnknownReplica) {
		t.Fatalf("wrong replica error=%v", err)
	}
	if err := p.Acknowledge(sub.ReplicaID, -1, time.Now()); !errors.Is(err, ErrInvalidAckOffset) {
		t.Fatalf("negative ACK error=%v", err)
	}
	if err := p.Acknowledge(sub.ReplicaID, frame.End+1, time.Now()); !errors.Is(err, ErrAckAhead) {
		t.Fatalf("ahead ACK error=%v", err)
	}
	if err := p.Acknowledge(sub.ReplicaID, frame.End, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := p.Acknowledge(sub.ReplicaID, frame.End-1, time.Now()); !errors.Is(err, ErrAckRegression) {
		t.Fatalf("regressing ACK error=%v", err)
	}

	progress := p.ReplicaProgressSnapshot(time.Now())
	if len(progress) != 1 || progress[0].AcknowledgedOffset != frame.End {
		t.Fatalf("rejected ACK changed state: %#v", progress)
	}
}

func TestPrimaryDisconnectRemovesLiveProgress(t *testing.T) {
	p, err := NewPrimaryWithID("primary-A", 1024, 8)
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("payload"))
	_, _, sub := p.SubscribeAtCurrent()
	if err := p.Acknowledge(sub.ReplicaID, p.Offset(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := len(p.ReplicaProgressSnapshot(time.Now())); got != 1 {
		t.Fatalf("live progress count=%d want=1", got)
	}
	p.Unsubscribe(sub)
	if got := len(p.ReplicaProgressSnapshot(time.Now())); got != 0 {
		t.Fatalf("progress survived disconnect: %d", got)
	}
}
