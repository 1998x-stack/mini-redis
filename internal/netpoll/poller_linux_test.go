//go:build linux

package netpoll

import (
	"syscall"
	"testing"
	"time"
)

func TestEpollReadableLifecycle(t *testing.T) {
	p, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var fds [2]int
	if err := syscall.Pipe2(fds[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[0])
	defer syscall.Close(fds[1])

	if err := p.Add(fds[0], Read); err != nil {
		t.Fatal(err)
	}
	if _, err := syscall.Write(fds[1], []byte("x")); err != nil {
		t.Fatal(err)
	}

	events, err := p.Wait(250 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].FD != fds[0] || !events[0].Readable {
		t.Fatalf("events=%+v", events)
	}

	if err := p.Del(fds[0]); err != nil {
		t.Fatal(err)
	}
	events, err = p.Wait(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("events after Del=%+v", events)
	}
}
