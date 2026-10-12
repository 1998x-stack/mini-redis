package netpoll

import "time"

type Interest uint8

const (
	Read Interest = 1 << iota
	Write
)

type Event struct {
	FD       int
	Readable bool
	Writable bool
	Closed   bool
	Errored  bool
}

type Poller interface {
	Add(fd int, interest Interest) error
	Mod(fd int, interest Interest) error
	Del(fd int) error
	Wait(timeout time.Duration) ([]Event, error)
	Close() error
}

// New returns the platform event poller. v0.2 intentionally models Redis'
// Linux event-loop path with epoll. Non-Linux builds keep a clear unsupported
// boundary instead of silently changing the mechanism.
func New() (Poller, error) {
	return newPlatformPoller()
}
