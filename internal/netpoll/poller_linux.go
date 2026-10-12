//go:build linux

package netpoll

import (
	"errors"
	"syscall"
	"time"
)

type epollPoller struct {
	fd     int
	events []syscall.EpollEvent
}

func newPlatformPoller() (Poller, error) {
	fd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &epollPoller{fd: fd, events: make([]syscall.EpollEvent, 128)}, nil
}

func (p *epollPoller) Add(fd int, interest Interest) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{Events: epollMask(interest), Fd: int32(fd)})
}

func (p *epollPoller) Mod(fd int, interest Interest) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_MOD, fd, &syscall.EpollEvent{Events: epollMask(interest), Fd: int32(fd)})
}

func (p *epollPoller) Del(fd int) error {
	err := syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.EBADF) {
		return nil
	}
	return err
}

func (p *epollPoller) Wait(timeout time.Duration) ([]Event, error) {
	ms := -1
	if timeout >= 0 {
		if timeout == 0 {
			ms = 0
		} else {
			ms = int((timeout + time.Millisecond - 1) / time.Millisecond)
			if ms < 1 {
				ms = 1
			}
		}
	}

	for {
		n, err := syscall.EpollWait(p.fd, p.events, ms)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out := make([]Event, 0, n)
		for _, raw := range p.events[:n] {
			flags := raw.Events
			out = append(out, Event{
				FD:       int(raw.Fd),
				Readable: flags&syscall.EPOLLIN != 0,
				Writable: flags&syscall.EPOLLOUT != 0,
				Closed:   flags&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0,
				Errored:  flags&syscall.EPOLLERR != 0,
			})
		}
		return out, nil
	}
}

func (p *epollPoller) Close() error {
	if p.fd < 0 {
		return nil
	}
	err := syscall.Close(p.fd)
	p.fd = -1
	return err
}

func epollMask(interest Interest) uint32 {
	mask := uint32(syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP)
	if interest&Read != 0 {
		mask |= syscall.EPOLLIN
	}
	if interest&Write != 0 {
		mask |= syscall.EPOLLOUT
	}
	return mask
}
