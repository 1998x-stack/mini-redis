//go:build !linux

package netpoll

import "fmt"

func newPlatformPoller() (Poller, error) {
	return nil, fmt.Errorf("reactor engine requires Linux epoll in mini-redis v0.2")
}
