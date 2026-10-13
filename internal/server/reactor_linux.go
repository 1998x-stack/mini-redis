//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/1998x-stack/mini-redis/internal/netpoll"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

var (
	errInputBufferLimit  = errors.New("client input buffer limit exceeded")
	errOutputBufferLimit = errors.New("client output buffer limit exceeded")
)

type reactorClient struct {
	fd              int
	input           []byte
	output          []byte
	lastActive      time.Time
	closeAfterWrite bool
	session         *clientSession
}

func (c *reactorClient) queue(payload []byte, limit int) error {
	if len(payload) > limit-len(c.output) {
		return errOutputBufferLimit
	}
	c.output = append(c.output, payload...)
	return nil
}

func (s *Server) serveReactor(ctx context.Context) error {
	poller, err := netpoll.New()
	if err != nil {
		return err
	}
	defer poller.Close()

	listenerFD, listenerAddr, err := openNonblockingTCPListener(s.cfg.Addr)
	if err != nil {
		return err
	}
	defer syscall.Close(listenerFD)
	s.setAddr(listenerAddr)

	var wakeFDs [2]int
	if err := syscall.Pipe2(wakeFDs[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return fmt.Errorf("create reactor wake pipe: %w", err)
	}
	wakeRead, wakeWrite := wakeFDs[0], wakeFDs[1]
	defer syscall.Close(wakeRead)
	defer syscall.Close(wakeWrite)

	if err := poller.Add(listenerFD, netpoll.Read); err != nil {
		return fmt.Errorf("register listener: %w", err)
	}
	if err := poller.Add(wakeRead, netpoll.Read); err != nil {
		return fmt.Errorf("register wake pipe: %w", err)
	}

	clients := make(map[int]*reactorClient)
	defer func() {
		for fd, client := range clients {
			s.closeClientSession(client.session)
			s.metrics.ActiveConnections.Add("reactor", -1)
			_ = poller.Del(fd)
			_ = syscall.Close(fd)
		}
	}()

	go func() {
		<-ctx.Done()
		_, _ = syscall.Write(wakeWrite, []byte{1})
	}()

	now := time.Now()
	nextExpire := now.Add(s.cfg.ActiveExpireEvery)
	idleSweepEvery := idleSweepInterval(s.cfg.IdleTimeout)
	nextIdleSweep := time.Time{}
	if idleSweepEvery > 0 {
		nextIdleSweep = now.Add(idleSweepEvery)
	}

	s.logger.Info("mini-redis listening", "addr", listenerAddr.String(), "network_engine", "reactor")

	for {
		if ctx.Err() != nil {
			return nil
		}

		timeout := untilNextTimer(time.Now(), nextExpire, nextIdleSweep)
		waitStarted := time.Now()
		s.metrics.ReactorPollCalls.Inc()
		events, err := poller.Wait(timeout)
		s.metrics.ReactorPollWait.ObserveDuration(time.Since(waitStarted))
		if len(events) > 0 {
			s.metrics.ReactorPollEvents.Add(uint64(len(events)))
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("epoll wait: %w", err)
		}

		now = time.Now()
		if !now.Before(nextExpire) {
			s.runMaintenance()
			nextExpire = now.Add(s.cfg.ActiveExpireEvery)
		}
		if !nextIdleSweep.IsZero() && !now.Before(nextIdleSweep) {
			s.closeIdleClients(poller, clients, now)
			nextIdleSweep = now.Add(idleSweepEvery)
		}

		for _, event := range events {
			switch event.FD {
			case wakeRead:
				drainFD(wakeRead)
				if ctx.Err() != nil {
					return nil
				}
				continue
			case listenerFD:
				if event.Readable {
					if err := s.acceptReady(poller, listenerFD, clients, now); err != nil {
						return err
					}
				}
				continue
			}

			client := clients[event.FD]
			if client == nil {
				continue
			}

			if event.Readable {
				peerClosed, err := s.readReady(client)
				if err != nil {
					s.closeReactorClient(poller, clients, client.fd)
					continue
				}
				if peerClosed {
					client.closeAfterWrite = true
				}
			}

			if clients[event.FD] == nil {
				continue
			}
			if event.Writable {
				closed, err := s.writeReady(client)
				if err != nil || closed {
					s.closeReactorClient(poller, clients, client.fd)
					continue
				}
			}

			if clients[event.FD] == nil {
				continue
			}
			if event.Errored {
				s.closeReactorClient(poller, clients, client.fd)
				continue
			}
			if event.Closed {
				client.closeAfterWrite = true
			}

			if client.closeAfterWrite && len(client.output) == 0 {
				s.closeReactorClient(poller, clients, client.fd)
				continue
			}
			if err := updateClientInterest(poller, client); err != nil {
				s.closeReactorClient(poller, clients, client.fd)
			}
		}
	}
}

func (s *Server) acceptReady(poller netpoll.Poller, listenerFD int, clients map[int]*reactorClient, now time.Time) error {
	for {
		fd, _, err := syscall.Accept4(listenerFD, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}

		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		s.metrics.Connections.Inc("reactor")
		s.metrics.ActiveConnections.Add("reactor", 1)
		client := &reactorClient{fd: fd, lastActive: now}
		client.session = s.newClientSession(func(value resp.Value) error {
			protocol := client.session.protocolMode()
			payload, err := resp.EncodeValueProtocol(value, protocol)
			if err != nil {
				return err
			}
			if err := client.queue(payload, s.cfg.ClientOutputBufferLimit); err != nil {
				if errors.Is(err, errOutputBufferLimit) {
					s.metrics.Backpressure.Inc("reactor_output")
				}
				return err
			}
			s.observeReply(protocol, value)
			return updateClientInterest(poller, client)
		})
		if err := poller.Add(fd, netpoll.Read); err != nil {
			s.closeClientSession(client.session)
			s.metrics.ActiveConnections.Add("reactor", -1)
			_ = syscall.Close(fd)
			return fmt.Errorf("register client: %w", err)
		}
		clients[fd] = client
	}
}

func (s *Server) readReady(client *reactorClient) (bool, error) {
	var buf [32 << 10]byte
	for {
		n, err := syscall.Read(client.fd, buf[:])
		if n > 0 {
			s.metrics.BytesRead.Add("reactor", uint64(n))
			client.lastActive = time.Now()
			if n > s.cfg.ClientInputBufferLimit-len(client.input) {
				s.metrics.Backpressure.Inc("reactor_input")
				return false, errInputBufferLimit
			}
			client.input = append(client.input, buf[:n]...)
			if err := s.processClientInput(client); err != nil {
				return false, err
			}
			if client.closeAfterWrite {
				return false, nil
			}
		}

		if err == nil {
			if n == 0 {
				return true, nil
			}
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return false, err
	}
}

func (s *Server) processClientInput(client *reactorClient) error {
	for len(client.input) > 0 {
		argv, consumed, complete, err := resp.ParseCommand(client.input)
		if err != nil {
			protocol := client.session.protocolMode()
			value := resp.Error("ERR " + err.Error())
			payload, encodeErr := resp.EncodeValueProtocol(value, protocol)
			if encodeErr != nil {
				return encodeErr
			}
			if err := client.queue(payload, s.cfg.ClientOutputBufferLimit); err != nil {
				if errors.Is(err, errOutputBufferLimit) {
					s.metrics.Backpressure.Inc("reactor_output")
				}
				return err
			}
			s.observeReply(protocol, value)
			client.input = nil
			client.closeAfterWrite = true
			return nil
		}
		if !complete {
			return nil
		}

		if consumed == len(client.input) {
			client.input = nil
		} else {
			client.input = client.input[consumed:]
		}

		for _, reply := range s.executeObservedClientCommand(client.session, argv) {
			protocol := client.session.protocolMode()
			payload, err := resp.EncodeValueProtocol(reply, protocol)
			if err != nil {
				return err
			}
			if err := client.queue(payload, s.cfg.ClientOutputBufferLimit); err != nil {
				if errors.Is(err, errOutputBufferLimit) {
					s.metrics.Backpressure.Inc("reactor_output")
				}
				return err
			}
			s.observeReply(protocol, reply)
		}
	}
	return nil
}

func (s *Server) writeReady(client *reactorClient) (bool, error) {
	for len(client.output) > 0 {
		n, err := syscall.Write(client.fd, client.output)
		if n > 0 {
			s.metrics.BytesWritten.Add("reactor", uint64(n))
			client.output = client.output[n:]
			client.lastActive = time.Now()
		}
		if err == nil {
			if n == 0 {
				break
			}
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return false, err
	}
	return client.closeAfterWrite && len(client.output) == 0, nil
}

func updateClientInterest(poller netpoll.Poller, client *reactorClient) error {
	interest := netpoll.Read
	if len(client.output) > 0 {
		interest |= netpoll.Write
	}
	if client.closeAfterWrite {
		interest = netpoll.Write
	}
	return poller.Mod(client.fd, interest)
}

func (s *Server) closeReactorClient(poller netpoll.Poller, clients map[int]*reactorClient, fd int) {
	client, ok := clients[fd]
	if !ok {
		return
	}
	s.closeClientSession(client.session)
	s.metrics.ActiveConnections.Add("reactor", -1)
	_ = poller.Del(fd)
	_ = syscall.Close(fd)
	delete(clients, fd)
}

func (s *Server) closeIdleClients(poller netpoll.Poller, clients map[int]*reactorClient, now time.Time) {
	if s.cfg.IdleTimeout <= 0 {
		return
	}
	for fd, client := range clients {
		if now.Sub(client.lastActive) >= s.cfg.IdleTimeout {
			s.closeReactorClient(poller, clients, fd)
		}
	}
}

func openNonblockingTCPListener(addr string) (int, net.Addr, error) {
	resolved, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return -1, nil, err
	}

	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, syscall.IPPROTO_TCP)
	if err != nil {
		return -1, nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = syscall.Close(fd)
		}
	}()

	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return -1, nil, err
	}

	var bindAddr [4]byte
	if resolved.IP != nil && !resolved.IP.IsUnspecified() {
		ip4 := resolved.IP.To4()
		if ip4 == nil {
			return -1, nil, fmt.Errorf("reactor engine currently supports IPv4 only: %s", addr)
		}
		copy(bindAddr[:], ip4)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Port: resolved.Port, Addr: bindAddr}); err != nil {
		return -1, nil, err
	}
	if err := syscall.Listen(fd, 512); err != nil {
		return -1, nil, err
	}

	sa, err := syscall.Getsockname(fd)
	if err != nil {
		return -1, nil, err
	}
	inet, okAddr := sa.(*syscall.SockaddrInet4)
	if !okAddr {
		return -1, nil, fmt.Errorf("unexpected listener sockaddr %T", sa)
	}
	ip := net.IPv4(inet.Addr[0], inet.Addr[1], inet.Addr[2], inet.Addr[3])
	actual := &net.TCPAddr{IP: ip, Port: inet.Port}
	ok = true
	return fd, actual, nil
}

func drainFD(fd int) {
	var buf [64]byte
	for {
		_, err := syscall.Read(fd, buf[:])
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return
		}
		if err != nil {
			return
		}
	}
}

func idleSweepInterval(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 0
	}
	if timeout < time.Second {
		return timeout
	}
	return time.Second
}

func untilNextTimer(now, expire, idle time.Time) time.Duration {
	next := expire
	if !idle.IsZero() && idle.Before(next) {
		next = idle
	}
	if !next.After(now) {
		return 0
	}
	return next.Sub(now)
}
