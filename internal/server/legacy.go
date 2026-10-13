package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/resp"
)

// serveLegacy preserves the v0.1 goroutine-per-client design as a reference
// baseline. It is intentionally not the default engine on Linux in v0.2.
func (s *Server) serveLegacy(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.setLegacyListener(listener)
	s.setAddr(listener.Addr())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(s.cfg.ActiveExpireEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.runMaintenance()
			}
		}
	}()

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	s.logger.Info("mini-redis listening", "addr", listener.Addr().String(), "network_engine", "legacy")
	defer func() {
		_ = listener.Close()
		wg.Wait()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleBlockingConnection(ctx, conn)
		}()
	}
}

func (s *Server) handleBlockingConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	var writeMu sync.Mutex
	writeReply := func(value resp.Value) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return writer.WriteValue(value)
	}
	session := s.newClientSession(writeReply)
	defer s.closeClientSession(session)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if s.cfg.IdleTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		}
		argv, err := reader.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			_ = writeReply(resp.Error("ERR " + err.Error()))
			return
		}

		for _, reply := range s.executeClientCommand(session, argv) {
			if err := writeReply(reply); err != nil {
				return
			}
		}
	}
}
