package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/aof"
	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
)

type Server struct {
	cfg      config.Config
	store    store.Store
	registry *command.Registry
	aof      *aof.Log
	logger   *slog.Logger

	listener net.Listener
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func New(cfg config.Config, st store.Store, registry *command.Registry, logger *slog.Logger) (*Server, error) {
	if st == nil || registry == nil {
		return nil, errors.New("store and command registry are required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{cfg: cfg, store: st, registry: registry, logger: logger}
	if cfg.AppendOnly {
		log, err := aof.Open(cfg.AppendFilename, cfg.AppendFsync)
		if err != nil {
			return nil, fmt.Errorf("open AOF: %w", err)
		}
		s.aof = log
		if err := s.replayAOF(); err != nil {
			_ = log.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) replayAOF() error {
	if s.aof == nil {
		return nil
	}
	ctx := &command.Context{Store: s.store}
	return s.aof.Replay(func(argv []string) error {
		result := s.registry.Execute(ctx, argv)
		if !result.Runnable || result.Reply.IsError() {
			return fmt.Errorf("AOF contains invalid command %q", strings.Join(argv, " "))
		}
		return nil
	})
}

// ListenAndServe accepts TCP connections. v0.1 intentionally uses one
// goroutine per client for clarity. The server boundary is isolated so v0.2
// can replace this transport with an epoll/kqueue event loop.
func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.listener = listener

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go s.expirationLoop(runCtx)

	s.logger.Info("mini-redis listening", "addr", listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-runCtx.Done():
				return nil
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(runCtx, conn)
		}()
	}
}

func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	cmdCtx := &command.Context{Store: s.store}

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
			_ = writer.WriteValue(resp.Error("ERR " + err.Error()))
			return
		}

		result := s.registry.Execute(cmdCtx, argv)
		if s.aof != nil {
			persist := result.AOFCommands(time.Now())
			if len(persist) > 0 {
				// Persistence is performed before the success reply. If append
				// fails, the client receives an error instead of false durability.
				if err := s.aof.AppendBatch(persist); err != nil {
					s.logger.Error("AOF append failed", "error", err)
					if err := writer.WriteValue(resp.Error("MISCONF AOF persistence failed")); err != nil {
						return
					}
					continue
				}
			}
		}

		if err := writer.WriteValue(result.Reply); err != nil {
			return
		}
	}
}

func (s *Server) expirationLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.ActiveExpireEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.store.ExpireDue(s.cfg.ActiveExpireSample)
		}
	}
}

func (s *Server) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	if s.aof != nil {
		return s.aof.Close()
	}
	return nil
}
