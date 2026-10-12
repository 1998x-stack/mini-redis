package server

import (
	"context"
	"errors"
	"fmt"
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

	stateMu        sync.RWMutex
	cancel         context.CancelFunc
	legacyListener net.Listener
	addr           net.Addr
	done           chan struct{}
	started        bool

	closeAOFOnce sync.Once
	closeAOFErr  error
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

// ListenAndServe chooses the requested network engine. On Linux, auto selects
// the v0.2 epoll reactor. On other platforms, auto keeps the v0.1 legacy path
// so the project remains runnable while the Linux event-loop mechanism stays
// explicit and inspectable.
func (s *Server) ListenAndServe(ctx context.Context) error {
	s.stateMu.Lock()
	if s.started {
		s.stateMu.Unlock()
		return errors.New("server already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.started = true
	done := s.done
	s.stateMu.Unlock()

	defer func() {
		cancel()
		close(done)
		_ = s.closeAOF()
	}()

	engine := s.cfg.EffectiveNetworkEngine()
	s.logger.Info("starting mini-redis", "network_engine", engine)

	switch engine {
	case config.NetworkReactor:
		return s.serveReactor(runCtx)
	case config.NetworkLegacy:
		return s.serveLegacy(runCtx)
	default:
		return fmt.Errorf("unsupported network engine %q", engine)
	}
}

func (s *Server) executeCommand(argv []string) resp.Value {
	result := s.registry.Execute(&command.Context{Store: s.store}, argv)
	if s.aof != nil {
		persist := result.AOFCommands(time.Now())
		if len(persist) > 0 {
			// Keep v0.1's durability boundary: append before acknowledging the
			// mutation. AOF writes remain synchronous in v0.2 so networking, not
			// disk I/O, is the isolated learning variable.
			if err := s.aof.AppendBatch(persist); err != nil {
				s.logger.Error("AOF append failed", "error", err)
				return resp.Error("MISCONF AOF persistence failed")
			}
		}
	}
	return result.Reply
}

func (s *Server) setAddr(addr net.Addr) {
	s.stateMu.Lock()
	s.addr = addr
	s.stateMu.Unlock()
}

func (s *Server) setLegacyListener(listener net.Listener) {
	s.stateMu.Lock()
	s.legacyListener = listener
	s.stateMu.Unlock()
}

func (s *Server) Addr() net.Addr {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.addr
}

func (s *Server) closeAOF() error {
	s.closeAOFOnce.Do(func() {
		if s.aof != nil {
			s.closeAOFErr = s.aof.Close()
		}
	})
	return s.closeAOFErr
}

func (s *Server) Close() error {
	s.stateMu.RLock()
	cancel := s.cancel
	listener := s.legacyListener
	done := s.done
	started := s.started
	s.stateMu.RUnlock()

	if cancel != nil {
		cancel()
	}
	if listener != nil {
		_ = listener.Close()
	}
	if started && done != nil {
		<-done
	}
	return s.closeAOF()
}
