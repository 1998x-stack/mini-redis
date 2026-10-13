package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/aof"
	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/persistence"
	"github.com/1998x-stack/mini-redis/internal/rdb"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
)

type Server struct {
	cfg      config.Config
	store    store.Store
	registry *command.Registry
	aof      *aof.Log
	logger   *slog.Logger

	// persistenceMu names the v0.5 state/AOF boundary. A command's Store mutation
	// and AOF append happen under the same lock relative to rewrite snapshotting.
	persistenceMu sync.Mutex

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

	// Startup policy is deliberately explicit: when AOF is enabled it is the
	// authoritative source and RDB is ignored. When AOF is disabled, an existing
	// RDB is loaded; a missing RDB means an empty database. Corrupt/incompatible
	// RDB and invalid complete AOF commands fail startup.
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
	} else if cfg.RDBFilename != "" {
		if err := s.loadRDBIfPresent(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) snapshotter() (store.Snapshotter, error) {
	snapshotter, ok := s.store.(store.Snapshotter)
	if !ok {
		return nil, errors.New("store does not support logical snapshots")
	}
	return snapshotter, nil
}

func (s *Server) loadRDBIfPresent() error {
	entries, err := rdb.LoadFile(s.cfg.RDBFilename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("load RDB: %w", err)
	}
	snapshotter, err := s.snapshotter()
	if err != nil {
		return err
	}
	if err := snapshotter.RestoreSnapshot(entries); err != nil {
		return fmt.Errorf("restore RDB: %w", err)
	}
	return nil
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

// SaveRDB performs the v0.5 blocking reference snapshot. Memory.Snapshot owns
// its Store lock while extracting logical state; file publication is then done
// through fsync + atomic rename without serializing private encodings.
func (s *Server) SaveRDB() error {
	if s.cfg.RDBFilename == "" {
		return errors.New("RDB filename is empty")
	}
	snapshotter, err := s.snapshotter()
	if err != nil {
		return err
	}
	entries, err := snapshotter.Snapshot()
	if err != nil {
		return err
	}
	return rdb.SaveFile(s.cfg.RDBFilename, entries)
}

// RewriteAOF compacts history into a logical snapshot command stream. The
// persistence lock makes snapshot -> BeginRewrite a gap-free boundary; later
// mutations are appended to the old AOF and buffered into the new one.
func (s *Server) RewriteAOF() error {
	if s.aof == nil {
		return errors.New("AOF is disabled")
	}
	snapshotter, err := s.snapshotter()
	if err != nil {
		return err
	}

	s.persistenceMu.Lock()
	entries, err := snapshotter.Snapshot()
	if err == nil {
		err = s.aof.BeginRewrite()
	}
	s.persistenceMu.Unlock()
	if err != nil {
		return err
	}

	commands, err := persistence.Commands(entries)
	if err != nil {
		s.aof.AbortRewrite()
		return err
	}
	if err := s.aof.FinishRewrite(commands); err != nil {
		s.aof.AbortRewrite()
		return err
	}
	return nil
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
	s.persistenceMu.Lock()
	defer s.persistenceMu.Unlock()

	result := s.registry.Execute(&command.Context{Store: s.store}, argv)
	if s.aof != nil {
		persist := result.AOFCommands(time.Now())
		if len(persist) > 0 {
			// Append before acknowledging the mutation. v0.5 additionally keeps
			// this command mutation + append inside persistenceMu relative to AOF
			// rewrite snapshot boundaries.
			if err := s.aof.AppendBatch(persist); err != nil {
				s.logger.Error("AOF append failed", "error", err)
				return resp.Error("MISCONF AOF persistence failed")
			}
		}
	}
	return result.Reply
}

func (s *Server) runMaintenance() {
	s.store.ExpireDue(s.cfg.ActiveExpireSample)
	if s.cfg.RehashWorkPerCycle <= 0 {
		return
	}
	if stepper, ok := s.store.(store.RehashStepper); ok {
		stepper.RehashStep(s.cfg.RehashWorkPerCycle)
	}
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
