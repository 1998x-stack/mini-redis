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
	"sync/atomic"
	"time"

	"github.com/1998x-stack/mini-redis/internal/aof"
	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/persistence"
	"github.com/1998x-stack/mini-redis/internal/rdb"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/memorypolicy"
)

type Server struct {
	cfg      config.Config
	store    store.Store
	registry *command.Registry
	aof      *aof.Log
	logger   *slog.Logger

	// persistenceMu names the v0.5 state/AOF boundary. v0.6 also keeps one
	// command, its memory-pressure decision and eviction DEL persistence inside
	// this boundary. v0.7 EXEC holds it across WATCH validation and the entire
	// queued command sequence, so other clients cannot interleave mutations.
	persistenceMu sync.Mutex

	versions   *keyVersionTable
	hub        *pubSubHub
	sessionSeq atomic.Uint64

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

	s := &Server{
		cfg:      cfg,
		store:    st,
		registry: registry,
		logger:   logger,
		versions: newKeyVersionTable(),
		hub:      newPubSubHub(),
	}

	// Recovery happens before installing the runtime memory-policy wrapper. LRU
	// and LFU heat are deliberately not reconstructed from AOF history or RDB.
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

	if cfg.MaxMemory > 0 {
		managed, err := memorypolicy.New(s.store, cfg.MemoryConfig(store.RealClock{}))
		if err != nil {
			if s.aof != nil {
				_ = s.aof.Close()
			}
			return nil, fmt.Errorf("configure memory policy: %w", err)
		}
		s.store = managed
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
	return s.executeCommandLocked(argv)
}

// executeCommandLocked runs one command while persistenceMu is already held.
// Normal requests enter through executeCommand; EXEC uses this form repeatedly
// so its queued commands are ordered as one non-interleaved server unit.
func (s *Server) executeCommandLocked(argv []string) resp.Value {
	var (
		controller    store.MemoryController
		rollbackStore store.Snapshotter
		rollbackState []store.SnapshotEntry
		memoryManaged bool
	)
	if c, ok := s.store.(store.MemoryController); ok {
		controller = c
		memoryManaged = c.MemoryPolicyConfig().MaxBytes > 0
	}

	mayGrow := len(argv) > 0 && s.registry.MayGrowMemory(argv[0])
	policy := store.PolicyNoEviction
	if memoryManaged {
		policy = controller.MemoryPolicyConfig().Policy
	}

	if memoryManaged && policy == store.PolicyNoEviction && mayGrow {
		var err error
		rollbackStore, err = s.snapshotter()
		if err != nil {
			return resp.Error("ERR memory policy requires logical snapshots")
		}
		rollbackState, err = rollbackStore.Snapshot()
		if err != nil {
			return resp.Error("ERR failed to capture memory rollback state")
		}
	}

	result := s.registry.Execute(&command.Context{Store: s.store}, argv)
	persist := result.AOFCommands(time.Now())
	var evicted []string

	if memoryManaged && result.Runnable && result.Spec.Mutating && !result.Reply.IsError() {
		shouldEnforce := policy != store.PolicyNoEviction || mayGrow
		if shouldEnforce {
			pressure, err := controller.EnforceMaxMemory()
			if err != nil {
				if rollbackStore != nil {
					if restoreErr := rollbackStore.RestoreSnapshot(rollbackState); restoreErr != nil {
						s.logger.Error("restore after maxmemory failure", "error", restoreErr)
						return resp.Error("MISCONF failed to rollback maxmemory rejection")
					}
				}
				if errors.Is(err, store.ErrOOM) {
					return resp.Error("OOM command not allowed when used memory > 'maxmemory'")
				}
				s.logger.Error("maxmemory enforcement failed", "error", err)
				return resp.Error("ERR maxmemory enforcement failed")
			}
			if len(pressure.EvictedKeys) > 0 {
				evicted = append(evicted, pressure.EvictedKeys...)
				del := append([]string{"DEL"}, pressure.EvictedKeys...)
				persist = append(persist, del)
			}
		}
	}

	acceptedMutation := result.Runnable && result.Spec.Mutating && !result.Reply.IsError()
	if acceptedMutation {
		s.noteAcceptedMutation(argv, evicted)
	}

	if s.aof != nil && len(persist) > 0 {
		if err := s.aof.AppendBatch(persist); err != nil {
			s.logger.Error("AOF append failed", "error", err)
			return resp.Error("MISCONF AOF persistence failed")
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
