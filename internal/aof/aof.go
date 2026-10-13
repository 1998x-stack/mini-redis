package aof

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/persistence"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverySec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

var (
	ErrRewriteInProgress = errors.New("AOF rewrite already in progress")
	ErrRewriteNotStarted = errors.New("AOF rewrite not started")
)

type Log struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	policy FsyncPolicy
	dirty  bool
	cancel context.CancelFunc
	wg     sync.WaitGroup

	rewriteActive bool
	rewriteBuffer []byte
}

func Open(path string, policy FsyncPolicy) (*Log, error) {
	switch policy {
	case FsyncAlways, FsyncEverySec, FsyncNo:
	default:
		return nil, fmt.Errorf("unknown fsync policy %q", policy)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{file: file, path: path, policy: policy}

	if policy == FsyncEverySec {
		ctx, cancel := context.WithCancel(context.Background())
		l.cancel = cancel
		l.wg.Add(1)
		go l.syncLoop(ctx)
	}
	return l, nil
}

func (l *Log) Append(argv []string) error {
	return l.AppendBatch([][]string{argv})
}

func encodeCommands(commands [][]string) []byte {
	var payload []byte
	for _, argv := range commands {
		payload = append(payload, resp.EncodeCommand(argv)...)
	}
	return payload
}

// AppendBatch writes a logical mutation to the current AOF. During rewrite it
// also buffers the exact bytes so mutations after the snapshot boundary can be
// appended to the replacement file before publication.
func (l *Log) AppendBatch(commands [][]string) error {
	payload := encodeCommands(commands)

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.file.Write(payload); err != nil {
		return err
	}
	if l.rewriteActive {
		l.rewriteBuffer = append(l.rewriteBuffer, payload...)
	}
	l.dirty = true
	if l.policy == FsyncAlways {
		if err := l.file.Sync(); err != nil {
			return err
		}
		l.dirty = false
	}
	return nil
}

// BeginRewrite establishes the buffer boundary. The caller must coordinate its
// state snapshot so no mutation can occur between taking that snapshot and
// calling BeginRewrite.
func (l *Log) BeginRewrite() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rewriteActive {
		return ErrRewriteInProgress
	}
	l.rewriteActive = true
	l.rewriteBuffer = nil
	return nil
}

func (l *Log) AbortRewrite() {
	l.mu.Lock()
	l.rewriteActive = false
	l.rewriteBuffer = nil
	l.mu.Unlock()
}

func (l *Log) FinishRewrite(commands [][]string) error {
	return l.finishRewrite(commands, nil)
}

type beforeRewriteRenameHook func(tempPath, targetPath string) error

// finishRewrite writes the compact base outside the AOF lock. The short commit
// phase then appends buffered mutations, fsyncs, atomically renames the temp
// file, and swaps the live descriptor while appends are blocked.
func (l *Log) finishRewrite(commands [][]string, beforeRename beforeRewriteRenameHook) error {
	base := encodeCommands(commands)
	dir := filepath.Dir(l.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		l.AbortRewrite()
		return err
	}
	temp, err := os.CreateTemp(dir, "."+filepath.Base(l.path)+".rewrite-*")
	if err != nil {
		l.AbortRewrite()
		return err
	}
	tempPath := temp.Name()
	published := false
	defer func() {
		if !published {
			_ = temp.Close()
			_ = os.Remove(tempPath)
		}
	}()

	if err := temp.Chmod(0o644); err != nil {
		l.AbortRewrite()
		return err
	}
	if _, err := temp.Write(base); err != nil {
		l.AbortRewrite()
		return err
	}
	if err := temp.Sync(); err != nil {
		l.AbortRewrite()
		return err
	}

	l.mu.Lock()
	if !l.rewriteActive {
		l.mu.Unlock()
		return ErrRewriteNotStarted
	}
	if len(l.rewriteBuffer) > 0 {
		if _, err := temp.Write(l.rewriteBuffer); err != nil {
			l.rewriteActive = false
			l.rewriteBuffer = nil
			l.mu.Unlock()
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		l.rewriteActive = false
		l.rewriteBuffer = nil
		l.mu.Unlock()
		return err
	}
	if beforeRename != nil {
		if err := beforeRename(tempPath, l.path); err != nil {
			l.rewriteActive = false
			l.rewriteBuffer = nil
			l.mu.Unlock()
			return err
		}
	}
	if err := os.Rename(tempPath, l.path); err != nil {
		l.rewriteActive = false
		l.rewriteBuffer = nil
		l.mu.Unlock()
		return err
	}
	published = true
	old := l.file
	l.file = temp
	l.dirty = false
	l.rewriteActive = false
	l.rewriteBuffer = nil
	l.mu.Unlock()

	if err := persistence.SyncDir(dir); err != nil {
		_ = old.Close()
		return err
	}
	return old.Close()
}

func (l *Log) syncLoop(ctx context.Context) {
	defer l.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = l.Sync()
		}
	}
}

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.dirty {
		return nil
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	l.dirty = false
	return nil
}

func (l *Log) Close() error {
	if l.cancel != nil {
		l.cancel()
		l.wg.Wait()
	}
	syncErr := l.Sync()

	l.mu.Lock()
	l.rewriteActive = false
	l.rewriteBuffer = nil
	closeErr := l.file.Close()
	l.mu.Unlock()
	return errors.Join(syncErr, closeErr)
}

// Replay reuses the normal RESP parser. A truncated final command is ignored,
// which remains the explicit v0.5 startup policy for crash-damaged AOF tails.
func (l *Log) Replay(apply func([]string) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := resp.NewReader(l.file)
	for {
		argv, err := reader.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "expected CRLF") {
				break
			}
			return fmt.Errorf("replay AOF: %w", err)
		}
		if err := apply(argv); err != nil {
			return err
		}
	}

	_, err := l.file.Seek(0, io.SeekEnd)
	return err
}
