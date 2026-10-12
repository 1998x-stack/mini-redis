package aof

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/1998x-stack/mini-redis/internal/resp"
)

type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverySec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

type Log struct {
	mu     sync.Mutex
	file   *os.File
	policy FsyncPolicy
	dirty  bool
	cancel context.CancelFunc
	wg     sync.WaitGroup
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
	l := &Log{file: file, policy: policy}

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

// AppendBatch writes a logical mutation as one OS write. This matters for AOF
// rewrites such as SET ... EX, which persists as SET + PEXPIREAT.
func (l *Log) AppendBatch(commands [][]string) error {
	var payload []byte
	for _, argv := range commands {
		payload = append(payload, resp.EncodeCommand(argv)...)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.file.Write(payload); err != nil {
		return err
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
	closeErr := l.file.Close()
	l.mu.Unlock()
	return errors.Join(syncErr, closeErr)
}

// Replay reuses the normal RESP parser. A truncated final command is ignored,
// which is a pragmatic v0.1 recovery rule for crash-damaged tails.
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
