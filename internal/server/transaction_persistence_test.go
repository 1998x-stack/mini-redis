package server

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/aof"
	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func newAOFSessionServer(t *testing.T, cfg config.Config) *Server {
	t.Helper()
	registry := command.NewRegistry()
	command.RegisterCore(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := New(cfg, versionedmemory.New(store.RealClock{}), registry, logger)
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestExecPersistsAsTransactionFrameAndRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := config.Default()
	cfg.AppendOnly = true
	cfg.AppendFilename = path
	cfg.AppendFsync = aof.FsyncAlways

	first := newAOFSessionServer(t, cfg)
	client := first.newClientSession(nil)
	oneReply(t, first, client, "MULTI")
	oneReply(t, first, client, "SET", "a", "1")
	oneReply(t, first, client, "SET", "b", "2")
	exec := oneReply(t, first, client, "EXEC")
	if exec.IsError() || len(exec.Array) != 2 {
		t.Fatalf("EXEC=%#v", exec)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, "MULTI") || !strings.Contains(text, "EXEC") {
		t.Fatalf("AOF transaction framing missing: %q", text)
	}

	second := newAOFSessionServer(t, cfg)
	defer second.Close()
	reader := second.newClientSession(nil)
	if got := oneReply(t, second, reader, "GET", "a"); got.Str != "1" {
		t.Fatalf("replayed a=%#v", got)
	}
	if got := oneReply(t, second, reader, "GET", "b"); got.Str != "2" {
		t.Fatalf("replayed b=%#v", got)
	}
}
