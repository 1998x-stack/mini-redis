package server

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func newEvictionServer(t *testing.T, cfg config.Config, st store.Store) *Server {
	t.Helper()
	registry := command.NewRegistry()
	command.RegisterCore(registry)
	srv, err := New(cfg, st, registry, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func stringValue(t *testing.T, st store.Store, key string) (string, bool) {
	t.Helper()
	obj, ok := st.Get(key)
	if !ok {
		return "", false
	}
	value, ok := obj.StringValue()
	if !ok {
		t.Fatalf("%s is not a string", key)
	}
	return value, true
}

func TestNoEvictionRollbackLeavesStateAndAOFUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := config.Default()
	cfg.AppendOnly = true
	cfg.AppendFilename = path
	cfg.AppendFsync = "always"
	cfg.MaxMemory = 250
	cfg.MaxMemoryPolicy = store.PolicyNoEviction

	base := versionedmemory.New(store.RealClock{})
	srv := newEvictionServer(t, cfg, base)
	if reply := srv.executeCommand([]string{"SET", "stable", strings.Repeat("s", 20)}); reply.IsError() {
		t.Fatalf("initial SET failed: %#v", reply)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	reply := srv.executeCommand([]string{"SET", "huge", strings.Repeat("x", 500)})
	if !reply.IsError() {
		t.Fatalf("expected OOM, got %#v", reply)
	}
	if _, ok := stringValue(t, base, "huge"); ok {
		t.Fatal("rejected key remained in memory")
	}
	if value, ok := stringValue(t, base, "stable"); !ok || value != strings.Repeat("s", 20) {
		t.Fatalf("stable=%q exists=%v", value, ok)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() {
		t.Fatalf("rejected command changed AOF size: before=%d after=%d", before.Size(), after.Size())
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart with maxmemory disabled: only the acknowledged stable SET may be
	// reconstructed from AOF.
	restartCfg := cfg
	restartCfg.MaxMemory = 0
	restored := versionedmemory.New(store.RealClock{})
	srv2 := newEvictionServer(t, restartCfg, restored)
	defer srv2.Close()
	if _, ok := stringValue(t, restored, "huge"); ok {
		t.Fatal("rejected key appeared after AOF replay")
	}
	if value, ok := stringValue(t, restored, "stable"); !ok || value != strings.Repeat("s", 20) {
		t.Fatalf("replayed stable=%q exists=%v", value, ok)
	}
}

func TestEvictionDELIsPersistedForRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := config.Default()
	cfg.AppendOnly = true
	cfg.AppendFilename = path
	cfg.AppendFsync = "always"
	cfg.MaxMemory = 250
	cfg.MaxMemoryPolicy = store.PolicyAllKeysRandom
	cfg.EvictionSeed = 1

	base := versionedmemory.New(store.RealClock{})
	srv := newEvictionServer(t, cfg, base)
	valueA := strings.Repeat("a", 100)
	valueB := strings.Repeat("b", 100)
	if reply := srv.executeCommand([]string{"SET", "a", valueA}); reply.IsError() {
		t.Fatalf("SET a failed: %#v", reply)
	}
	if reply := srv.executeCommand([]string{"SET", "b", valueB}); reply.IsError() {
		t.Fatalf("SET b failed: %#v", reply)
	}
	if base.Len() != 1 {
		t.Fatalf("len=%d want 1 after pressure eviction", base.Len())
	}
	remaining := ""
	if _, ok := base.Get("a"); ok {
		remaining = "a"
	}
	if _, ok := base.Get("b"); ok {
		if remaining != "" {
			t.Fatal("both keys survived pressure")
		}
		remaining = "b"
	}
	if remaining == "" {
		t.Fatal("both keys were evicted")
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}

	restartCfg := cfg
	restartCfg.MaxMemory = 0
	restored := versionedmemory.New(store.RealClock{})
	srv2 := newEvictionServer(t, restartCfg, restored)
	defer srv2.Close()
	if restored.Len() != 1 {
		t.Fatalf("replayed len=%d want 1", restored.Len())
	}
	if _, ok := restored.Get(remaining); !ok {
		t.Fatalf("survivor %q missing after replay", remaining)
	}
	other := "a"
	if remaining == "a" {
		other = "b"
	}
	if _, ok := restored.Get(other); ok {
		t.Fatalf("evicted key %q reappeared after replay", other)
	}
}

func TestNoEvictionAllowsMemoryReducingCommandAboveLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := config.Default()
	cfg.AppendOnly = true
	cfg.AppendFilename = path
	cfg.AppendFsync = "always"
	cfg.MaxMemory = 1
	cfg.MaxMemoryPolicy = store.PolicyNoEviction

	// Seed an already-over-limit dataset through AOF replay. Reads and
	// removal-only writes must remain usable so an operator can recover.
	payload := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n"
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	base := versionedmemory.New(store.RealClock{})
	srv := newEvictionServer(t, cfg, base)
	defer srv.Close()
	if reply := srv.executeCommand([]string{"GET", "k"}); reply.IsError() {
		t.Fatalf("read should work above limit: %#v", reply)
	}
	if reply := srv.executeCommand([]string{"DEL", "k"}); reply.IsError() {
		t.Fatalf("DEL should work above noeviction limit: %#v", reply)
	}
	if base.Len() != 0 {
		t.Fatalf("len=%d want 0", base.Len())
	}
}
