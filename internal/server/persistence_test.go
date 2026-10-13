package server

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/rdb"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func newPersistenceServer(t *testing.T, cfg config.Config, st store.Store) *Server {
	t.Helper()
	registry := command.NewRegistry()
	command.RegisterCore(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := New(cfg, st, registry, logger)
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestRDBStartupWhenAOFDisabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.mrdb")
	source := versionedmemory.New(store.RealClock{})
	source.Set("str", store.NewString("001"), nil)
	_, _ = source.ListPush("list", false, []string{"a", "b"})
	_, _ = source.HashSet("hash", []store.FieldValue{{Field: "name", Value: "mini"}})
	_, _ = source.SetAdd("set", "1", "2")
	_, _ = source.ZAdd("z", []store.ScoredMember{{Member: "alice", Score: 1.5}})
	if !source.ExpireAt("list", time.Now().Add(time.Hour)) {
		t.Fatal("expire")
	}
	entries, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.SaveFile(path, entries); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.AppendOnly = false
	cfg.RDBFilename = path
	restored := versionedmemory.New(store.RealClock{})
	srv := newPersistenceServer(t, cfg, restored)
	defer srv.Close()
	if restored.Len() != 5 {
		t.Fatalf("len=%d", restored.Len())
	}
	obj, ok := restored.Get("str")
	if !ok {
		t.Fatal("missing str")
	}
	value, _ := obj.StringValue()
	if value != "001" {
		t.Fatalf("str=%q", value)
	}
	if values, err := restored.ListRange("list", 0, -1); err != nil || len(values) != 2 || values[1] != "b" {
		t.Fatalf("list=%v err=%v", values, err)
	}
}

func TestAOFIsAuthoritativeWhenBothFilesExist(t *testing.T) {
	dir := t.TempDir()
	rdbPath := filepath.Join(dir, "dump.mrdb")
	aofPath := filepath.Join(dir, "appendonly.aof")
	if err := rdb.SaveFile(rdbPath, []store.SnapshotEntry{{Key: "source", Type: store.ObjectString, StringValue: "rdb"}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(aofPath, resp.EncodeCommand([]string{"SET", "source", "aof"}), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.AppendOnly = true
	cfg.AppendFilename = aofPath
	cfg.RDBFilename = rdbPath
	st := versionedmemory.New(store.RealClock{})
	srv := newPersistenceServer(t, cfg, st)
	defer srv.Close()
	obj, ok := st.Get("source")
	if !ok {
		t.Fatal("missing source")
	}
	value, _ := obj.StringValue()
	if value != "aof" {
		t.Fatalf("source=%q", value)
	}
}

func TestCorruptRDBFailsStartupWhenAOFDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.mrdb")
	if err := os.WriteFile(path, []byte("broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.AppendOnly = false
	cfg.RDBFilename = path
	registry := command.NewRegistry()
	command.RegisterCore(registry)
	_, err := New(cfg, versionedmemory.New(store.RealClock{}), registry, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("expected corrupt RDB startup failure")
	}
}

func TestRewriteAOFCompactsHistoryAndRestartsCorrectly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := config.Default()
	cfg.AppendOnly = true
	cfg.AppendFilename = path
	cfg.AppendFsync = "always"
	st := versionedmemory.New(store.RealClock{})
	srv := newPersistenceServer(t, cfg, st)

	for i := 0; i < 25; i++ {
		reply := srv.executeCommand([]string{"SET", "x", string(rune('a' + i%26))})
		if reply.IsError() {
			t.Fatalf("SET failed: %#v", reply)
		}
	}
	if reply := srv.executeCommand([]string{"RPUSH", "list", "a", "b", "c"}); reply.IsError() {
		t.Fatalf("RPUSH failed: %#v", reply)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("rewrite did not compact history: before=%d after=%d", before.Size(), after.Size())
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}

	restored := versionedmemory.New(store.RealClock{})
	srv2 := newPersistenceServer(t, cfg, restored)
	defer srv2.Close()
	obj, ok := restored.Get("x")
	if !ok {
		t.Fatal("missing x")
	}
	value, _ := obj.StringValue()
	if value != "y" {
		t.Fatalf("x=%q want y", value)
	}
	values, err := restored.ListRange("list", 0, -1)
	if err != nil || len(values) != 3 || values[2] != "c" {
		t.Fatalf("list=%v err=%v", values, err)
	}
}
