package server

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func newSessionTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.AppendOnly = false
	cfg.RDBFilename = ""
	registry := command.NewRegistry()
	command.RegisterCore(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := New(cfg, versionedmemory.New(store.RealClock{}), registry, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func oneReply(t *testing.T, srv *Server, session *clientSession, argv ...string) resp.Value {
	t.Helper()
	replies := srv.executeClientCommand(session, argv)
	if len(replies) != 1 {
		t.Fatalf("%v returned %d replies: %#v", argv, len(replies), replies)
	}
	return replies[0]
}

func TestMultiQueuesThenExecutesInOrder(t *testing.T) {
	srv := newSessionTestServer(t)
	client := srv.newClientSession(nil)

	if got := oneReply(t, srv, client, "MULTI"); got.Str != "OK" {
		t.Fatalf("MULTI=%#v", got)
	}
	if got := oneReply(t, srv, client, "SET", "k", "v"); got.Str != "QUEUED" {
		t.Fatalf("SET queue=%#v", got)
	}
	if got := oneReply(t, srv, client, "GET", "k"); got.Str != "QUEUED" {
		t.Fatalf("GET queue=%#v", got)
	}

	exec := oneReply(t, srv, client, "EXEC")
	if exec.Type != resp.ArrayType || len(exec.Array) != 2 {
		t.Fatalf("EXEC=%#v", exec)
	}
	if exec.Array[0].Str != "OK" || exec.Array[1].Str != "v" {
		t.Fatalf("EXEC replies=%#v", exec.Array)
	}
}

func TestExecRuntimeErrorDoesNotRollbackOtherCommands(t *testing.T) {
	srv := newSessionTestServer(t)
	client := srv.newClientSession(nil)
	oneReply(t, srv, client, "SET", "n", "not-an-int")
	oneReply(t, srv, client, "MULTI")
	oneReply(t, srv, client, "INCR", "n")
	oneReply(t, srv, client, "SET", "survivor", "yes")

	exec := oneReply(t, srv, client, "EXEC")
	if exec.Type != resp.ArrayType || len(exec.Array) != 2 {
		t.Fatalf("EXEC=%#v", exec)
	}
	if !exec.Array[0].IsError() || exec.Array[1].Str != "OK" {
		t.Fatalf("runtime-error transaction replies=%#v", exec.Array)
	}
	if got := oneReply(t, srv, client, "GET", "survivor"); got.Str != "yes" {
		t.Fatalf("survivor=%#v", got)
	}
}

func TestQueueTimeErrorAbortsTransaction(t *testing.T) {
	srv := newSessionTestServer(t)
	client := srv.newClientSession(nil)
	oneReply(t, srv, client, "MULTI")
	if got := oneReply(t, srv, client, "NO_SUCH_COMMAND"); !got.IsError() {
		t.Fatalf("expected queue-time error, got %#v", got)
	}
	oneReply(t, srv, client, "SET", "k", "v")

	exec := oneReply(t, srv, client, "EXEC")
	if !exec.IsError() || exec.Str != "EXECABORT Transaction discarded because of previous errors." {
		t.Fatalf("EXEC after dirty queue=%#v", exec)
	}
	if got := oneReply(t, srv, client, "GET", "k"); got.Type != resp.NullBulkStringType {
		t.Fatalf("dirty transaction leaked write: %#v", got)
	}
}

func TestWatchConflictAndUnrelatedWrite(t *testing.T) {
	srv := newSessionTestServer(t)
	watcher := srv.newClientSession(nil)
	writer := srv.newClientSession(nil)

	oneReply(t, srv, writer, "SET", "watched", "v1")
	oneReply(t, srv, watcher, "WATCH", "watched")
	oneReply(t, srv, writer, "SET", "other", "x")
	oneReply(t, srv, watcher, "MULTI")
	oneReply(t, srv, watcher, "GET", "watched")
	okExec := oneReply(t, srv, watcher, "EXEC")
	if okExec.Type != resp.ArrayType || okExec.Array[0].Str != "v1" {
		t.Fatalf("unrelated write aborted WATCH: %#v", okExec)
	}

	oneReply(t, srv, watcher, "WATCH", "watched")
	oneReply(t, srv, writer, "SET", "watched", "v2")
	oneReply(t, srv, writer, "SET", "watched", "v1")
	oneReply(t, srv, watcher, "MULTI")
	oneReply(t, srv, watcher, "GET", "watched")
	conflict := oneReply(t, srv, watcher, "EXEC")
	if conflict.Type != resp.NullArrayType {
		t.Fatalf("change-back should still conflict, got %#v", conflict)
	}
}

func TestWatchNoticesExpirationWithoutExplicitWriteVersion(t *testing.T) {
	srv := newSessionTestServer(t)
	client := srv.newClientSession(nil)
	oneReply(t, srv, client, "SET", "short", "v", "PX", "1")
	oneReply(t, srv, client, "WATCH", "short")
	time.Sleep(3 * time.Millisecond)
	oneReply(t, srv, client, "MULTI")
	oneReply(t, srv, client, "GET", "short")
	if got := oneReply(t, srv, client, "EXEC"); got.Type != resp.NullArrayType {
		t.Fatalf("expiry should invalidate WATCH, got %#v", got)
	}
}

func TestPubSubFanoutAndSubscribedMode(t *testing.T) {
	srv := newSessionTestServer(t)
	var gotA, gotB []resp.Value
	a := srv.newClientSession(func(v resp.Value) error { gotA = append(gotA, v); return nil })
	b := srv.newClientSession(func(v resp.Value) error { gotB = append(gotB, v); return nil })
	publisher := srv.newClientSession(nil)

	if replies := srv.executeClientCommand(a, []string{"SUBSCRIBE", "news"}); len(replies) != 1 || replies[0].Array[2].Int != 1 {
		t.Fatalf("subscribe A=%#v", replies)
	}
	if replies := srv.executeClientCommand(b, []string{"SUBSCRIBE", "news"}); len(replies) != 1 || replies[0].Array[2].Int != 1 {
		t.Fatalf("subscribe B=%#v", replies)
	}
	if got := oneReply(t, srv, publisher, "PUBLISH", "news", "hello"); got.Int != 2 {
		t.Fatalf("PUBLISH recipients=%#v", got)
	}
	if len(gotA) != 1 || len(gotB) != 1 {
		t.Fatalf("fanout A=%#v B=%#v", gotA, gotB)
	}
	msg := gotA[0]
	if msg.Type != resp.ArrayType || len(msg.Array) != 3 || msg.Array[0].Str != "message" || msg.Array[1].Str != "news" || msg.Array[2].Str != "hello" {
		t.Fatalf("message=%#v", msg)
	}
	if got := oneReply(t, srv, a, "SET", "blocked", "1"); !got.IsError() {
		t.Fatalf("subscribed client executed normal command: %#v", got)
	}

	if replies := srv.executeClientCommand(a, []string{"UNSUBSCRIBE", "news"}); len(replies) != 1 || replies[0].Array[2].Int != 0 {
		t.Fatalf("unsubscribe A=%#v", replies)
	}
	if got := oneReply(t, srv, publisher, "PUBLISH", "news", "again"); got.Int != 1 {
		t.Fatalf("recipient count after unsubscribe=%#v", got)
	}
}
