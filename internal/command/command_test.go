package command

import (
	"strings"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func testRegistry() (*Registry, *Context) {
	r := NewRegistry()
	RegisterCore(r)
	return r, &Context{Store: versionedmemory.New(store.RealClock{})}
}

func TestSetGetIncr(t *testing.T) {
	r, ctx := testRegistry()
	if got := r.Execute(ctx, []string{"SET", "n", "41"}).Reply; got.IsError() {
		t.Fatalf("SET failed: %#v", got)
	}
	inc := r.Execute(ctx, []string{"INCR", "n"}).Reply
	if inc.Int != 42 {
		t.Fatalf("INCR=%d want 42", inc.Int)
	}
	get := r.Execute(ctx, []string{"GET", "n"}).Reply
	if get.Str != "42" {
		t.Fatalf("GET=%q want 42", get.Str)
	}
}

func TestWrongArity(t *testing.T) {
	r, ctx := testRegistry()
	got := r.Execute(ctx, []string{"GET"})
	if !got.Reply.IsError() || got.Runnable {
		t.Fatalf("unexpected execution %#v", got)
	}
}

func TestSetExpiryAOFUsesAbsoluteDeadline(t *testing.T) {
	r, ctx := testRegistry()
	exec := r.Execute(ctx, []string{"SET", "session", "abc", "EX", "10"})
	if exec.Reply.IsError() {
		t.Fatalf("SET failed: %#v", exec.Reply)
	}
	now := time.Unix(100, 0)
	commands := exec.AOFCommands(now)
	if len(commands) != 2 {
		t.Fatalf("commands=%#v", commands)
	}
	if got := strings.Join(commands[0], " "); got != "SET session abc" {
		t.Fatalf("first command=%q", got)
	}
	if got := strings.Join(commands[1], " "); got != "PEXPIREAT session 110000" {
		t.Fatalf("second command=%q", got)
	}
}
