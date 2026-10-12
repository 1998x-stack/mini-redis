package command

import (
	"strings"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/resp"
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

func TestObjectEncodingShowsPhysicalStringRepresentation(t *testing.T) {
	r, ctx := testRegistry()
	for _, tc := range []struct {
		value string
		want  string
	}{
		{"42", "int"},
		{"001", "raw"},
		{"hello", "raw"},
	} {
		if reply := r.Execute(ctx, []string{"SET", "k", tc.value}).Reply; reply.IsError() {
			t.Fatalf("SET %q failed: %#v", tc.value, reply)
		}
		enc := r.Execute(ctx, []string{"OBJECT", "ENCODING", "k"}).Reply
		if enc.Str != tc.want {
			t.Fatalf("OBJECT ENCODING after SET %q = %q want %q", tc.value, enc.Str, tc.want)
		}
		get := r.Execute(ctx, []string{"GET", "k"}).Reply
		if get.Str != tc.value {
			t.Fatalf("GET after encoding %q = %q", tc.value, get.Str)
		}
	}
}

func TestIncrConvertsRawNumericStringToIntEncoding(t *testing.T) {
	r, ctx := testRegistry()
	r.Execute(ctx, []string{"SET", "n", "001"})
	before := r.Execute(ctx, []string{"OBJECT", "ENCODING", "n"}).Reply
	if before.Str != "raw" {
		t.Fatalf("before encoding=%q", before.Str)
	}
	inc := r.Execute(ctx, []string{"INCR", "n"}).Reply
	if inc.Int != 2 {
		t.Fatalf("INCR=%d", inc.Int)
	}
	after := r.Execute(ctx, []string{"OBJECT", "ENCODING", "n"}).Reply
	if after.Str != "int" {
		t.Fatalf("after encoding=%q", after.Str)
	}
	get := r.Execute(ctx, []string{"GET", "n"}).Reply
	if get.Str != "2" {
		t.Fatalf("GET=%q", get.Str)
	}
}

func TestObjectEncodingMissingAndSyntaxErrors(t *testing.T) {
	r, ctx := testRegistry()
	missing := r.Execute(ctx, []string{"OBJECT", "ENCODING", "missing"}).Reply
	if missing.Type != resp.NullBulkStringType {
		t.Fatalf("missing OBJECT ENCODING reply=%#v", missing)
	}
	bad := r.Execute(ctx, []string{"OBJECT", "NOPE", "k"}).Reply
	if !bad.IsError() {
		t.Fatalf("expected syntax error, got %#v", bad)
	}
}
