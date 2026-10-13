package aof

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/resp"
)

func TestAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	commands := [][]string{{"SET", "a", "1"}, {"INCR", "a"}}
	for _, cmd := range commands {
		if err := log.Append(cmd); err != nil {
			t.Fatal(err)
		}
	}

	var got [][]string
	if err := log.Replay(func(argv []string) error {
		got = append(got, append([]string(nil), argv...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, commands) {
		t.Fatalf("got %#v want %#v", got, commands)
	}
}

func TestAppendTransactionReplaysCompleteFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	commands := [][]string{{"SET", "a", "1"}, {"INCR", "a"}, {"SET", "b", "2"}}
	if err := log.AppendTransaction(commands); err != nil {
		t.Fatal(err)
	}

	var got [][]string
	if err := log.Replay(func(argv []string) error {
		got = append(got, append([]string(nil), argv...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, commands) {
		t.Fatalf("replayed %#v want %#v", got, commands)
	}
}

func TestReplayDiscardsTruncatedTransactionTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	payload := append([]byte{}, resp.EncodeCommand([]string{"SET", "base", "1"})...)
	payload = append(payload, resp.EncodeCommand([]string{"MULTI"})...)
	payload = append(payload, resp.EncodeCommand([]string{"SET", "half", "1"})...)
	payload = append(payload, resp.EncodeCommand([]string{"INCR", "half"})...)
	// No EXEC: the transaction is a crash-truncated tail and must be ignored.
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	log, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	var got [][]string
	if err := log.Replay(func(argv []string) error {
		got = append(got, append([]string(nil), argv...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"SET", "base", "1"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed %#v want %#v", got, want)
	}
}

func TestReplayRejectsExecWithoutMulti(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	if err := os.WriteFile(path, resp.EncodeCommand([]string{"EXEC"}), 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if err := log.Replay(func([]string) error { return nil }); err == nil {
		t.Fatal("expected EXEC without MULTI to be rejected")
	}
}
