package aof

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func replayCommands(t *testing.T, log *Log) [][]string {
	t.Helper()
	var got [][]string
	if err := log.Replay(func(argv []string) error {
		got = append(got, append([]string(nil), argv...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestRewritePublishesCompactBasePlusBufferedMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	for i := 0; i < 10; i++ {
		if err := log.Append([]string{"SET", "x", string(rune('0' + i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.BeginRewrite(); err != nil {
		t.Fatal(err)
	}
	if err := log.Append([]string{"INCR", "counter"}); err != nil {
		t.Fatal(err)
	}
	base := [][]string{{"SET", "x", "9"}, {"SET", "counter", "0"}}
	if err := log.FinishRewrite(base); err != nil {
		t.Fatal(err)
	}

	want := [][]string{{"SET", "x", "9"}, {"SET", "counter", "0"}, {"INCR", "counter"}}
	if got := replayCommands(t, log); !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
}

func TestInterruptedRewriteLeavesOldAOFValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	old := [][]string{{"SET", "x", "1"}, {"INCR", "x"}}
	for _, cmd := range old {
		if err := log.Append(cmd); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := log.BeginRewrite(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("crash before rename")
	err = log.finishRewrite([][]string{{"SET", "x", "2"}}, func(_, _ string) error { return injected })
	if !errors.Is(err, injected) {
		t.Fatalf("err=%v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("old AOF changed before atomic publication")
	}
	if got := replayCommands(t, log); !reflect.DeepEqual(got, old) {
		t.Fatalf("old AOF no longer replays: %#v", got)
	}
}

func TestBeginRewriteRejectsOverlap(t *testing.T) {
	log, err := Open(filepath.Join(t.TempDir(), "aof"), FsyncNo)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if err := log.BeginRewrite(); err != nil {
		t.Fatal(err)
	}
	if err := log.BeginRewrite(); !errors.Is(err, ErrRewriteInProgress) {
		t.Fatalf("err=%v", err)
	}
	log.AbortRewrite()
}
