package aof

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/resp"
)

func TestReplayIgnoresTruncatedFinalCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	complete := resp.EncodeCommand([]string{"SET", "safe", "1"})
	truncated := resp.EncodeCommand([]string{"SET", "partial", "2"})
	truncated = truncated[:len(truncated)-3]
	if err := os.WriteFile(path, append(complete, truncated...), 0o644); err != nil {
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
	want := [][]string{{"SET", "safe", "1"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
}
