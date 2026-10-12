package aof

import (
	"path/filepath"
	"reflect"
	"testing"
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
