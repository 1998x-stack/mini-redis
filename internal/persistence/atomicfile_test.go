package persistence

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAtomicWriteFailureBeforeRenamePreservesOldFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.rdb")
	if err := os.WriteFile(path, []byte("old-valid-state"), 0o644); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected crash window")
	err := writeFileAtomic(path, []byte("new-state"), 0o644, func(_, _ string) error {
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("err=%v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old-valid-state" {
		t.Fatalf("old file changed: %q", got)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".dump.rdb.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files leaked: %v", matches)
	}
}

func TestAtomicWritePublishesCompleteNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")
	if err := WriteFileAtomic(path, []byte("complete"), 0o640); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "complete" {
		t.Fatalf("got %q", got)
	}
}
