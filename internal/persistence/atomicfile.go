package persistence

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// WriteFileAtomic publishes bytes with the classic durable replacement shape:
// same-directory temp file -> write -> fsync(file) -> rename -> fsync(dir).
// A failure before rename leaves the old target untouched.
func WriteFileAtomic(path string, data []byte, perm fs.FileMode) error {
	return writeFileAtomic(path, data, perm, nil)
}

type beforeRenameHook func(tempPath, targetPath string) error

func writeFileAtomic(path string, data []byte, perm fs.FileMode, beforeRename beforeRenameHook) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create persistence directory: %w", err)
	}

	f, err := os.CreateTemp(dir, "."+base+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp persistence file: %w", err)
	}
	tempPath := f.Name()
	published := false
	defer func() {
		_ = f.Close()
		if !published {
			_ = os.Remove(tempPath)
		}
	}()

	if err := f.Chmod(perm); err != nil {
		return fmt.Errorf("chmod temp persistence file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write temp persistence file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync temp persistence file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp persistence file: %w", err)
	}
	if beforeRename != nil {
		if err := beforeRename(tempPath, path); err != nil {
			return err
		}
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publish persistence file: %w", err)
	}
	published = true
	if err := SyncDir(dir); err != nil {
		return fmt.Errorf("fsync persistence directory: %w", err)
	}
	return nil
}

// SyncDir makes the rename's directory entry durable on filesystems that
// support directory fsync. v0.5 targets the Linux/Unix durability model used by
// the reactor CI path.
func SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
