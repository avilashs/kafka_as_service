package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// File stores each secret as a 0600 JSON file under dir. Development and tests only: the values
// sit unencrypted on disk.
type File struct {
	dir string
	mu  sync.Mutex
}

type fileRecord struct {
	Version int               `json:"version"`
	Data    map[string]string `json:"data"`
}

// NewFile creates the directory (0700) if needed.
func NewFile(dir string) (*File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &File{dir: dir}, nil
}

const leaf = "_secret.json"

func (f *File) file(path string) (string, error) {
	if err := ValidPath(path); err != nil {
		return "", err
	}
	return filepath.Join(f.dir, filepath.FromSlash(path), leaf), nil
}

// Put implements Store.
func (f *File) Put(_ context.Context, path string, data map[string]string) (int, error) {
	fn, err := f.file(path)
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rec := fileRecord{Version: 1}
	if b, err := os.ReadFile(fn); err == nil {
		var old fileRecord
		if json.Unmarshal(b, &old) == nil {
			rec.Version = old.Version + 1
		}
	}
	rec.Data = data
	if err := os.MkdirAll(filepath.Dir(fn), 0o700); err != nil {
		return 0, err
	}
	b, _ := json.Marshal(rec)
	tmp := fn + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, fn); err != nil {
		return 0, err
	}
	return rec.Version, nil
}

// Get implements Store.
func (f *File) Get(_ context.Context, path string) (map[string]string, int, error) {
	fn, err := f.file(path)
	if err != nil {
		return nil, 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := os.ReadFile(fn)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	var rec fileRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, 0, fmt.Errorf("corrupt secret %s: %w", path, err)
	}
	return rec.Data, rec.Version, nil
}

// Delete implements Store.
func (f *File) Delete(_ context.Context, path string) error {
	if err := ValidPath(path); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return os.RemoveAll(filepath.Join(f.dir, filepath.FromSlash(path)))
}
