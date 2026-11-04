package secrets

import (
	"context"
	"strings"
	"sync"
)

// Memory is an in-process store for unit tests.
type Memory struct {
	mu   sync.Mutex
	data map[string]fileRecord
}

// NewMemory returns an empty store.
func NewMemory() *Memory { return &Memory{data: map[string]fileRecord{}} }

// Put implements Store.
func (m *Memory) Put(_ context.Context, path string, data map[string]string) (int, error) {
	if err := ValidPath(path); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.data[path]
	r.Version++
	r.Data = copyMap(data)
	m.data[path] = r
	return r.Version, nil
}

// Get implements Store.
func (m *Memory) Get(_ context.Context, path string) (map[string]string, int, error) {
	if err := ValidPath(path); err != nil {
		return nil, 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.data[path]
	if !ok {
		return nil, 0, ErrNotFound
	}
	return copyMap(r.Data), r.Version, nil
}

// Delete implements Store.
func (m *Memory) Delete(_ context.Context, path string) error {
	if err := ValidPath(path); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for p := range m.data {
		if p == path || strings.HasPrefix(p, path+"/") {
			delete(m.data, p)
		}
	}
	return nil
}

func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
