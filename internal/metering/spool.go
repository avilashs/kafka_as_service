package metering

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Spool is the write-ahead log of closed records that the control plane has not acknowledged yet. It is a
// JSON-lines file: a record is appended (and synced) before the state that produced it is saved, and is
// removed only when the control plane confirms it. Delivery is at least once; the control plane's
// ON CONFLICT DO NOTHING makes it exactly once in effect.
type Spool struct {
	path string
	mu   sync.Mutex
	recs map[string]Record
}

// OpenSpool loads the spool at path (creating its directory).
func OpenSpool(path string) (*Spool, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &Spool{path: path, recs: map[string]Record{}}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			continue // a torn last line from a crash: the record is re-derived on restart
		}
		s.recs[r.ID] = r
	}
	return s, sc.Err()
}

// Append adds records and syncs them to disk. Appending a record that is already spooled is a no-op.
func (s *Spool) Append(rs []Record) error {
	if len(rs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, r := range rs {
		if _, dup := s.recs[r.ID]; dup {
			continue
		}
		b, _ := json.Marshal(r)
		w.Write(b)
		w.WriteByte('\n')
		s.recs[r.ID] = r
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

// Pending returns up to limit unacknowledged records, oldest interval first.
func (s *Spool) Pending(limit int) []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0, len(s.recs))
	for _, r := range s.recs {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Start.Equal(out[j].Start) {
			return out[i].Start.Before(out[j].Start)
		}
		return out[i].ID < out[j].ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Len is the number of unacknowledged records.
func (s *Spool) Len() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.recs) }

// Ack removes acknowledged records (rewriting the file atomically).
func (s *Spool) Ack(ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for _, id := range ids {
		if _, ok := s.recs[id]; ok {
			delete(s.recs, id)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, r := range s.sorted() {
		b, _ := json.Marshal(r)
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, s.path)
}

func (s *Spool) sorted() []Record {
	out := make([]Record, 0, len(s.recs))
	for _, r := range s.recs {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// SaveFile writes b to path atomically (write, sync, rename).
func SaveFile(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, path)
}
