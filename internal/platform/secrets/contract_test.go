package secrets

import (
	"context"
	"errors"
	"os"
	"testing"
)

func stores(t *testing.T) map[string]Store {
	t.Helper()
	f, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]Store{"memory": NewMemory(), "file": f}
	if addr := os.Getenv("STREAMCELL_E2E_OPENBAO_ADDR"); addr != "" {
		b, err := NewOpenBao(addr, os.Getenv("STREAMCELL_E2E_OPENBAO_TOKEN"), "secret")
		if err != nil {
			t.Fatal(err)
		}
		out["openbao (real server)"] = b
	} else {
		t.Log("OpenBao contract test skipped: run `eval \"$(hack/e2e-deps.sh up)\"` first")
	}
	return out
}

// Every implementation must behave the same.
func TestStoreContract(t *testing.T) {
	ctx := context.Background()
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			prefix := "streamcell/test-" + name[:3] + "/kc-" + randSuffix()
			a, b := prefix+"/users/alice", prefix+"/users/bob"

			if _, _, err := s.Get(ctx, a); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get of a missing secret: %v, want ErrNotFound", err)
			}
			v, err := s.Put(ctx, a, map[string]string{"password": "one"})
			if err != nil || v != 1 {
				t.Fatalf("first Put: version %d, err %v", v, err)
			}
			if v, err = s.Put(ctx, a, map[string]string{"password": "two"}); err != nil || v != 2 {
				t.Fatalf("second Put: version %d, err %v", v, err)
			}
			data, v, err := s.Get(ctx, a)
			if err != nil || v != 2 || data["password"] != "two" {
				t.Fatalf("Get: %v v%d %v", data, v, err)
			}
			if _, err := s.Put(ctx, b, map[string]string{"password": "b"}); err != nil {
				t.Fatal(err)
			}
			// Deleting a prefix removes everything below it and nothing else.
			other := "streamcell/test-" + name[:3] + "/other-" + randSuffix()
			if _, err := s.Put(ctx, other+"/users/carol", map[string]string{"password": "c"}); err != nil {
				t.Fatal(err)
			}
			if err := s.Delete(ctx, prefix); err != nil {
				t.Fatalf("Delete prefix: %v", err)
			}
			for _, p := range []string{a, b} {
				if _, _, err := s.Get(ctx, p); !errors.Is(err, ErrNotFound) {
					t.Errorf("%s survived the prefix delete: %v", p, err)
				}
			}
			if _, _, err := s.Get(ctx, other+"/users/carol"); err != nil {
				t.Errorf("an unrelated secret was deleted: %v", err)
			}
			if err := s.Delete(ctx, prefix); err != nil {
				t.Errorf("deleting a missing path must not fail: %v", err)
			}
			// A recreated secret starts again from version 1... or higher, but never serves old data.
			if _, err := s.Put(ctx, a, map[string]string{"password": "fresh"}); err != nil {
				t.Fatal(err)
			}
			if data, _, _ := s.Get(ctx, a); data["password"] != "fresh" {
				t.Errorf("recreated secret = %v", data)
			}
			_ = s.Delete(ctx, prefix)
			_ = s.Delete(ctx, other)
		})
	}
}

func TestPathValidation(t *testing.T) {
	for _, bad := range []string{"", "/abs", "trailing/", "a//b", "a/../b", "a/./b", "a b/c", "../x", "a/b\\c", ".hidden"} {
		if ValidPath(bad) == nil {
			t.Errorf("path %q must be rejected", bad)
		}
	}
	for _, good := range []string{"streamcell/cell-1/kc-0123456789/users/alice", "a", "a/b.c/d_e-f"} {
		if err := ValidPath(good); err != nil {
			t.Errorf("path %q rejected: %v", good, err)
		}
	}
	m := NewMemory()
	if _, err := m.Put(context.Background(), "a/../b", map[string]string{"x": "y"}); err == nil {
		t.Error("Put accepted a traversal path")
	}
}

func randSuffix() string {
	b := make([]byte, 4)
	f, _ := os.Open("/dev/urandom")
	defer f.Close()
	_, _ = f.Read(b)
	const hex = "0123456789abcdef"
	out := make([]byte, 8)
	for i, c := range b {
		out[i*2], out[i*2+1] = hex[c>>4], hex[c&15]
	}
	return string(out)
}
