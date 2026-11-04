// Package secrets is the platform's secret manager interface. The database stores only
// references (paths and versions), never values (ADR-0012). Implementations:
//
//   - File:    a directory tree with 0600 files. For local development and tests ONLY.
//   - OpenBao: KV v2 over HTTP, for self-hosted installs.
//   - AWS Secrets Manager is the production target on AWS and arrives in M6.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// ErrNotFound means the path does not exist.
var ErrNotFound = errors.New("secret not found")

// Store keeps small maps of strings under slash-separated paths, with a version that
// increases on every Put.
type Store interface {
	// Put writes data at path and returns the new version (1 for a new secret).
	Put(ctx context.Context, path string, data map[string]string) (version int, err error)
	// Get returns the current data and version, or ErrNotFound.
	Get(ctx context.Context, path string) (data map[string]string, version int, err error)
	// Delete removes path and everything below it. A missing path is not an error.
	Delete(ctx context.Context, path string) error
}

var segmentRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ValidPath reports whether path is made of safe segments only (no traversal, no empty parts).
func ValidPath(path string) error {
	if path == "" || strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") {
		return fmt.Errorf("invalid secret path %q", path)
	}
	for _, seg := range strings.Split(path, "/") {
		if !segmentRE.MatchString(seg) || seg == "." || seg == ".." {
			return fmt.Errorf("invalid secret path segment %q in %q", seg, path)
		}
	}
	return nil
}

// ClusterPrefix is the path under which everything belonging to one cluster is stored.
// A cell agent only needs read access to streamcell/<its cell>/*.
func ClusterPrefix(cell, cluster string) string { return "streamcell/" + cell + "/" + cluster }

// UserPath is where a SCRAM user's password lives.
func UserPath(cell, cluster, user string) string {
	return ClusterPrefix(cell, cluster) + "/users/" + user
}

// Ref points at one field of a secret. Version is informational for change detection:
// bumping it changes the rendered desired state, so the agent re-materialises the secret.
type Ref struct {
	Path    string `json:"path"`
	Key     string `json:"key"`
	Version int    `json:"version"`
}

// FromEnv builds the configured store.
//
//	SECRETS_BACKEND=file    SECRETS_DIR=/path
//	SECRETS_BACKEND=openbao OPENBAO_ADDR=http://… OPENBAO_TOKEN=… [OPENBAO_MOUNT=secret]
func FromEnv() (Store, error) {
	switch b := os.Getenv("SECRETS_BACKEND"); b {
	case "file":
		dir := os.Getenv("SECRETS_DIR")
		if dir == "" {
			return nil, errors.New("SECRETS_DIR is required for the file backend")
		}
		return NewFile(dir)
	case "openbao":
		mount := os.Getenv("OPENBAO_MOUNT")
		if mount == "" {
			mount = "secret"
		}
		return NewOpenBao(os.Getenv("OPENBAO_ADDR"), os.Getenv("OPENBAO_TOKEN"), mount)
	case "":
		return nil, errors.New("SECRETS_BACKEND is not set (file or openbao)")
	default:
		return nil, fmt.Errorf("unknown SECRETS_BACKEND %q", b)
	}
}
