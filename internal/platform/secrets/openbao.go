package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenBao talks to an OpenBao (or Vault-compatible) KV version 2 mount over HTTP.
type OpenBao struct {
	addr, token, mount string
	hc                 *http.Client
}

// NewOpenBao returns a client. addr is like http://127.0.0.1:8200.
func NewOpenBao(addr, token, mount string) (*OpenBao, error) {
	if addr == "" || token == "" {
		return nil, fmt.Errorf("OPENBAO_ADDR and OPENBAO_TOKEN are required")
	}
	return &OpenBao{addr: strings.TrimRight(addr, "/"), token: token, mount: mount, hc: &http.Client{Timeout: 15 * time.Second}}, nil
}

func (o *OpenBao) do(ctx context.Context, method, url string, body any, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("X-Vault-Token", o.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := o.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode == http.StatusNotFound {
		return resp.StatusCode, ErrNotFound
	}
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("openbao %s %s: %s: %s", method, url, resp.Status, strings.TrimSpace(string(raw)))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

// Put implements Store.
func (o *OpenBao) Put(ctx context.Context, path string, data map[string]string) (int, error) {
	if err := ValidPath(path); err != nil {
		return 0, err
	}
	var out struct {
		Data struct {
			Version int `json:"version"`
		} `json:"data"`
	}
	if _, err := o.do(ctx, http.MethodPost, fmt.Sprintf("%s/v1/%s/data/%s", o.addr, o.mount, path), map[string]any{"data": data}, &out); err != nil {
		return 0, err
	}
	return out.Data.Version, nil
}

// Get implements Store.
func (o *OpenBao) Get(ctx context.Context, path string) (map[string]string, int, error) {
	if err := ValidPath(path); err != nil {
		return nil, 0, err
	}
	var out struct {
		Data struct {
			Data     map[string]string `json:"data"`
			Metadata struct {
				Version      int    `json:"version"`
				DeletionTime string `json:"deletion_time"`
			} `json:"metadata"`
		} `json:"data"`
	}
	if _, err := o.do(ctx, http.MethodGet, fmt.Sprintf("%s/v1/%s/data/%s", o.addr, o.mount, path), nil, &out); err != nil {
		return nil, 0, err
	}
	if out.Data.Data == nil { // soft-deleted version: data is null
		return nil, 0, ErrNotFound
	}
	return out.Data.Data, out.Data.Metadata.Version, nil
}

// Delete implements Store: it removes the metadata (every version) of path and of everything below.
func (o *OpenBao) Delete(ctx context.Context, path string) error {
	if err := ValidPath(path); err != nil {
		return err
	}
	var walk func(p string) error
	walk = func(p string) error {
		var list struct {
			Data struct {
				Keys []string `json:"keys"`
			} `json:"data"`
		}
		_, err := o.do(ctx, "LIST", fmt.Sprintf("%s/v1/%s/metadata/%s", o.addr, o.mount, p), nil, &list)
		if err != nil && err != ErrNotFound {
			return err
		}
		for _, k := range list.Data.Keys {
			if strings.HasSuffix(k, "/") {
				if err := walk(p + "/" + strings.TrimSuffix(k, "/")); err != nil {
					return err
				}
				continue
			}
			if _, err := o.do(ctx, http.MethodDelete, fmt.Sprintf("%s/v1/%s/metadata/%s/%s", o.addr, o.mount, p, k), nil, nil); err != nil && err != ErrNotFound {
				return err
			}
		}
		return nil
	}
	if err := walk(path); err != nil {
		return err
	}
	if _, err := o.do(ctx, http.MethodDelete, fmt.Sprintf("%s/v1/%s/metadata/%s", o.addr, o.mount, path), nil, nil); err != nil && err != ErrNotFound {
		return err
	}
	return nil
}
