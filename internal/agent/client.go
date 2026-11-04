package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/avilashs/kafka_as_service/internal/platform/cellapi"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// CPClient talks to the control plane's cell API. All connections are outbound.
type CPClient struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func (c *CPClient) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(msg))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// Desired long-polls for entries newer than since.
func (c *CPClient) Desired(ctx context.Context, since int64, wait time.Duration) (*cellapi.DesiredResponse, error) {
	var out cellapi.DesiredResponse
	q := url.Values{"since": {fmt.Sprint(since)}, "wait": {wait.String()}}
	return &out, c.do(ctx, http.MethodGet, "/cell/v1/desired?"+q.Encode(), nil, &out)
}

// Status pushes observations.
func (c *CPClient) Status(ctx context.Context, reports []store.StatusReport) error {
	return c.do(ctx, http.MethodPost, "/cell/v1/status", cellapi.StatusRequest{Reports: reports}, nil)
}

// Heartbeat pushes liveness and inventory.
func (c *CPClient) Heartbeat(ctx context.Context, inventory any) error {
	return c.do(ctx, http.MethodPost, "/cell/v1/heartbeat", inventory, nil)
}
