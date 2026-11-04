package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	path := filepath.Join("..", "..", "..", "api", "openapi.yaml")
	doc, err := openapi3.NewLoader().LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("openapi.yaml is invalid: %v", err)
	}
	return doc
}

// Every route the server registers is documented, and vice versa.
func TestOpenAPIRouteParity(t *testing.T) {
	doc := loadSpec(t)
	src, err := os.ReadFile("api.go")
	if err != nil {
		t.Fatal(err)
	}
	registered := map[string]bool{}
	for _, line := range strings.Split(string(src), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, `mux.HandleFunc("`) {
			continue
		}
		pattern := strings.SplitN(strings.TrimPrefix(line, `mux.HandleFunc("`), `"`, 2)[0]
		registered[pattern] = true
	}
	documented := map[string]bool{}
	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			documented[method+" "+path] = true
		}
	}
	for r := range registered {
		if !documented[r] {
			t.Errorf("route %q is served but not in openapi.yaml", r)
		}
	}
	for d := range documented {
		if !registered[d] {
			t.Errorf("route %q is documented but not served", d)
		}
	}
}

// Wire types serialise to objects that satisfy the documented schemas.
func TestWireTypesMatchSchemas(t *testing.T) {
	doc := loadSpec(t)
	now := time.Now().UTC()
	step := "wait-ready"
	cell := "cell-1"
	op := toOperation(&store.Operation{ID: "op-1", ClusterID: "kc-0123456789", Type: "CreateCluster", State: "RUNNING", Step: &step, CreatedAt: now, UpdatedAt: now})
	c := toCluster(&store.Cluster{ID: "kc-0123456789", Name: "orders", Engine: "kafka", Tier: engine.TierShared, State: "ACTIVE", CellID: &cell,
		Spec: json.RawMessage(`{"kafkaVersion":"4.3.1","brokerType":"s.small","brokerCount":3,"storage":{"sizeGiB":100}}`), CreatedAt: now, UpdatedAt: now})
	for name, v := range map[string]any{"Operation": op, "Cluster": c} {
		b, _ := json.Marshal(v)
		var generic any
		_ = json.Unmarshal(b, &generic)
		if err := doc.Components.Schemas[name].Value.VisitJSON(generic); err != nil {
			t.Errorf("%s does not match schema: %v\n%s", name, err, b)
		}
	}
}

// Validation and auth errors are returned before touching the database.
func TestRequestValidation(t *testing.T) {
	s := &Server{Engines: engine.NewRegistry(kafka.New()), Auth: StaticTokens{"tok": "t1"}}
	h := s.Handler()
	do := func(method, path, key, body string, auth bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if auth {
			r.Header.Set("Authorization", "Bearer tok")
		}
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	good := `{"name":"a","tier":"shared","spec":{"kafkaVersion":"4.3.1","brokerType":"s.small","brokerCount":3,"storage":{"sizeGiB":10}}}`
	cases := []struct {
		name, key, body string
		auth            bool
		want            int
	}{
		{"no auth", "k", good, false, http.StatusUnauthorized},
		{"no idempotency key", "", good, true, http.StatusBadRequest},
		{"unknown field", "k", `{"name":"a","tier":"shared","spec":{},"x":1}`, true, http.StatusBadRequest},
		{"bad tier", "k", strings.Replace(good, "shared", "gold", 1), true, http.StatusUnprocessableEntity},
		{"dedicated not yet", "k", strings.Replace(good, "shared", "dedicated", 1), true, http.StatusUnprocessableEntity},
		{"bad version", "k", strings.Replace(good, "4.3.1", "2.8.0", 1), true, http.StatusUnprocessableEntity},
		{"forbidden config", "k", strings.Replace(good, `"storage"`, `"config":{"ssl.keystore.location":"/x"},"storage"`, 1), true, http.StatusUnprocessableEntity},
	}
	for _, tc := range cases {
		if w := do("POST", "/v1/clusters", tc.key, tc.body, tc.auth); w.Code != tc.want {
			t.Errorf("%s: got %d (%s), want %d", tc.name, w.Code, w.Body.String(), tc.want)
		}
	}
}
