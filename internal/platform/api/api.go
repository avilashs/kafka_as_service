// Package api is the tenant-facing REST API (spec: api/openapi.yaml). Every
// mutation is asynchronous, requires an Idempotency-Key, and returns an Operation.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/ids"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// Authenticator maps a request to a tenant ID.
type Authenticator interface {
	Tenant(r *http.Request) (string, error)
}

// StaticTokens authenticates "Authorization: Bearer <token>" against a fixed map.
// It is for development and tests only; OIDC replaces it in M3.
type StaticTokens map[string]string

// Tenant implements Authenticator.
func (s StaticTokens) Tenant(r *http.Request) (string, error) {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || tok == "" {
		return "", errors.New("missing bearer token")
	}
	t, ok := s[tok]
	if !ok {
		return "", errors.New("invalid token")
	}
	return t, nil
}

// Server serves the tenant API.
type Server struct {
	Store   *store.Store
	Engines *engine.Registry
	Secrets secrets.Store
	Auth    Authenticator
	Log     *slog.Logger
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /v1/clusters", s.authed(s.createCluster))
	mux.HandleFunc("GET /v1/clusters", s.authed(s.listClusters))
	mux.HandleFunc("GET /v1/clusters/{id}", s.authed(s.getCluster))
	mux.HandleFunc("DELETE /v1/clusters/{id}", s.authed(s.deleteCluster))
	mux.HandleFunc("GET /v1/clusters/{id}/bootstrap-brokers", s.authed(s.bootstrap))
	mux.HandleFunc("GET /v1/clusters/{id}/ca-certificate", s.authed(s.caCertificate))
	mux.HandleFunc("GET /v1/clusters/{id}/scram-users", s.authed(s.listUsers(mechSCRAM)))
	mux.HandleFunc("POST /v1/clusters/{id}/scram-users", s.authed(s.createUser(mechSCRAM)))
	mux.HandleFunc("DELETE /v1/clusters/{id}/scram-users/{name}", s.authed(s.deleteUser(mechSCRAM)))
	mux.HandleFunc("GET /v1/clusters/{id}/mtls-users", s.authed(s.listUsers(mechMTLS)))
	mux.HandleFunc("POST /v1/clusters/{id}/mtls-users", s.authed(s.createUser(mechMTLS)))
	mux.HandleFunc("DELETE /v1/clusters/{id}/mtls-users/{name}", s.authed(s.deleteUser(mechMTLS)))
	mux.HandleFunc("POST /v1/clusters/{id}/mtls-users/{name}/renew", s.authed(s.renewCertificate))
	mux.HandleFunc("GET /v1/clusters/{id}/mtls-users/{name}/certificate", s.authed(s.mtlsCertificate))
	mux.HandleFunc("POST /v1/clusters/{id}/scram-users/{name}/rotate", s.authed(s.rotateUser))
	mux.HandleFunc("GET /v1/clusters/{id}/scram-users/{name}/credentials", s.authed(s.userCredentials))
	mux.HandleFunc("GET /v1/operations/{id}", s.authed(s.getOperation))
	return mux
}

type handler func(w http.ResponseWriter, r *http.Request, tenant string)

func (s *Server) authed(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t, err := s.Auth.Tenant(r)
		if err != nil {
			writeErr(w, http.StatusUnauthorized, "UNAUTHENTICATED", err.Error())
			return
		}
		h(w, r, t)
	}
}

// ---------- wire types (keep in sync with api/openapi.yaml) ----------

// Cluster is the API representation of a cluster.
type Cluster struct {
	ID                 string          `json:"id"`
	Name               string          `json:"name"`
	Engine             string          `json:"engine"`
	Tier               string          `json:"tier"`
	State              string          `json:"state"`
	StateReason        string          `json:"stateReason,omitempty"`
	Spec               json.RawMessage `json:"spec"`
	CellID             string          `json:"cellId,omitempty"`
	CurrentOperationID string          `json:"currentOperationId,omitempty"`
	CreatedAt          time.Time       `json:"createdAt"`
	UpdatedAt          time.Time       `json:"updatedAt"`
}

// Operation is the API representation of an operation.
type Operation struct {
	ID          string          `json:"id"`
	ClusterID   string          `json:"clusterId"`
	Type        string          `json:"type"`
	State       string          `json:"state"`
	Step        string          `json:"step,omitempty"`
	Error       json.RawMessage `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	CompletedAt *time.Time      `json:"completedAt,omitempty"`
}

// CreateClusterRequest is the body of POST /v1/clusters.
type CreateClusterRequest struct {
	Name   string          `json:"name"`
	Engine string          `json:"engine,omitempty"`
	Tier   string          `json:"tier"`
	Spec   json.RawMessage `json:"spec"`
}

func toCluster(c *store.Cluster) Cluster {
	out := Cluster{ID: c.ID, Name: c.Name, Engine: c.Engine, Tier: string(c.Tier), State: c.State, Spec: withoutUsers(c.Spec),
		CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt}
	if c.StateReason != nil {
		out.StateReason = *c.StateReason
	}
	if c.CellID != nil {
		out.CellID = *c.CellID
	}
	if c.LockOperationID != nil {
		out.CurrentOperationID = *c.LockOperationID
	}
	return out
}

func toOperation(o *store.Operation) Operation {
	out := Operation{ID: o.ID, ClusterID: o.ClusterID, Type: o.Type, State: o.State, Error: o.Error,
		CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt, CompletedAt: o.CompletedAt}
	if o.Step != nil {
		out.Step = *o.Step
	}
	return out
}

// ---------- handlers ----------

const maxBody = 1 << 20

func (s *Server) createCluster(w http.ResponseWriter, r *http.Request, tenant string) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeErr(w, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header (1-255 chars) is required")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	var req CreateClusterRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body: "+err.Error())
		return
	}
	if req.Engine == "" {
		req.Engine = "kafka"
	}
	eng, err := s.Engines.Get(req.Engine)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", err.Error())
		return
	}
	tier := engine.Tier(req.Tier)
	if tier != engine.TierShared && tier != engine.TierDedicated {
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", `tier must be "shared" or "dedicated"`)
		return
	}
	if hasUsers(req.Spec) {
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", "users are created with POST /v1/clusters/{id}/scram-users once the cluster is ACTIVE")
		return
	}
	hash, err := canonicalHash(req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	clusterID := ids.Cluster()
	rs := engine.ResourceSpec{ID: clusterID, Name: req.Name, Tenant: tenant, Engine: req.Engine, Tier: tier, Spec: req.Spec}
	if err := eng.Validate(rs, nil); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", err.Error())
		return
	}
	reqJSON, _ := json.Marshal(req)
	op, replay, err := s.Store.CreateCluster(r.Context(), store.CreateClusterInput{
		MutationInput: store.MutationInput{TenantID: tenant, Actor: "tenant:" + tenant, IdempotencyKey: key, RequestHash: hash, Request: reqJSON},
		ClusterID:     clusterID, OpID: ids.Operation(), Name: req.Name, Engine: req.Engine, Tier: tier, Spec: compactJSON(req.Spec),
	})
	if s.storeErr(w, err) {
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusAccepted, toOperation(op))
}

func (s *Server) listClusters(w http.ResponseWriter, r *http.Request, tenant string) {
	cs, err := s.Store.ListClusters(r.Context(), tenant)
	if s.storeErr(w, err) {
		return
	}
	out := make([]Cluster, 0, len(cs))
	for _, c := range cs {
		out = append(out, toCluster(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"clusters": out})
}

func (s *Server) getCluster(w http.ResponseWriter, r *http.Request, tenant string) {
	c, err := s.Store.GetCluster(r.Context(), tenant, r.PathValue("id"))
	if s.storeErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, toCluster(c))
}

func (s *Server) deleteCluster(w http.ResponseWriter, r *http.Request, tenant string) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeErr(w, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header (1-255 chars) is required")
		return
	}
	id := r.PathValue("id")
	hash, _ := canonicalHash(map[string]string{"op": "DeleteCluster", "clusterId": id})
	reqJSON, _ := json.Marshal(map[string]string{"clusterId": id})
	op, replay, err := s.Store.DeleteCluster(r.Context(),
		store.MutationInput{TenantID: tenant, Actor: "tenant:" + tenant, IdempotencyKey: key, RequestHash: hash, Request: reqJSON},
		id, ids.Operation())
	if s.storeErr(w, err) {
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusAccepted, toOperation(op))
}

func (s *Server) bootstrap(w http.ResponseWriter, r *http.Request, tenant string) {
	c, err := s.Store.GetCluster(r.Context(), tenant, r.PathValue("id"))
	if s.storeErr(w, err) {
		return
	}
	if c.State != "ACTIVE" && c.State != "UPDATING" && c.State != "DEGRADED" {
		writeErr(w, http.StatusConflict, "NOT_READY", "bootstrap endpoints are available once the cluster is ACTIVE")
		return
	}
	eng, err := s.Engines.Get(c.Engine)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	obs, err := s.Store.GetObserved(r.Context(), c.ID)
	if s.storeErr(w, err) {
		return
	}
	st, err := eng.ObserveStatus(c.ResourceSpec(), obs.Observed)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	endpoints := map[string]string{}
	for k, v := range st.Values {
		if name, ok := strings.CutPrefix(k, "endpoint."); ok && v != "" {
			endpoints[name] = v
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"clusterId": c.ID, "endpoints": endpoints})
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request, tenant string) {
	op, err := s.Store.GetOperation(r.Context(), tenant, r.PathValue("id"))
	if s.storeErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, toOperation(op))
}

// ---------- helpers ----------

func (s *Server) storeErr(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	var ce *store.ConflictError
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
	case errors.Is(err, store.ErrIdempotencyMismatch):
		writeErr(w, http.StatusUnprocessableEntity, "IDEMPOTENCY_KEY_REUSED", err.Error())
	case errors.As(err, &ce):
		writeErr(w, http.StatusConflict, "CONFLICT", ce.Msg)
	case errors.Is(err, context.Canceled):
		writeErr(w, http.StatusServiceUnavailable, "UNAVAILABLE", "request cancelled")
	default:
		s.Log.Error("store error", "err", err)
		writeErr(w, http.StatusServiceUnavailable, "UNAVAILABLE", "the control plane is temporarily unavailable")
	}
	return true
}

func canonicalHash(v any) (string, error) {
	// Marshal → unmarshal into generic → marshal sorts map keys, giving a canonical form.
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	var g any
	if err := json.Unmarshal(b, &g); err != nil {
		return "", err
	}
	b, _ = json.Marshal(g)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func compactJSON(raw json.RawMessage) json.RawMessage {
	var g any
	if err := json.Unmarshal(raw, &g); err != nil {
		return raw
	}
	b, _ := json.Marshal(g)
	return b
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]string{"code": errCode, "message": msg}})
}

// withoutUsers hides the users a spec holds: they are managed (and shown) through the user endpoints.
func withoutUsers(spec json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(spec, &m) != nil {
		return spec
	}
	if _, ok := m["users"]; !ok {
		return spec
	}
	delete(m, "users")
	b, err := json.Marshal(m)
	if err != nil {
		return spec
	}
	return b
}

func hasUsers(spec json.RawMessage) bool {
	var m map[string]json.RawMessage
	if json.Unmarshal(spec, &m) != nil {
		return false
	}
	_, ok := m["users"]
	return ok
}
