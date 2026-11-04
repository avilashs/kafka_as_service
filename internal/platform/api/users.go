package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/ids"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// updateRequest is stored in operations.request; the UpdateCluster workflow reads it.
// Keep in sync with workflows.updateRequest.
type updateRequest struct {
	Action string          `json:"action"`
	User   string          `json:"user,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"` // user.create: the request as sent (no secrets are ever in it)
}

// userManager returns the engine's user support for a cluster the tenant owns.
func (s *Server) userManager(w http.ResponseWriter, r *http.Request, tenant string) (*store.Cluster, engine.UserManager, bool) {
	c, err := s.Store.GetCluster(r.Context(), tenant, r.PathValue("id"))
	if s.storeErr(w, err) {
		return nil, nil, false
	}
	e, err := s.Engines.Get(c.Engine)
	um, ok := e.(engine.UserManager)
	if err != nil || !ok {
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", "this engine has no users")
		return nil, nil, false
	}
	return c, um, true
}

func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeErr(w, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header (1-255 chars) is required")
		return "", false
	}
	return key, true
}

// mutateError maps engine errors raised inside the update transaction.
func (s *Server) mutateError(w http.ResponseWriter, err error) bool {
	var ve *engine.ValidationError
	var nf *engine.NotFoundError
	switch {
	case errors.As(err, &ve):
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", ve.Msg)
		return true
	case errors.As(err, &nf):
		writeErr(w, http.StatusNotFound, "NOT_FOUND", nf.Error())
		return true
	}
	return s.storeErr(w, err)
}

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request, tenant string) {
	c, um, ok := s.userManager(w, r, tenant)
	if !ok {
		return
	}
	users, err := um.ListUsers(c.Spec)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": json.RawMessage(users)})
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request, tenant string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	_, um, ok := s.userManager(w, r, tenant)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	// Mutate runs inside the transaction, after the idempotency lookup and before anything is written,
	// so an invalid request creates no operation and a replay of a valid one is still a replay.
	var named struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(body, &named)
	s.update(w, r, tenant, key, "CreateScramUser", updateRequest{Action: "user.create", User: named.Name, Body: compactJSON(body)},
		func(c *store.Cluster) (json.RawMessage, error) {
			spec, _, err := um.AddUser(c.Spec, body)
			return spec, err
		})
}

func (s *Server) rotateUser(w http.ResponseWriter, r *http.Request, tenant string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	_, um, ok := s.userManager(w, r, tenant)
	if !ok {
		return
	}
	name := r.PathValue("name")
	s.update(w, r, tenant, key, "RotateScramUser", updateRequest{Action: "user.rotate", User: name},
		func(c *store.Cluster) (json.RawMessage, error) { return um.RotateUser(c.Spec, name) })
}

func (s *Server) deleteUser(w http.ResponseWriter, r *http.Request, tenant string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	_, um, ok := s.userManager(w, r, tenant)
	if !ok {
		return
	}
	name := r.PathValue("name")
	s.update(w, r, tenant, key, "DeleteScramUser", updateRequest{Action: "user.delete", User: name},
		func(c *store.Cluster) (json.RawMessage, error) { return um.RemoveUser(c.Spec, name) })
}

func (s *Server) update(w http.ResponseWriter, r *http.Request, tenant, key, opType string, req updateRequest, mutate func(*store.Cluster) (json.RawMessage, error)) {
	id := r.PathValue("id")
	hash, _ := canonicalHash(map[string]any{"op": opType, "clusterId": id, "req": req})
	reqJSON, _ := json.Marshal(req)
	op, replay, err := s.Store.UpdateClusterSpec(r.Context(), store.UpdateSpecInput{
		MutationInput: store.MutationInput{TenantID: tenant, Actor: "tenant:" + tenant, IdempotencyKey: key, RequestHash: hash, Request: reqJSON},
		ClusterID:     id, OpID: ids.Operation(), OpType: opType, Mutate: mutate,
	})
	if s.mutateError(w, err) {
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusAccepted, toOperation(op))
}

// userCredentials returns the SCRAM password of a user of the caller's own cluster. The password is
// read from the secret manager; it is never stored in the database or in logs.
func (s *Server) userCredentials(w http.ResponseWriter, r *http.Request, tenant string) {
	c, um, ok := s.userManager(w, r, tenant)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if !userExists(um, c.Spec, name) {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "user "+name+" not found")
		return
	}
	if c.CellID == nil || s.Secrets == nil {
		writeErr(w, http.StatusConflict, "NOT_READY", "credentials are not available yet")
		return
	}
	data, version, err := s.Secrets.Get(r.Context(), secrets.UserPath(*c.CellID, c.ID, name))
	if errors.Is(err, secrets.ErrNotFound) {
		writeErr(w, http.StatusConflict, "NOT_READY", "the user's credentials are still being created")
		return
	}
	if err != nil {
		s.Log.Error("secret read", "err", err)
		writeErr(w, http.StatusServiceUnavailable, "UNAVAILABLE", "the secret manager is temporarily unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"username": name, "password": data["password"], "mechanism": "SCRAM-SHA-512", "secretVersion": version,
	})
}

func userExists(um engine.UserManager, spec json.RawMessage, name string) bool {
	raw, err := um.ListUsers(spec)
	if err != nil {
		return false
	}
	var us []struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(raw, &us)
	for _, u := range us {
		if u.Name == name {
			return true
		}
	}
	return false
}

// caCertificate returns the PEM CA certificate clients must trust for this cluster. Every cluster has
// its own CA (design §16); the certificate is public.
func (s *Server) caCertificate(w http.ResponseWriter, r *http.Request, tenant string) {
	c, err := s.Store.GetCluster(r.Context(), tenant, r.PathValue("id"))
	if s.storeErr(w, err) {
		return
	}
	obs, err := s.Store.GetObserved(r.Context(), c.ID)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusConflict, "NOT_READY", "the cluster CA is not available yet")
		return
	}
	if s.storeErr(w, err) {
		return
	}
	pem := strings.TrimSpace(obs.Observed.Outputs["cluster-ca.crt"])
	if pem == "" {
		writeErr(w, http.StatusConflict, "NOT_READY", "the cluster CA is not available yet")
		return
	}
	w.Header().Set("Content-Type", "application/x-pem-file")
	_, _ = io.WriteString(w, pem+"\n")
}
