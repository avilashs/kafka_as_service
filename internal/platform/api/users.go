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
	// Kind and NeedsCapacity describe a spec.change (see workflows.updateRequest).
	Kind          string `json:"kind,omitempty"`
	NeedsCapacity bool   `json:"needsCapacity,omitempty"`
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

const (
	mechSCRAM = "SCRAM-SHA-512"
	mechMTLS  = "mTLS"
)

// listUsers lists the users of one mechanism.
func (s *Server) listUsers(mech string) handler {
	return func(w http.ResponseWriter, r *http.Request, tenant string) {
		c, um, ok := s.userManager(w, r, tenant)
		if !ok {
			return
		}
		raw, err := um.ListUsers(c.Spec)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		var all []map[string]any
		_ = json.Unmarshal(raw, &all)
		users := []map[string]any{}
		for _, u := range all {
			if u["mechanism"] == mech {
				users = append(users, u)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"users": users})
	}
}

// userMechanism returns the mechanism of an existing user, or "".
func userMechanism(um engine.UserManager, spec json.RawMessage, name string) string {
	raw, err := um.ListUsers(spec)
	if err != nil {
		return ""
	}
	var us []struct {
		Name      string `json:"name"`
		Mechanism string `json:"mechanism"`
	}
	_ = json.Unmarshal(raw, &us)
	for _, u := range us {
		if u.Name == name {
			return u.Mechanism
		}
	}
	return ""
}

// requireMechanism answers 404 unless the named user exists with that mechanism.
func (s *Server) requireMechanism(w http.ResponseWriter, um engine.UserManager, spec json.RawMessage, name, mech string) bool {
	if userMechanism(um, spec, name) != mech {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "user "+name+" not found")
		return false
	}
	return true
}

// userBody reads the request body as a JSON object and sets the mechanism the endpoint stands for. A
// client can not choose the mechanism (nor smuggle a certificate request into a SCRAM user).
func userBody(w http.ResponseWriter, r *http.Request, mech string) (json.RawMessage, bool) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return nil, false
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the body must be a JSON object")
		return nil, false
	}
	for _, k := range []string{"auth", "secretVersion"} {
		if _, ok := m[k]; ok {
			writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", "unknown field \""+k+"\"")
			return nil, false
		}
	}
	if mech == mechMTLS {
		m["auth"] = json.RawMessage(`"tls"`)
	} else if _, ok := m["csr"]; ok {
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", "unknown field \"csr\" (certificate users are created at /mtls-users)")
		return nil, false
	}
	out, _ := json.Marshal(m)
	return out, true
}

func (s *Server) createUser(mech string) handler {
	opType, action := "CreateScramUser", "user.create"
	if mech == mechMTLS {
		opType, action = "CreateMtlsUser", "mtls.create"
	}
	return func(w http.ResponseWriter, r *http.Request, tenant string) {
		key, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		_, um, ok := s.userManager(w, r, tenant)
		if !ok {
			return
		}
		body, ok := userBody(w, r, mech)
		if !ok {
			return
		}
		// Mutate runs inside the transaction, after the idempotency lookup and before anything is written,
		// so an invalid request creates no operation and a replay of a valid one is still a replay.
		var named struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(body, &named)
		// The stored operation request keeps no certificate request: the spec has it already.
		s.update(w, r, tenant, key, opType, updateRequest{Action: action, User: named.Name},
			func(c *store.Cluster) (json.RawMessage, error) {
				spec, _, err := um.AddUser(c.Spec, body)
				return spec, err
			})
	}
}

func (s *Server) rotateUser(w http.ResponseWriter, r *http.Request, tenant string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	c, um, ok := s.userManager(w, r, tenant)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if !s.requireMechanism(w, um, c.Spec, name, mechSCRAM) {
		return
	}
	s.update(w, r, tenant, key, "RotateScramUser", updateRequest{Action: "user.rotate", User: name},
		func(c *store.Cluster) (json.RawMessage, error) { return um.RotateUser(c.Spec, name) })
}

// renewCertificate replaces a certificate user's request; the cell signs the new one.
func (s *Server) renewCertificate(w http.ResponseWriter, r *http.Request, tenant string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	c, um, ok := s.userManager(w, r, tenant)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if !s.requireMechanism(w, um, c.Spec, name, mechMTLS) {
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	var req struct {
		CSR string `json:"csr"`
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err != nil || dec.Decode(&req) != nil || req.CSR == "" {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", `the body must be {"csr": "<PEM>"}`)
		return
	}
	s.update(w, r, tenant, key, "RenewMtlsCertificate", updateRequest{Action: "mtls.renew", User: name},
		func(c *store.Cluster) (json.RawMessage, error) { return um.SetUserCSR(c.Spec, name, req.CSR) })
}

// mtlsCertificate returns the certificate the cell issued for a certificate user.
func (s *Server) mtlsCertificate(w http.ResponseWriter, r *http.Request, tenant string) {
	c, um, ok := s.userManager(w, r, tenant)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if !s.requireMechanism(w, um, c.Spec, name, mechMTLS) {
		return
	}
	obs, err := s.Store.GetObserved(r.Context(), c.ID)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusConflict, "NOT_READY", "the certificate has not been issued yet")
		return
	}
	if s.storeErr(w, err) {
		return
	}
	pem := strings.TrimSpace(obs.Observed.Outputs[engine.CertOutputPrefix+name])
	if pem == "" {
		writeErr(w, http.StatusConflict, "NOT_READY", "the certificate has not been issued yet")
		return
	}
	w.Header().Set("Content-Type", "application/x-pem-file")
	_, _ = io.WriteString(w, pem+"\n")
}

func (s *Server) deleteUser(mech string) handler {
	opType, action := "DeleteScramUser", "user.delete"
	if mech == mechMTLS {
		opType, action = "DeleteMtlsUser", "mtls.delete"
	}
	return func(w http.ResponseWriter, r *http.Request, tenant string) {
		key, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		c, um, ok := s.userManager(w, r, tenant)
		if !ok {
			return
		}
		name := r.PathValue("name")
		if !s.requireMechanism(w, um, c.Spec, name, mech) {
			return
		}
		s.update(w, r, tenant, key, opType, updateRequest{Action: action, User: name},
			func(c *store.Cluster) (json.RawMessage, error) { return um.RemoveUser(c.Spec, name) })
	}
}

func (s *Server) update(w http.ResponseWriter, r *http.Request, tenant, key, opType string, req updateRequest, mutate func(*store.Cluster) (json.RawMessage, error)) {
	id := r.PathValue("id")
	hash, _ := canonicalHash(map[string]any{"op": opType, "clusterId": id, "req": req})
	s.updateHashed(w, r, tenant, key, hash, opType, req, mutate)
}

func (s *Server) updateHashed(w http.ResponseWriter, r *http.Request, tenant, key, hash, opType string, req updateRequest, mutate func(*store.Cluster) (json.RawMessage, error)) {
	id := r.PathValue("id")
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
	if !s.requireMechanism(w, um, c.Spec, name, mechSCRAM) {
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
