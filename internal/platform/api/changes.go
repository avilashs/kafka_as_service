package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// updateCluster handles PATCH /v1/clusters/{id}: a lifecycle change expressed as a JSON merge patch of the
// spec (RFC 7386). The engine decides what kind of change it is and refuses a patch that mixes kinds, so
// each operation has one safety argument (design §16).
func (s *Server) updateCluster(w http.ResponseWriter, r *http.Request, tenant string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	c, err := s.Store.GetCluster(r.Context(), tenant, r.PathValue("id"))
	if s.storeErr(w, err) {
		return
	}
	eng, err := s.Engines.Get(c.Engine)
	ch, canChange := eng.(engine.Changer)
	if err != nil || !canChange {
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", "this engine has no lifecycle changes")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	var body struct {
		Spec json.RawMessage `json:"spec"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil || len(body.Spec) == 0 {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", `the body must be {"spec": {…}} (a merge patch of the cluster spec)`)
		return
	}
	if hasUsers(body.Spec) {
		writeErr(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", "users are changed through the user endpoints")
		return
	}
	// A replay is answered before the patch is validated: validation would see the state the first call changed.
	hash, _ := canonicalHash(map[string]any{"op": "PatchCluster", "clusterId": c.ID, "spec": body.Spec})
	mi := store.MutationInput{TenantID: tenant, IdempotencyKey: key, RequestHash: hash}
	if prev, err := s.Store.ReplayOperation(r.Context(), mi); s.storeErr(w, err) {
		return
	} else if prev != nil {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusAccepted, toOperation(prev))
		return
	}
	// Plan against the spec we just read to name the operation and see whether capacity is needed. The
	// same plan is recomputed in the transaction against the locked spec; if it differs, 409.
	plan := func(cur *store.Cluster) (json.RawMessage, engine.Change, error) {
		next, err := mergePatch(cur.Spec, body.Spec)
		if err != nil {
			return nil, engine.Change{}, engine.Invalid("invalid spec patch: %v", err)
		}
		from := cur.ResourceSpec()
		to := from
		to.Spec = next
		if err := eng.Validate(to, &from); err != nil {
			return nil, engine.Change{}, err
		}
		chg, err := ch.ClassifyChange(from, to)
		return next, chg, err
	}
	_, chg, err := plan(c)
	if err != nil {
		s.mutateError(w, err)
		return
	}
	opType := map[string]string{"config": "ChangeConfig", "size": "ResizeCluster", "scale": "ScaleCluster",
		"storage": "ExpandStorage", "version": "UpgradeCluster"}[chg.Kind]
	if opType == "" {
		opType = "UpdateCluster"
	}
	s.updateHashed(w, r, tenant, key, hash, opType,
		updateRequest{Action: "spec.change", Kind: chg.Kind, NeedsCapacity: chg.NeedsCapacity},
		func(cur *store.Cluster) (json.RawMessage, error) {
			next, again, err := plan(cur)
			if err != nil {
				return nil, err
			}
			if again != chg {
				return nil, &store.ConflictError{Msg: "the cluster changed while this request was processed; retry"}
			}
			return next, nil
		})
}

// mergePatch applies an RFC 7386 JSON merge patch.
func mergePatch(orig, patch json.RawMessage) (json.RawMessage, error) {
	var o, p any
	d := json.NewDecoder(bytes.NewReader(orig))
	d.UseNumber()
	if err := d.Decode(&o); err != nil {
		return nil, err
	}
	d = json.NewDecoder(bytes.NewReader(patch))
	d.UseNumber()
	if err := d.Decode(&p); err != nil {
		return nil, err
	}
	return json.Marshal(applyPatch(o, p))
}

func applyPatch(o, p any) any {
	pm, ok := p.(map[string]any)
	if !ok {
		return p
	}
	om, ok := o.(map[string]any)
	if !ok {
		om = map[string]any{}
	}
	for k, v := range pm {
		if v == nil {
			delete(om, k)
			continue
		}
		om[k] = applyPatch(om[k], v)
	}
	return om
}
