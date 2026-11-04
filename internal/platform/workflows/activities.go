// Package workflows holds the durable Temporal workflows and their activities.
// Every activity is idempotent and safe to re-run (design §6).
package workflows

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/avilashs/kafka_as_service/internal/platform/capacity"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// TaskQueue is the control-plane task queue.
const TaskQueue = "streamcell-control"

// TerminalErrorType marks non-retryable application errors.
const TerminalErrorType = "Terminal"

// Terminal wraps err as a non-retryable error.
func Terminal(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), TerminalErrorType, err)
}

// classify maps known error classes to terminal; everything else is retryable.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var ve *engine.ValidationError
	switch {
	case errors.As(err, &ve),
		errors.Is(err, capacity.ErrExhausted),
		errors.Is(err, store.ErrLockLost),
		errors.Is(err, store.ErrDigestMismatch),
		errors.Is(err, store.ErrNotFound):
		return Terminal(err)
	}
	return err
}

// Activities carries activity dependencies.
type Activities struct {
	Store     *store.Store
	Allocator *capacity.Allocator
	Engines   *engine.Registry
	// Secrets is the secret manager. Passwords are generated here and never written to the database.
	Secrets secrets.Store
	// PollInterval controls how often WaitFor checks observed status.
	PollInterval time.Duration
}

// Activity names (registered explicitly so renames cannot break history replay).
const (
	ActMarkStep         = "MarkStep"
	ActReserve          = "ReserveCapacity"
	ActRenderPublish    = "RenderAndPublish"
	ActPublishTombstone = "PublishTombstone"
	ActWaitFor          = "WaitForCondition"
	ActFinishCreate     = "FinishCreate"
	ActFailCreate       = "FailCreate"
	ActFinishDelete     = "FinishDelete"
	ActFailDelete       = "FailDelete"
	ActPrepareUpdate    = "PrepareUpdate"
	ActFinishUpdate     = "FinishUpdate"
	ActFailUpdate       = "FailUpdate"
	ActDeleteSecrets    = "DeleteClusterSecrets"
)

// MarkStep records progress on the operation.
func (a *Activities) MarkStep(ctx context.Context, opID, step string) error {
	return classify(faultPoint(ActMarkStep+":"+step, a.Store.SetOperationStep(ctx, opID, step)))
}

// ReserveCapacity plans placement and reserves capacity (idempotent per operation).
func (a *Activities) ReserveCapacity(ctx context.Context, opID, clusterID string) (engine.Assignment, error) {
	c, err := a.Store.ClusterByID(ctx, clusterID)
	if err != nil {
		return engine.Assignment{}, classify(err)
	}
	e, err := a.Engines.Get(c.Engine)
	if err != nil {
		return engine.Assignment{}, classify(err)
	}
	req, err := e.PlanPlacement(c.ResourceSpec())
	if err != nil {
		return engine.Assignment{}, classify(err)
	}
	as, err := a.Allocator.Reserve(ctx, opID, clusterID, c.TenantID, req)
	if err != nil {
		return engine.Assignment{}, classify(err)
	}
	if err := a.Store.SetAssignment(ctx, opID, clusterID, as); err != nil {
		return engine.Assignment{}, classify(err)
	}
	return as, classify(faultPoint(ActReserve, nil))
}

// RenderAndPublish renders the cluster's current spec and appends it to the cell's desired state as
// generation gen; gen 0 means "the spec's own generation" (used by updates). It returns the generation.
func (a *Activities) RenderAndPublish(ctx context.Context, opID, clusterID string, gen int64) (int64, error) {
	c, err := a.Store.ClusterByID(ctx, clusterID)
	if err != nil {
		return 0, classify(err)
	}
	if gen == 0 {
		gen = c.SpecGeneration
	}
	if c.Assignment == nil {
		return 0, Terminal(fmt.Errorf("cluster %s has no assignment", clusterID))
	}
	env, err := a.Store.CellEnv(ctx, c.Assignment.CellID)
	if err != nil {
		return 0, classify(err)
	}
	e, err := a.Engines.Get(c.Engine)
	if err != nil {
		return 0, classify(err)
	}
	b, err := e.Render(c.ResourceSpec(), *c.Assignment, env)
	if err != nil {
		return 0, Terminal(err)
	}
	if _, err := a.Store.PublishDesired(ctx, opID, clusterID, gen, b, false); err != nil {
		return 0, classify(err)
	}
	return gen, classify(faultPoint(ActRenderPublish, nil))
}

// PublishTombstone publishes a delete marker one generation above the latest
// non-tombstone generation; re-running it returns the same generation.
func (a *Activities) PublishTombstone(ctx context.Context, opID, clusterID string) (int64, error) {
	c, err := a.Store.ClusterByID(ctx, clusterID)
	if err != nil {
		return 0, classify(err)
	}
	if c.CellID == nil {
		return 0, nil // never placed: nothing to delete in any cell
	}
	latest, err := a.Store.LatestGeneration(ctx, clusterID)
	if err != nil {
		return 0, classify(err)
	}
	gen := latest + 1
	if latest > 0 {
		if tomb, err := a.isTombstone(ctx, clusterID, latest); err != nil {
			return 0, classify(err)
		} else if tomb {
			gen = latest
		}
	}
	// The tombstone carries references (not specs) to everything the last live
	// bundle created, so an agent that lost its local state can still delete.
	live, err := a.Store.LatestLiveBundle(ctx, clusterID)
	if err != nil {
		return 0, classify(err)
	}
	var refs *engine.Bundle
	if live != nil {
		refs = &engine.Bundle{DeleteGraceSeconds: live.DeleteGraceSeconds}
		for _, np := range live.NodePools { // enough to find and release the pool
			refs.NodePools = append(refs.NodePools, engine.NodePoolSpec{ID: np.ID, Owner: np.Owner, InstanceType: np.InstanceType})
		}
		for _, o := range live.Objects {
			u := &unstructured.Unstructured{}
			u.SetAPIVersion(o.GetAPIVersion())
			u.SetKind(o.GetKind())
			u.SetNamespace(o.GetNamespace())
			u.SetName(o.GetName())
			refs.Objects = append(refs.Objects, u)
		}
	}
	if _, err := a.Store.PublishDesired(ctx, opID, clusterID, gen, refs, true); err != nil {
		return 0, classify(err)
	}
	return gen, classify(faultPoint(ActPublishTombstone, nil))
}

func (a *Activities) isTombstone(ctx context.Context, clusterID string, gen int64) (bool, error) {
	var tomb bool
	err := a.Store.Pool.QueryRow(ctx, `SELECT tombstone FROM desired_states WHERE cluster_id=$1 AND generation=$2`, clusterID, gen).Scan(&tomb)
	return tomb, err
}

// WaitForCondition polls the agent-reported observation until the engine says
// cond holds for generation gen. It heartbeats (and extends the reservation) while waiting.
func (a *Activities) WaitForCondition(ctx context.Context, opID, clusterID string, gen int64, cond engine.Condition) (engine.Status, error) {
	if gen == 0 && cond.Type == "Deleted" {
		return engine.Status{Deleted: true}, nil
	}
	c, err := a.Store.ClusterByID(ctx, clusterID)
	if err != nil {
		return engine.Status{}, classify(err)
	}
	e, err := a.Engines.Get(c.Engine)
	if err != nil {
		return engine.Status{}, classify(err)
	}
	interval := a.PollInterval
	if interval == 0 {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	var last engine.Status
	for {
		obs, err := a.Store.GetObserved(ctx, clusterID)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return last, err
		}
		if obs != nil && obs.Observed.Generation >= gen {
			st, err := e.ObserveStatus(c.ResourceSpec(), obs.Observed)
			if err != nil {
				return last, err
			}
			last = st
			if st.Satisfies(cond) {
				return st, classify(faultPoint(ActWaitFor+":"+cond.Type, nil))
			}
		}
		_ = a.Allocator.Heartbeat(ctx, opID)
		activity.RecordHeartbeat(ctx, last.Reason)
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-t.C:
		}
	}
}

// FinishCreate completes a successful create.
func (a *Activities) FinishCreate(ctx context.Context, opID, clusterID string) error {
	return classify(faultPoint(ActFinishCreate, a.Store.FinishCreate(ctx, opID, clusterID)))
}

// FailCreate completes a failed create.
func (a *Activities) FailCreate(ctx context.Context, opID, clusterID string, e store.OpError, cleanedUp bool) error {
	return classify(a.Store.FailCreate(ctx, opID, clusterID, e, cleanedUp))
}

// FinishDelete completes a successful delete.
func (a *Activities) FinishDelete(ctx context.Context, opID, clusterID string) error {
	return classify(faultPoint(ActFinishDelete, a.Store.FinishDelete(ctx, opID, clusterID)))
}

// FailDelete completes a failed delete.
func (a *Activities) FailDelete(ctx context.Context, opID, clusterID string, e store.OpError) error {
	return classify(a.Store.FailDelete(ctx, opID, clusterID, e))
}

// ---------- updates (users today; resize, upgrade and configuration in M4) ----------

// updateRequest is what the API stores in operations.request for an UpdateCluster operation.
type updateRequest struct {
	Action string `json:"action"` // user.create | user.rotate | user.delete
	User   string `json:"user,omitempty"`
}

func (a *Activities) loadUpdate(ctx context.Context, opID string) (updateRequest, error) {
	raw, err := a.Store.OperationRequest(ctx, opID)
	if err != nil {
		return updateRequest{}, err
	}
	var r updateRequest
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, Terminal(fmt.Errorf("unreadable update request: %w", err))
	}
	return r, nil
}

func (a *Activities) userManager(c *store.Cluster) (engine.UserManager, error) {
	e, err := a.Engines.Get(c.Engine)
	if err != nil {
		return nil, err
	}
	um, ok := e.(engine.UserManager)
	if !ok {
		return nil, Terminal(fmt.Errorf("engine %s has no users", c.Engine))
	}
	return um, nil
}

// GeneratePassword returns a 256-bit random password in a URL-safe alphabet, so that it needs no
// escaping in a JAAS configuration or a properties file.
func GeneratePassword() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// PrepareUpdate creates (user.create) or replaces (user.rotate) the secret a user needs and records
// its version in the cluster spec. It is safe to re-run: user.create keeps an existing secret.
func (a *Activities) PrepareUpdate(ctx context.Context, opID, clusterID string) error {
	req, err := a.loadUpdate(ctx, opID)
	if err != nil {
		return classify(err)
	}
	if req.Action != "user.create" && req.Action != "user.rotate" {
		return nil
	}
	c, err := a.Store.ClusterByID(ctx, clusterID)
	if err != nil {
		return classify(err)
	}
	if c.CellID == nil {
		return Terminal(fmt.Errorf("cluster %s has no cell", clusterID))
	}
	um, err := a.userManager(c)
	if err != nil {
		return classify(err)
	}
	path := secrets.UserPath(*c.CellID, clusterID, req.User)
	var version int
	if req.Action == "user.create" {
		if _, v, err := a.Secrets.Get(ctx, path); err == nil {
			version = v
		} else if !errors.Is(err, secrets.ErrNotFound) {
			return err
		}
	}
	if version == 0 {
		pw, err := GeneratePassword()
		if err != nil {
			return err
		}
		if version, err = a.Secrets.Put(ctx, path, map[string]string{"password": pw}); err != nil {
			return err
		}
	}
	return classify(a.Store.MutateSpecUnderLock(ctx, opID, clusterID, func(spec json.RawMessage) (json.RawMessage, error) {
		return um.SetUserSecretVersion(spec, req.User, version)
	}))
}

// FinishUpdate removes the secret of a deleted user and completes the operation.
func (a *Activities) FinishUpdate(ctx context.Context, opID, clusterID string) error {
	req, err := a.loadUpdate(ctx, opID)
	if err != nil {
		return classify(err)
	}
	if req.Action == "user.delete" {
		c, err := a.Store.ClusterByID(ctx, clusterID)
		if err != nil {
			return classify(err)
		}
		if c.CellID != nil {
			if err := a.Secrets.Delete(ctx, secrets.UserPath(*c.CellID, clusterID, req.User)); err != nil {
				return err
			}
		}
	}
	return classify(faultPoint(ActFinishUpdate, a.Store.FinishUpdate(ctx, opID, clusterID, req.Action)))
}

// FailUpdate completes an update as failed.
func (a *Activities) FailUpdate(ctx context.Context, opID, clusterID string, e store.OpError) error {
	req, err := a.loadUpdate(ctx, opID)
	if err != nil {
		req = updateRequest{Action: "update"}
	}
	return classify(a.Store.FailUpdate(ctx, opID, clusterID, req.Action, e))
}

// DeleteClusterSecrets removes everything the cluster keeps in the secret manager. Idempotent.
func (a *Activities) DeleteClusterSecrets(ctx context.Context, clusterID string) error {
	c, err := a.Store.ClusterByID(ctx, clusterID)
	if err != nil {
		return classify(err)
	}
	if c.CellID == nil || a.Secrets == nil {
		return nil
	}
	return a.Secrets.Delete(ctx, secrets.ClusterPrefix(*c.CellID, clusterID))
}
