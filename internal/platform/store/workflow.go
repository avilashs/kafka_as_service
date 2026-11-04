package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// ErrLockLost means the cluster's lock is no longer held by the calling operation.
// It is a terminal, bug-class error: two workflows must never mutate one cluster.
var ErrLockLost = errors.New("cluster lock is not held by this operation")

// ErrDigestMismatch means a generation was re-published with different content.
var ErrDigestMismatch = errors.New("desired state generation already published with different content")

// ClusterByID loads a cluster without tenant scoping (internal callers only).
func (s *Store) ClusterByID(ctx context.Context, id string) (*Cluster, error) {
	return scanCluster(s.Pool.QueryRow(ctx, `SELECT `+clusterCols+` FROM clusters WHERE id=$1`, id))
}

// SetOperationStep marks an operation RUNNING at a step (idempotent).
func (s *Store) SetOperationStep(ctx context.Context, opID, step string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE operations SET state='RUNNING', step=$2, updated_at=now()
		WHERE id=$1 AND state IN ('PENDING','RUNNING')`, opID, step)
	return err
}

// CellEnv loads a cell's render environment.
func (s *Store) CellEnv(ctx context.Context, cellID string) (engine.CellEnv, error) {
	var env engine.CellEnv
	var raw []byte
	var shards int
	err := s.Pool.QueryRow(ctx, `SELECT env, operator_shards FROM cells WHERE id=$1`, cellID).Scan(&raw, &shards)
	if errors.Is(err, pgx.ErrNoRows) {
		return env, ErrNotFound
	}
	if err != nil {
		return env, err
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return env, err
	}
	env.CellID = cellID
	env.OperatorShards = shards
	return env, nil
}

// SetAssignment records where the cluster was placed. Idempotent; guarded by the lock.
func (s *Store) SetAssignment(ctx context.Context, opID, clusterID string, a engine.Assignment) error {
	raw, _ := json.Marshal(a)
	tag, err := s.Pool.Exec(ctx, `UPDATE clusters SET cell_id=$3, assignment=$4, updated_at=now()
		WHERE id=$1 AND lock_operation_id=$2`, clusterID, opID, a.CellID, raw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLockLost
	}
	return nil
}

// PublishDesired appends generation gen of the cluster's desired state and bumps
// the cell revision. Re-publishing the same generation with the same digest is a
// no-op that returns the original revision.
func (s *Store) PublishDesired(ctx context.Context, opID, clusterID string, gen int64, bundle *engine.Bundle, tomb bool) (rev int64, err error) {
	var raw []byte
	if bundle != nil {
		if raw, err = json.Marshal(bundle); err != nil {
			return 0, err
		}
	}
	digest, err := Digest(map[string]any{"bundle": json.RawMessage(nonNullJSON(raw)), "tombstone": tomb})
	if err != nil {
		return 0, err
	}
	err = s.InTx(ctx, func(tx pgx.Tx) error {
		var cellID *string
		var lock *string
		if err := tx.QueryRow(ctx, `SELECT cell_id, lock_operation_id FROM clusters WHERE id=$1 FOR UPDATE`, clusterID).Scan(&cellID, &lock); err != nil {
			return err
		}
		if lock == nil || *lock != opID {
			return ErrLockLost
		}
		if cellID == nil {
			return fmt.Errorf("cluster %s has no cell assignment", clusterID)
		}
		var existingDigest string
		err := tx.QueryRow(ctx, `SELECT bundle_digest, cell_revision FROM desired_states WHERE cluster_id=$1 AND generation=$2`, clusterID, gen).Scan(&existingDigest, &rev)
		if err == nil {
			if existingDigest != digest {
				return ErrDigestMismatch
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err := tx.QueryRow(ctx, `UPDATE cells SET revision = revision + 1 WHERE id=$1 RETURNING revision`, *cellID).Scan(&rev); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO desired_states (cluster_id, generation, cell_id, cell_revision, bundle, bundle_digest, tombstone)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`, clusterID, gen, *cellID, rev, nullableJSON(raw), digest, tomb)
		return err
	})
	return rev, err
}

func nullableJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nonNullJSON(b []byte) []byte {
	if len(b) == 0 {
		return []byte("null")
	}
	return b
}

// LatestLiveBundle returns the most recent non-tombstone bundle (nil if none).
func (s *Store) LatestLiveBundle(ctx context.Context, clusterID string) (*engine.Bundle, error) {
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT bundle FROM desired_states WHERE cluster_id=$1 AND NOT tombstone
		ORDER BY generation DESC LIMIT 1`, clusterID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) || len(raw) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var b engine.Bundle
	return &b, json.Unmarshal(raw, &b)
}

// LatestGeneration returns the highest published generation for a cluster (0 if none).
func (s *Store) LatestGeneration(ctx context.Context, clusterID string) (int64, error) {
	var g *int64
	err := s.Pool.QueryRow(ctx, `SELECT max(generation) FROM desired_states WHERE cluster_id=$1`, clusterID).Scan(&g)
	if err != nil || g == nil {
		return 0, err
	}
	return *g, nil
}

// ObservedRow is the latest report from the cell agent.
type ObservedRow struct {
	Observed   engine.Observed
	ReportedAt time.Time
}

// GetObserved returns the latest observation, or ErrNotFound.
func (s *Store) GetObserved(ctx context.Context, clusterID string) (*ObservedRow, error) {
	var raw []byte
	var o ObservedRow
	err := s.Pool.QueryRow(ctx, `SELECT observed, reported_at FROM observed_status WHERE cluster_id=$1`, clusterID).Scan(&raw, &o.ReportedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &o.Observed); err != nil {
		return nil, err
	}
	return &o, nil
}

func finishOp(ctx context.Context, tx pgx.Tx, opID, state string, opErr *OpError) error {
	var errJSON []byte
	if opErr != nil {
		errJSON, _ = json.Marshal(opErr)
	}
	_, err := tx.Exec(ctx, `UPDATE operations SET state=$2, error=$3, updated_at=now(), completed_at=now()
		WHERE id=$1 AND state IN ('PENDING','RUNNING')`, opID, state, nullableJSON(errJSON))
	return err
}

func audit(ctx context.Context, tx pgx.Tx, actor, tenant, action, resource, result string) error {
	_, err := tx.Exec(ctx, `INSERT INTO audit_log (actor, tenant_id, action, resource, result) VALUES ($1,$2,$3,$4,$5)`,
		actor, tenant, action, resource, result)
	return err
}

// lockedCluster selects the cluster FOR UPDATE and reports whether opID still holds
// the lock. done=true means the operation already finished (idempotent re-run).
func lockedCluster(ctx context.Context, tx pgx.Tx, opID, clusterID string) (c *Cluster, done bool, err error) {
	c, err = scanCluster(tx.QueryRow(ctx, `SELECT `+clusterCols+` FROM clusters WHERE id=$1 FOR UPDATE`, clusterID))
	if err != nil {
		return nil, false, err
	}
	if c.LockOperationID == nil || *c.LockOperationID != opID {
		var st string
		if err := tx.QueryRow(ctx, `SELECT state FROM operations WHERE id=$1`, opID).Scan(&st); err != nil {
			return nil, false, err
		}
		if st == "SUCCEEDED" || st == "FAILED" {
			return c, true, nil
		}
		return nil, false, ErrLockLost
	}
	return c, false, nil
}

// FinishCreate commits the reservation, marks the cluster ACTIVE and the operation SUCCEEDED.
func (s *Store) FinishCreate(ctx context.Context, opID, clusterID string) error {
	return s.InTx(ctx, func(tx pgx.Tx) error {
		c, done, err := lockedCluster(ctx, tx, opID, clusterID)
		if err != nil || done {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE reservations SET status='COMMITTED' WHERE operation_id=$1 AND status='HELD'`, opID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET state='ACTIVE', state_reason=NULL, lock_operation_id=NULL, updated_at=now() WHERE id=$1`, clusterID); err != nil {
			return err
		}
		if err := finishOp(ctx, tx, opID, "SUCCEEDED", nil); err != nil {
			return err
		}
		return audit(ctx, tx, "system", c.TenantID, "CreateCluster", "cluster/"+clusterID, "succeeded")
	})
}

// FailCreate marks the cluster FAILED and fails the operation. When cleanedUp is
// true (compensation removed everything from the cell) the reservation is released;
// otherwise it is committed so the capacity stays accounted until the tenant deletes.
func (s *Store) FailCreate(ctx context.Context, opID, clusterID string, opErr OpError, cleanedUp bool) error {
	return s.InTx(ctx, func(tx pgx.Tx) error {
		c, done, err := lockedCluster(ctx, tx, opID, clusterID)
		if err != nil || done {
			return err
		}
		q := `UPDATE reservations SET status='RELEASED', released_at=now() WHERE operation_id=$1 AND status<>'RELEASED'`
		if !cleanedUp {
			q = `UPDATE reservations SET status='COMMITTED' WHERE operation_id=$1 AND status='HELD'`
		}
		if _, err := tx.Exec(ctx, q, opID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET state='FAILED', state_reason=$2, lock_operation_id=NULL, updated_at=now() WHERE id=$1`, clusterID, opErr.Message); err != nil {
			return err
		}
		if err := finishOp(ctx, tx, opID, "FAILED", &opErr); err != nil {
			return err
		}
		return audit(ctx, tx, "system", c.TenantID, "CreateCluster", "cluster/"+clusterID, "failed")
	})
}

// FinishDelete soft-deletes the cluster and releases all of its capacity.
func (s *Store) FinishDelete(ctx context.Context, opID, clusterID string) error {
	return s.InTx(ctx, func(tx pgx.Tx) error {
		c, done, err := lockedCluster(ctx, tx, opID, clusterID)
		if err != nil || done {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE reservations SET status='RELEASED', released_at=now() WHERE cluster_id=$1 AND status<>'RELEASED'`, clusterID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET deleted_at=now(), lock_operation_id=NULL, updated_at=now() WHERE id=$1`, clusterID); err != nil {
			return err
		}
		if err := finishOp(ctx, tx, opID, "SUCCEEDED", nil); err != nil {
			return err
		}
		return audit(ctx, tx, "system", c.TenantID, "DeleteCluster", "cluster/"+clusterID, "succeeded")
	})
}

// FailDelete leaves the cluster FAILED (so delete can be retried) and fails the operation.
func (s *Store) FailDelete(ctx context.Context, opID, clusterID string, opErr OpError) error {
	return s.InTx(ctx, func(tx pgx.Tx) error {
		c, done, err := lockedCluster(ctx, tx, opID, clusterID)
		if err != nil || done {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET state='FAILED', state_reason=$2, lock_operation_id=NULL, updated_at=now() WHERE id=$1`,
			clusterID, "delete failed: "+opErr.Message); err != nil {
			return err
		}
		if err := finishOp(ctx, tx, opID, "FAILED", &opErr); err != nil {
			return err
		}
		return audit(ctx, tx, "system", c.TenantID, "DeleteCluster", "cluster/"+clusterID, "failed")
	})
}

// OperationRequest returns the stored request of an operation (what the tenant asked for).
func (s *Store) OperationRequest(ctx context.Context, opID string) (json.RawMessage, error) {
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT request FROM operations WHERE id=$1`, opID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return raw, err
}

// SetUserSecretVersion records, in the cluster spec, which secret version a user's rendered state
// refers to. It is done by the workflow after the secret exists and before the new generation is
// rendered, so that a rotation changes the rendered state. mutate gets the current spec.
func (s *Store) MutateSpecUnderLock(ctx context.Context, opID, clusterID string, mutate func(spec json.RawMessage) (json.RawMessage, error)) error {
	return s.InTx(ctx, func(tx pgx.Tx) error {
		c, done, err := lockedCluster(ctx, tx, opID, clusterID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		newSpec, err := mutate(c.Spec)
		if err != nil {
			return err
		}
		if string(newSpec) == string(c.Spec) {
			return nil
		}
		_, err = tx.Exec(ctx, `UPDATE clusters SET spec=$2, updated_at=now() WHERE id=$1`, clusterID, []byte(newSpec))
		return err
	})
}

// RevertSpec restores the spec the cluster had before the operation's change (used when the change
// could not be placed, before anything was published). The spec generation moves forward, never back.
func (s *Store) RevertSpec(ctx context.Context, opID, clusterID string) error {
	return s.InTx(ctx, func(tx pgx.Tx) error {
		_, done, err := lockedCluster(ctx, tx, opID, clusterID)
		if err != nil || done {
			return err
		}
		var req struct {
			Prior json.RawMessage `json:"prior"`
		}
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT request FROM operations WHERE id=$1`, opID).Scan(&raw); err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &req); err != nil || len(req.Prior) == 0 {
			return fmt.Errorf("operation %s has no prior spec to restore", opID)
		}
		_, err = tx.Exec(ctx, `UPDATE clusters SET spec=$2, spec_generation=spec_generation+1, updated_at=now() WHERE id=$1`, clusterID, []byte(req.Prior))
		return err
	})
}

// settleReservations is run when an update that held a reservation ends. On success the new reservation is
// committed and every older one of the cluster released. On failure the new one is committed too and the
// old one kept: the cell may hold part of the change, so both stay accounted until the next successful
// change or the delete reconciles them (over-counting is the safe direction).
func settleReservations(ctx context.Context, tx pgx.Tx, opID, clusterID string, success bool) error {
	tag, err := tx.Exec(ctx, `UPDATE reservations SET status='COMMITTED' WHERE operation_id=$1 AND status='HELD'`, opID)
	if err != nil || tag.RowsAffected() == 0 || !success {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE reservations SET status='RELEASED', released_at=now()
		WHERE cluster_id=$1 AND operation_id<>$2 AND status<>'RELEASED'`, clusterID, opID)
	return err
}

// FinishUpdate returns the cluster to ACTIVE and completes the operation.
func (s *Store) FinishUpdate(ctx context.Context, opID, clusterID, action string) error {
	return s.InTx(ctx, func(tx pgx.Tx) error {
		c, done, err := lockedCluster(ctx, tx, opID, clusterID)
		if err != nil || done {
			return err
		}
		if err := settleReservations(ctx, tx, opID, clusterID, true); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET state='ACTIVE', state_reason=NULL, lock_operation_id=NULL, updated_at=now() WHERE id=$1`, clusterID); err != nil {
			return err
		}
		if err := finishOp(ctx, tx, opID, "SUCCEEDED", nil); err != nil {
			return err
		}
		return audit(ctx, tx, "system", c.TenantID, action, "cluster/"+clusterID, "succeeded")
	})
}

// FailUpdate completes the operation as FAILED. The cluster keeps serving, so it goes back to
// ACTIVE (not FAILED); the new spec stays as the desired state and the reason is recorded.
func (s *Store) FailUpdate(ctx context.Context, opID, clusterID, action string, opErr OpError) error {
	return s.InTx(ctx, func(tx pgx.Tx) error {
		c, done, err := lockedCluster(ctx, tx, opID, clusterID)
		if err != nil || done {
			return err
		}
		if err := settleReservations(ctx, tx, opID, clusterID, false); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET state='ACTIVE', state_reason=$2, lock_operation_id=NULL, updated_at=now() WHERE id=$1`,
			clusterID, action+" failed: "+opErr.Message); err != nil {
			return err
		}
		if err := finishOp(ctx, tx, opID, "FAILED", &opErr); err != nil {
			return err
		}
		return audit(ctx, tx, "system", c.TenantID, action, "cluster/"+clusterID, "failed")
	})
}
