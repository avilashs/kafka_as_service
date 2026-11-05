// Package store is the PostgreSQL repository. Postgres is the authoritative
// source of desired state; every multi-row change happens in one transaction.
package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Errors returned by the store.
var (
	ErrNotFound            = errors.New("not found")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
)

// ConflictError means the request conflicts with current resource state (HTTP 409).
type ConflictError struct{ Msg string }

func (e *ConflictError) Error() string { return e.Msg }

// Store wraps a pgx pool.
type Store struct {
	Pool *pgxpool.Pool
}

// Open connects and runs migrations.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	goose.SetBaseFS(migrations)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.UpContext(ctx, db, "migrations")
}

// Close releases the pool.
func (s *Store) Close() { s.Pool.Close() }

// InTx runs fn in a transaction.
func (s *Store) InTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.Pool, fn)
}

// HashToken returns the stored form of a cell agent token.
func HashToken(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

// Digest is a stable hex sha256 of a JSON-marshalable value.
func Digest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func isUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

// ---------- domain rows ----------

// Cluster is a row of clusters.
type Cluster struct {
	ID              string             `json:"id"`
	TenantID        string             `json:"tenantId"`
	Name            string             `json:"name"`
	Engine          string             `json:"engine"`
	Tier            engine.Tier        `json:"tier"`
	CellID          *string            `json:"cellId,omitempty"`
	State           string             `json:"state"`
	StateReason     *string            `json:"stateReason,omitempty"`
	Spec            json.RawMessage    `json:"spec"`
	SpecGeneration  int64              `json:"specGeneration"`
	Assignment      *engine.Assignment `json:"assignment,omitempty"`
	LockOperationID *string            `json:"lockOperationId,omitempty"`
	CreatedAt       time.Time          `json:"createdAt"`
	UpdatedAt       time.Time          `json:"updatedAt"`
	DeletedAt       *time.Time         `json:"deletedAt,omitempty"`
}

// ResourceSpec converts the row to the engine envelope.
func (c *Cluster) ResourceSpec() engine.ResourceSpec {
	return engine.ResourceSpec{ID: c.ID, Name: c.Name, Tenant: c.TenantID, Engine: c.Engine, Tier: c.Tier, Spec: c.Spec}
}

// Operation is a row of operations.
type Operation struct {
	ID          string          `json:"id"`
	TenantID    string          `json:"tenantId"`
	ClusterID   string          `json:"clusterId"`
	Type        string          `json:"type"`
	State       string          `json:"state"`
	Step        *string         `json:"step,omitempty"`
	Error       json.RawMessage `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	CompletedAt *time.Time      `json:"completedAt,omitempty"`
}

// OpError is stored in operations.error.
type OpError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

const clusterCols = `id, tenant_id, name, engine, tier, cell_id, state, state_reason, spec, spec_generation,
	assignment, lock_operation_id, created_at, updated_at, deleted_at`

func scanCluster(row pgx.Row) (*Cluster, error) { return scanClusterInto(row) }

// scanClusterInto scans the cluster columns followed by any extra destinations (joined columns).
func scanClusterInto(row pgx.Row, extra ...any) (*Cluster, error) {
	var c Cluster
	var assignment []byte
	dest := append([]any{&c.ID, &c.TenantID, &c.Name, &c.Engine, &c.Tier, &c.CellID, &c.State, &c.StateReason,
		&c.Spec, &c.SpecGeneration, &assignment, &c.LockOperationID, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt}, extra...)
	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if len(assignment) > 0 {
		c.Assignment = &engine.Assignment{}
		if err := json.Unmarshal(assignment, c.Assignment); err != nil {
			return nil, err
		}
	}
	return &c, nil
}

// clusterColsQualified is clusterCols with every column taken from the alias c (for joins).
var clusterColsQualified = func() string {
	parts := strings.Split(clusterCols, ",")
	for i, p := range parts {
		parts[i] = "c." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}()

const opCols = `id, tenant_id, cluster_id, type, state, step, error, created_at, updated_at, completed_at`

func scanOp(row pgx.Row) (*Operation, error) {
	var o Operation
	var errJSON []byte
	if err := row.Scan(&o.ID, &o.TenantID, &o.ClusterID, &o.Type, &o.State, &o.Step, &errJSON, &o.CreatedAt, &o.UpdatedAt, &o.CompletedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if len(errJSON) > 0 {
		o.Error = errJSON
	}
	return &o, nil
}

// ---------- tenant-facing reads ----------

// GetCluster returns a live cluster owned by tenant.
func (s *Store) GetCluster(ctx context.Context, tenant, id string) (*Cluster, error) {
	return scanCluster(s.Pool.QueryRow(ctx, `SELECT `+clusterCols+` FROM clusters WHERE id=$1 AND tenant_id=$2 AND deleted_at IS NULL`, id, tenant))
}

// ListClusters returns the tenant's live clusters.
func (s *Store) ListClusters(ctx context.Context, tenant string) ([]*Cluster, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+clusterCols+` FROM clusters WHERE tenant_id=$1 AND deleted_at IS NULL ORDER BY created_at, id`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GetOperation returns an operation owned by tenant.
func (s *Store) GetOperation(ctx context.Context, tenant, id string) (*Operation, error) {
	return scanOp(s.Pool.QueryRow(ctx, `SELECT `+opCols+` FROM operations WHERE id=$1 AND tenant_id=$2`, id, tenant))
}

// ---------- mutations from the API ----------

// MutationInput is common to every tenant mutation.
type MutationInput struct {
	TenantID       string
	Actor          string
	IdempotencyKey string
	RequestHash    string
	Request        json.RawMessage
}

// lookupIdempotency returns an existing operation for the key, if any.
func lookupIdempotency(ctx context.Context, tx pgx.Tx, in MutationInput) (*Operation, error) {
	var hash, opID string
	err := tx.QueryRow(ctx, `SELECT request_hash, operation_id FROM idempotency_keys
		WHERE tenant_id=$1 AND key=$2 AND expires_at > now()`, in.TenantID, in.IdempotencyKey).Scan(&hash, &opID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if hash != in.RequestHash {
		return nil, ErrIdempotencyMismatch
	}
	return scanOp(tx.QueryRow(ctx, `SELECT `+opCols+` FROM operations WHERE id=$1`, opID))
}

// ReplayOperation returns the operation an earlier request with the same idempotency key started, or nil.
// It lets a handler answer a replay before validating the request against state the first call changed.
func (s *Store) ReplayOperation(ctx context.Context, in MutationInput) (*Operation, error) {
	var op *Operation
	err := s.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		op, err = lookupIdempotency(ctx, tx, in)
		return err
	})
	return op, err
}

func insertOpAndOutbox(ctx context.Context, tx pgx.Tx, in MutationInput, opID, clusterID, opType, workflow string) (*Operation, error) {
	if _, err := tx.Exec(ctx, `INSERT INTO operations (id, tenant_id, cluster_id, type, state, request) VALUES ($1,$2,$3,$4,'PENDING',$5)`,
		opID, in.TenantID, clusterID, opType, []byte(in.Request)); err != nil {
		return nil, err
	}
	// Expired keys are removed lazily so a key may be reused after expiry.
	if _, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE tenant_id=$1 AND key=$2 AND expires_at <= now()`, in.TenantID, in.IdempotencyKey); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (tenant_id, key, request_hash, operation_id, expires_at)
		VALUES ($1,$2,$3,$4, now() + interval '7 days')`, in.TenantID, in.IdempotencyKey, in.RequestHash, opID); err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(map[string]string{"workflow": workflow, "operationId": opID, "clusterId": clusterID})
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (kind, payload) VALUES ('start_workflow', $1)`, payload); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_log (actor, tenant_id, action, resource, request_digest, result) VALUES ($1,$2,$3,$4,$5,'accepted')`,
		in.Actor, in.TenantID, opType, "cluster/"+clusterID, in.RequestHash); err != nil {
		return nil, err
	}
	return scanOp(tx.QueryRow(ctx, `SELECT `+opCols+` FROM operations WHERE id=$1`, opID))
}

// CreateClusterInput describes a create request that already passed engine validation.
type CreateClusterInput struct {
	MutationInput
	ClusterID string
	OpID      string
	Name      string
	Engine    string
	Tier      engine.Tier
	Spec      json.RawMessage
}

// CreateCluster inserts the cluster (CREATING, locked by the op), the operation,
// idempotency key, outbox entry and audit record atomically. replay is true when
// the idempotency key matched an earlier identical request.
func (s *Store) CreateCluster(ctx context.Context, in CreateClusterInput) (op *Operation, replay bool, err error) {
	err = s.InTx(ctx, func(tx pgx.Tx) error {
		existing, err := lookupIdempotency(ctx, tx, in.MutationInput)
		if err != nil {
			return err
		}
		if existing != nil {
			op, replay = existing, true
			return nil
		}
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM tenants WHERE id=$1`, in.TenantID).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			return &ConflictError{Msg: "tenant is not registered"}
		}
		_, err = tx.Exec(ctx, `INSERT INTO clusters (id, tenant_id, name, engine, tier, state, spec, lock_operation_id)
			VALUES ($1,$2,$3,$4,$5,'CREATING',$6,$7)`, in.ClusterID, in.TenantID, in.Name, in.Engine, string(in.Tier), []byte(in.Spec), in.OpID)
		if isUniqueViolation(err) {
			return &ConflictError{Msg: fmt.Sprintf("a cluster named %q already exists", in.Name)}
		}
		if err != nil {
			return err
		}
		op, err = insertOpAndOutbox(ctx, tx, in.MutationInput, in.OpID, in.ClusterID, "CreateCluster", "CreateCluster")
		return err
	})
	return op, replay, err
}

// DeleteCluster marks a cluster DELETING (taking the per-resource lock) and enqueues the workflow.
func (s *Store) DeleteCluster(ctx context.Context, in MutationInput, clusterID, opID string) (op *Operation, replay bool, err error) {
	err = s.InTx(ctx, func(tx pgx.Tx) error {
		existing, err := lookupIdempotency(ctx, tx, in)
		if err != nil {
			return err
		}
		if existing != nil {
			op, replay = existing, true
			return nil
		}
		c, err := scanCluster(tx.QueryRow(ctx, `SELECT `+clusterCols+` FROM clusters WHERE id=$1 AND tenant_id=$2 AND deleted_at IS NULL FOR UPDATE`, clusterID, in.TenantID))
		if err != nil {
			return err
		}
		if c.LockOperationID != nil {
			return &ConflictError{Msg: fmt.Sprintf("cluster has an operation in progress (%s)", *c.LockOperationID)}
		}
		switch c.State {
		case "ACTIVE", "DEGRADED", "FAILED":
		default:
			return &ConflictError{Msg: fmt.Sprintf("cluster cannot be deleted in state %s", c.State)}
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET state='DELETING', lock_operation_id=$2, updated_at=now() WHERE id=$1`, clusterID, opID); err != nil {
			return err
		}
		op, err = insertOpAndOutbox(ctx, tx, in, opID, clusterID, "DeleteCluster", "DeleteCluster")
		return err
	})
	return op, replay, err
}

// ---------- outbox ----------

// OutboxEntry is a pending outbox row.
type OutboxEntry struct {
	ID      int64
	Kind    string
	Payload json.RawMessage
}

// DispatchOutbox claims up to limit pending entries (SKIP LOCKED, so several
// dispatchers can run) and calls fn for each; entries for which fn returns nil
// are marked dispatched. fn must be idempotent: the entry may be redelivered if
// the process dies after fn succeeds but before commit.
func (s *Store) DispatchOutbox(ctx context.Context, limit int, fn func(OutboxEntry) error) (int, error) {
	done := 0
	err := s.InTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id, kind, payload FROM outbox WHERE dispatched_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		var entries []OutboxEntry
		for rows.Next() {
			var e OutboxEntry
			if err := rows.Scan(&e.ID, &e.Kind, &e.Payload); err != nil {
				rows.Close()
				return err
			}
			entries = append(entries, e)
		}
		rows.Close()
		for _, e := range entries {
			if ferr := fn(e); ferr != nil {
				if _, err := tx.Exec(ctx, `UPDATE outbox SET attempts=attempts+1, last_error=$2 WHERE id=$1`, e.ID, ferr.Error()); err != nil {
					return err
				}
				continue
			}
			if _, err := tx.Exec(ctx, `UPDATE outbox SET dispatched_at=now(), attempts=attempts+1 WHERE id=$1`, e.ID); err != nil {
				return err
			}
			done++
		}
		return nil
	})
	return done, err
}

// UpdateSpecInput describes a tenant change to a live cluster's spec (add or remove a user, ...).
type UpdateSpecInput struct {
	MutationInput
	ClusterID string
	OpID      string
	OpType    string // operation type, for example "CreateScramUser"
	// Mutate receives the cluster and returns its new spec. It runs inside the transaction that
	// holds the cluster's row lock, so it sees exactly the spec the new generation is derived from.
	// An error from Mutate is returned as is (the API maps validation errors to 422).
	Mutate func(c *Cluster) (json.RawMessage, error)
}

// UpdateClusterSpec atomically takes the per-resource lock, replaces the spec, bumps the spec
// generation, marks the cluster UPDATING and enqueues the UpdateCluster workflow. The spec is the
// desired state: if the workflow later fails, the spec stays and the operation reports the failure.
func (s *Store) UpdateClusterSpec(ctx context.Context, in UpdateSpecInput) (op *Operation, replay bool, err error) {
	err = s.InTx(ctx, func(tx pgx.Tx) error {
		existing, err := lookupIdempotency(ctx, tx, in.MutationInput)
		if err != nil {
			return err
		}
		if existing != nil {
			op, replay = existing, true
			return nil
		}
		c, err := scanCluster(tx.QueryRow(ctx, `SELECT `+clusterCols+` FROM clusters WHERE id=$1 AND tenant_id=$2 AND deleted_at IS NULL FOR UPDATE`, in.ClusterID, in.TenantID))
		if err != nil {
			return err
		}
		if c.LockOperationID != nil {
			return &ConflictError{Msg: fmt.Sprintf("cluster has an operation in progress (%s)", *c.LockOperationID)}
		}
		if c.State != "ACTIVE" && c.State != "DEGRADED" {
			return &ConflictError{Msg: fmt.Sprintf("cluster cannot be changed in state %s", c.State)}
		}
		newSpec, err := in.Mutate(c)
		if err != nil {
			return err
		}
		// The spec before the change travels with the operation, so a change that cannot get its capacity
		// can be undone (RevertSpec) instead of leaving an unplaceable desired state behind.
		var reqObj map[string]json.RawMessage
		if json.Unmarshal(in.Request, &reqObj) == nil && reqObj != nil {
			reqObj["prior"] = c.Spec
			in.MutationInput.Request, _ = json.Marshal(reqObj)
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET spec=$2, spec_generation=spec_generation+1, state='UPDATING',
			lock_operation_id=$3, updated_at=now() WHERE id=$1`, in.ClusterID, []byte(newSpec), in.OpID); err != nil {
			return err
		}
		op, err = insertOpAndOutbox(ctx, tx, in.MutationInput, in.OpID, in.ClusterID, in.OpType, "UpdateCluster")
		return err
	})
	return op, replay, err
}
