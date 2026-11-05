package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// CanaryResult is one report window of the canary for one cluster.
type CanaryResult struct {
	ClusterID string    `json:"clusterId"`
	At        time.Time `json:"at"`
	Attempts  int       `json:"attempts"`
	Successes int       `json:"successes"`
	P99ms     int       `json:"p99Ms"`
}

// InsertCanary stores canary reports for clusters of the cell; reports for other clusters are dropped.
func (s *Store) InsertCanary(ctx context.Context, cellID string, rs []CanaryResult) (int, error) {
	n := 0
	err := s.InTx(ctx, func(tx pgx.Tx) error {
		for _, r := range rs {
			if r.Attempts < 0 || r.Successes < 0 || r.Successes > r.Attempts {
				continue
			}
			tag, err := tx.Exec(ctx, `INSERT INTO canary_results (cluster_id, cell_id, at, attempts, successes, p99_ms)
				SELECT $1,$2,$3,$4,$5,$6 FROM clusters WHERE id=$1 AND cell_id=$2`, r.ClusterID, cellID, r.At, r.Attempts, r.Successes, r.P99ms)
			if err != nil {
				return err
			}
			n += int(tag.RowsAffected())
		}
		// keep a day of history, not more
		_, err := tx.Exec(ctx, `DELETE FROM canary_results WHERE at < now() - interval '1 day'`)
		return err
	})
	return n, err
}

// CanaryWindow sums the canary results of a cluster reported in [from, to].
func (s *Store) CanaryWindow(ctx context.Context, clusterID string, from, to time.Time) (attempts, successes int, last time.Time, err error) {
	var l *time.Time
	err = s.Pool.QueryRow(ctx, `SELECT COALESCE(sum(attempts),0), COALESCE(sum(successes),0), max(at)
		FROM canary_results WHERE cluster_id=$1 AND at >= $2 AND at <= $3`, clusterID, from, to).Scan(&attempts, &successes, &l)
	if l != nil {
		last = *l
	}
	return
}

// CellCluster is a live cluster of a cell with what the canary needs to reach it.
type CellCluster struct {
	Cluster  *Cluster
	Observed *engine.Observed
}

// ListCellClusters returns the clusters of a cell that are serving (ACTIVE or DEGRADED) with their latest observation.
func (s *Store) ListCellClusters(ctx context.Context, cellID string) ([]CellCluster, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+clusterColsQualified+`, o.observed
		FROM clusters c LEFT JOIN observed_status o ON o.cluster_id = c.id
		WHERE c.cell_id=$1 AND c.deleted_at IS NULL AND c.state IN ('ACTIVE','DEGRADED') ORDER BY c.id`, cellID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CellCluster
	for rows.Next() {
		var raw []byte
		c, err := scanClusterInto(rows, &raw)
		if err != nil {
			return nil, err
		}
		cc := CellCluster{Cluster: c}
		if len(raw) > 0 {
			var o engine.Observed
			if json.Unmarshal(raw, &o) == nil {
				cc.Observed = &o
			}
		}
		out = append(out, cc)
	}
	return out, rows.Err()
}

// HealthCandidate is a cluster the health evaluator may judge: serving, not being changed.
type HealthCandidate struct {
	Cluster    *Cluster
	Observed   *engine.Observed
	ReportedAt time.Time
}

// HealthCandidates lists ACTIVE and DEGRADED clusters without an operation in flight.
func (s *Store) HealthCandidates(ctx context.Context) ([]HealthCandidate, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+clusterColsQualified+`, o.observed, o.reported_at
		FROM clusters c LEFT JOIN observed_status o ON o.cluster_id = c.id
		WHERE c.deleted_at IS NULL AND c.state IN ('ACTIVE','DEGRADED') AND c.lock_operation_id IS NULL ORDER BY c.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HealthCandidate
	for rows.Next() {
		var raw []byte
		var rep *time.Time
		c, err := scanClusterInto(rows, &raw, &rep)
		if err != nil {
			return nil, err
		}
		hc := HealthCandidate{Cluster: c}
		if rep != nil {
			hc.ReportedAt = *rep
		}
		if len(raw) > 0 {
			var o engine.Observed
			if json.Unmarshal(raw, &o) == nil {
				hc.Observed = &o
			}
		}
		out = append(out, hc)
	}
	return out, rows.Err()
}

// HealthSignal is one unhealthy condition of a cluster.
type HealthSignal struct {
	Kind   string
	Since  time.Time
	Detail string
}

// SyncHealth records the signals currently active for a cluster and returns them with the time each
// began: a signal that was already recorded keeps its start, a signal that is gone is deleted.
func (s *Store) SyncHealth(ctx context.Context, clusterID string, now time.Time, active map[string]string) ([]HealthSignal, error) {
	var out []HealthSignal
	err := s.InTx(ctx, func(tx pgx.Tx) error {
		out = out[:0]
		for kind, detail := range active {
			var since time.Time
			err := tx.QueryRow(ctx, `INSERT INTO cluster_health (cluster_id, kind, since, detail) VALUES ($1,$2,$3,$4)
				ON CONFLICT (cluster_id, kind) DO UPDATE SET detail=EXCLUDED.detail RETURNING since`, clusterID, kind, now, detail).Scan(&since)
			if err != nil {
				return err
			}
			out = append(out, HealthSignal{Kind: kind, Since: since, Detail: detail})
		}
		kinds := make([]string, 0, len(active))
		for k := range active {
			kinds = append(kinds, k)
		}
		_, err := tx.Exec(ctx, `DELETE FROM cluster_health WHERE cluster_id=$1 AND NOT (kind = ANY($2))`, clusterID, kinds)
		return err
	})
	return out, err
}

// SetHealthState moves a cluster between ACTIVE and DEGRADED, only if it still is in `from` and no
// operation holds it. It returns whether the state changed.
func (s *Store) SetHealthState(ctx context.Context, clusterID, from, to, reason string) (bool, error) {
	changed := false
	err := s.InTx(ctx, func(tx pgx.Tx) error {
		var tenant string
		var r any
		if reason != "" {
			r = reason
		}
		err := tx.QueryRow(ctx, `UPDATE clusters SET state=$3, state_reason=$4, updated_at=now()
			WHERE id=$1 AND state=$2 AND lock_operation_id IS NULL AND deleted_at IS NULL RETURNING tenant_id`, clusterID, from, to, r).Scan(&tenant)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		changed = true
		return audit(ctx, tx, "system", tenant, "Health", "cluster/"+clusterID, from+" -> "+to+": "+reason)
	})
	return changed, err
}
