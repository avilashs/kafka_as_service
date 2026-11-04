package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// CellByToken authenticates a cell agent token.
func (s *Store) CellByToken(ctx context.Context, token string) (string, error) {
	var id string
	err := s.Pool.QueryRow(ctx, `SELECT id FROM cells WHERE agent_token_sha256=$1`, HashToken(token)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return id, err
}

// DesiredEntry is one cluster's latest desired state as served to the agent.
type DesiredEntry struct {
	ClusterID    string         `json:"clusterId"`
	Generation   int64          `json:"generation"`
	CellRevision int64          `json:"cellRevision"`
	Tombstone    bool           `json:"tombstone"`
	BundleDigest string         `json:"bundleDigest"`
	Bundle       *engine.Bundle `json:"bundle,omitempty"`
}

// DesiredSince returns the latest desired entry of every cluster in the cell whose
// latest entry has cell_revision > since, ordered by revision, plus the cell's
// current revision. With since=0 (agent bootstrap), clusters whose deletion has
// completed are omitted.
func (s *Store) DesiredSince(ctx context.Context, cellID string, since int64) ([]DesiredEntry, int64, error) {
	var current int64
	if err := s.Pool.QueryRow(ctx, `SELECT revision FROM cells WHERE id=$1`, cellID).Scan(&current); err != nil {
		return nil, 0, err
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT d.cluster_id, d.generation, d.cell_revision, d.tombstone, d.bundle_digest, d.bundle
		FROM (SELECT DISTINCT ON (cluster_id) * FROM desired_states WHERE cell_id=$1 ORDER BY cluster_id, generation DESC) d
		JOIN clusters c ON c.id = d.cluster_id
		WHERE d.cell_revision > $2 AND NOT ($2 = 0 AND c.deleted_at IS NOT NULL)
		ORDER BY d.cell_revision`, cellID, since)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []DesiredEntry
	for rows.Next() {
		var e DesiredEntry
		var raw []byte
		if err := rows.Scan(&e.ClusterID, &e.Generation, &e.CellRevision, &e.Tombstone, &e.BundleDigest, &raw); err != nil {
			return nil, 0, err
		}
		if len(raw) > 0 {
			e.Bundle = &engine.Bundle{}
			if err := json.Unmarshal(raw, e.Bundle); err != nil {
				return nil, 0, err
			}
		}
		out = append(out, e)
	}
	return out, current, rows.Err()
}

// StatusReport is one cluster's observation from the agent.
type StatusReport struct {
	ClusterID string          `json:"clusterId"`
	Observed  engine.Observed `json:"observed"`
}

// ReportStatus stores observations. Reports for clusters not assigned to the cell
// are ignored; an older generation never overwrites a newer one.
func (s *Store) ReportStatus(ctx context.Context, cellID string, reports []StatusReport) (accepted int, err error) {
	err = s.InTx(ctx, func(tx pgx.Tx) error {
		for _, r := range reports {
			raw, err := json.Marshal(r.Observed)
			if err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `
				INSERT INTO observed_status (cluster_id, cell_id, observed_generation, observed, reported_at)
				SELECT $1, $2, $3, $4, now() FROM clusters WHERE id=$1 AND cell_id=$2
				ON CONFLICT (cluster_id) DO UPDATE SET observed_generation=EXCLUDED.observed_generation,
					observed=EXCLUDED.observed, reported_at=now()
				WHERE observed_status.observed_generation <= EXCLUDED.observed_generation`,
				r.ClusterID, cellID, r.Observed.Generation, raw)
			if err != nil {
				return err
			}
			accepted += int(tag.RowsAffected())
		}
		return nil
	})
	return accepted, err
}

// Heartbeat records agent liveness and inventory.
func (s *Store) Heartbeat(ctx context.Context, cellID string, inventory json.RawMessage) error {
	var inv any
	if len(inventory) > 0 && string(inventory) != "null" {
		inv = []byte(inventory)
	}
	_, err := s.Pool.Exec(ctx, `UPDATE cells SET last_heartbeat_at=now(), last_inventory=COALESCE($2::jsonb, last_inventory) WHERE id=$1`, cellID, inv)
	return err
}
