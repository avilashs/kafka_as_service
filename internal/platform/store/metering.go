package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/avilashs/kafka_as_service/internal/metering"
)

// IngestMetering stores records delivered by a cell agent and returns the IDs it now holds (including
// those it already held), which the agent may drop from its spool. A record whose ID does not match its
// content, or that names another cell, is rejected and not acknowledged. A record that exists with a
// different quantity is left as it is and noted in metering_conflicts; it is acknowledged, because
// re-sending it cannot change the answer.
func (s *Store) IngestMetering(ctx context.Context, cellID string, recs []metering.Record) (acked []string, rejected []string, err error) {
	err = s.InTx(ctx, func(tx pgx.Tx) error {
		acked, rejected = acked[:0], rejected[:0]
		for _, r := range recs {
			if r.Cell != cellID || !r.Verify() || !r.End.After(r.Start) {
				rejected = append(rejected, r.ID)
				continue
			}
			labels, _ := json.Marshal(r.Labels)
			if r.Labels == nil {
				labels = []byte(`{}`)
			}
			tag, err := tx.Exec(ctx, `INSERT INTO metering_records (record_id, cell_id, cluster_id, meter, labels, interval_start, interval_end, quantity, unit, schema_version)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (record_id) DO NOTHING`,
				r.ID, r.Cell, r.Cluster, r.Meter, labels, r.Start, r.End, r.Quantity, r.Unit, r.Schema)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				var stored int64
				if err := tx.QueryRow(ctx, `SELECT quantity FROM metering_records WHERE record_id=$1`, r.ID).Scan(&stored); err != nil {
					return fmt.Errorf("record %s: %w", r.ID, err)
				}
				if stored != r.Quantity {
					if _, err := tx.Exec(ctx, `INSERT INTO metering_conflicts (record_id, stored_quantity, received_quantity) VALUES ($1,$2,$3)`, r.ID, stored, r.Quantity); err != nil {
						return err
					}
				}
			}
			acked = append(acked, r.ID)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return acked, rejected, nil
}
