# ADR-0017: Idempotent metering with deterministic record IDs

**Status:** Proposed

## Decision

The cell agent produces hourly records:

- `broker_hours{size}`
- `storage_gb_hours`
- `data_in_bytes` and `data_out_bytes` (client-facing)
- `privatelink_bytes`

Each record has `record_id = sha256(cell, cluster, meter, labels, interval, schema_version)`. Records are spooled to a WAL on a PVC until the control plane acknowledges them, then inserted with `ON CONFLICT DO NOTHING`.

If a record arrives with the same ID but a different quantity, it is a conflict. It is flagged and never overwrites the original. Corrections are separate adjustment records.

## Consequences

- Retries, agent restarts and control-plane outages cannot double-bill.
- M5 reconciles totals against a ground-truth timeline.
