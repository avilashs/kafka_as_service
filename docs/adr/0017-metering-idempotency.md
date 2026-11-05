# ADR-0017: Idempotent metering with deterministic record IDs

**Status:** Accepted for the four meters that exist (M5); `privatelink_bytes` and the billing adjustments are not built.

## Decision

The cell agent produces hourly records:

- `broker_hours{size}`
- `storage_gb_hours`
- `data_in_bytes` and `data_out_bytes` (client-facing)
- `privatelink_bytes`

Each record has `record_id = sha256(cell, cluster, meter, labels, interval_start, interval_end, schema_version)` (never the quantity). Records are spooled to a WAL on a PVC until the control plane acknowledges them, then inserted with `ON CONFLICT DO NOTHING`.

If a record arrives with the same ID but a different quantity, it is a conflict. It is flagged and never overwrites the original. Corrections are separate adjustment records.

## Consequences

- Retries, agent restarts and control-plane outages cannot double-bill.
- M5 reconciles totals against a ground-truth timeline.

## What M5 added to this decision

- **Accounting is in integer seconds / bytes.** Gauges (broker-seconds, GiB-seconds) are integrated between samples; counters (bytes) are summed from deltas of each broker's cumulative counter, a reset (restart) counts the new value as the delta. Intervals are 5 minutes in the kind cell, an hour in production; a sample that straddles a boundary is split exactly.
- **Gaps are bounded.** A gap between samples longer than `MaxGap` is not carried over (a cell that was down is not billed for the time it did not report); shorter gaps carry the last value forward.
- **Delivery.** `POST /cell/v1/metering` returns the IDs it stored or already had; only those are removed from the WAL. A record whose ID exists with another quantity lands in `metering_conflicts` and nothing is overwritten.
- **Evidence.** Unit tests (boundary split, resets, gaps, restart equivalence, spool) and an envtest suite with an independent oracle under duplicate deliveries, agent restarts, a failing control plane. On a real cell (kind): `broker_hours` and `storage_gb_hours` match a timeline sampled by the test itself for four complete intervals, one of them with a broker lost, across a 60 s control-plane freeze and an agent restart. See [M5](../milestones/m5.md) for the data meters.
