-- +goose Up
-- Closed metering intervals, one row per record. record_id is the deterministic hash computed by the
-- agent and re-verified here; inserting it again is a no-op. A second delivery with a different quantity
-- is not applied but recorded in metering_conflicts (corrections are separate adjustment records).
CREATE TABLE metering_records (
    record_id      text PRIMARY KEY,
    cell_id        text NOT NULL REFERENCES cells (id),
    cluster_id     text NOT NULL,
    meter          text NOT NULL,
    labels         jsonb NOT NULL DEFAULT '{}',
    interval_start timestamptz NOT NULL,
    interval_end   timestamptz NOT NULL,
    quantity       bigint NOT NULL,
    unit           text NOT NULL,
    schema_version int NOT NULL,
    received_at    timestamptz NOT NULL DEFAULT now(),
    CHECK (interval_end > interval_start)
);
CREATE INDEX metering_cluster_time ON metering_records (cluster_id, meter, interval_start);

CREATE TABLE metering_conflicts (
    id                bigserial PRIMARY KEY,
    record_id         text NOT NULL,
    stored_quantity   bigint NOT NULL,
    received_quantity bigint NOT NULL,
    received_at       timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE metering_conflicts, metering_records;
