-- +goose Up
-- Round trips measured by the per-cell canary through the public endpoint (design §17).
CREATE TABLE canary_results (
    id         bigserial PRIMARY KEY,
    cluster_id text NOT NULL,
    cell_id    text NOT NULL REFERENCES cells (id),
    at         timestamptz NOT NULL,
    attempts   int NOT NULL,
    successes  int NOT NULL,
    p99_ms     int NOT NULL DEFAULT 0,
    CHECK (successes >= 0 AND successes <= attempts)
);
CREATE INDEX canary_results_cluster_at ON canary_results (cluster_id, at);

-- Unhealthy signals the health evaluator has seen, with the time each began. A signal only degrades a
-- cluster once it has lasted its configured duration; a signal that stops is deleted.
CREATE TABLE cluster_health (
    cluster_id text NOT NULL,
    kind       text NOT NULL,
    since      timestamptz NOT NULL,
    detail     text NOT NULL DEFAULT '',
    PRIMARY KEY (cluster_id, kind)
);

-- +goose Down
DROP TABLE cluster_health, canary_results;
