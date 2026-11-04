-- +goose Up
CREATE TABLE tenants (
    id             text PRIMARY KEY,
    name           text NOT NULL,
    pinned_cell_id text,
    limits         jsonb NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE cells (
    id                 text PRIMARY KEY,
    region             text NOT NULL,
    cloud              text NOT NULL,
    state              text NOT NULL CHECK (state IN ('ACTIVE', 'DRAINING', 'SUSPENDED')),
    limits             jsonb NOT NULL DEFAULT '{}',
    env                jsonb NOT NULL DEFAULT '{}',
    operator_shards    int NOT NULL DEFAULT 1 CHECK (operator_shards > 0),
    revision           bigint NOT NULL DEFAULT 0,
    agent_token_sha256 bytea NOT NULL,
    last_heartbeat_at  timestamptz,
    last_inventory     jsonb,
    created_at         timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE tenants ADD CONSTRAINT tenants_pinned_cell_fk FOREIGN KEY (pinned_cell_id) REFERENCES cells (id);

CREATE TABLE node_pools (
    id               text PRIMARY KEY,
    cell_id          text NOT NULL REFERENCES cells (id),
    kind             text NOT NULL CHECK (kind IN ('control', 'shared', 'dedicated')),
    owner_cluster_id text,
    instance_type    text NOT NULL,
    zones            text[] NOT NULL,
    max_nodes        int NOT NULL
);

-- Capacity totals per pool and zone. Free capacity is always computed as
-- total minus the sum of active reservation items, under a row lock on these rows,
-- so there are no counters that can drift.
CREATE TABLE pool_capacity (
    pool_id        text NOT NULL REFERENCES node_pools (id),
    zone           text NOT NULL,
    cpu_m_total    bigint NOT NULL,
    mem_mib_total  bigint NOT NULL,
    ebs_mbps_total bigint NOT NULL,
    attach_total   bigint NOT NULL,
    nodes_total    int NOT NULL,
    PRIMARY KEY (pool_id, zone)
);

CREATE TABLE clusters (
    id                text PRIMARY KEY,
    tenant_id         text NOT NULL REFERENCES tenants (id),
    name              text NOT NULL,
    engine            text NOT NULL,
    tier              text NOT NULL CHECK (tier IN ('shared', 'dedicated')),
    cell_id           text REFERENCES cells (id),
    state             text NOT NULL CHECK (state IN ('CREATING', 'ACTIVE', 'UPDATING', 'DEGRADED', 'FAILED', 'DELETING')),
    state_reason      text,
    spec              jsonb NOT NULL,
    spec_generation   bigint NOT NULL DEFAULT 1,
    assignment        jsonb,
    lock_operation_id text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    deleted_at        timestamptz
);
CREATE UNIQUE INDEX clusters_tenant_name_live ON clusters (tenant_id, name) WHERE deleted_at IS NULL;

CREATE TABLE reservations (
    id           text PRIMARY KEY,
    operation_id text NOT NULL UNIQUE,
    cluster_id   text NOT NULL REFERENCES clusters (id),
    cell_id      text NOT NULL REFERENCES cells (id),
    status       text NOT NULL CHECK (status IN ('HELD', 'COMMITTED', 'RELEASED')),
    assignment   jsonb NOT NULL,
    expires_at   timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    created_at   timestamptz NOT NULL DEFAULT now(),
    released_at  timestamptz
);
CREATE INDEX reservations_active ON reservations (cell_id) WHERE status <> 'RELEASED';

CREATE TABLE reservation_items (
    reservation_id text NOT NULL REFERENCES reservations (id),
    pool_id        text NOT NULL,
    zone           text NOT NULL,
    cpu_m          bigint NOT NULL,
    mem_mib        bigint NOT NULL,
    ebs_mbps       bigint NOT NULL,
    attach         bigint NOT NULL,
    pods           int NOT NULL,
    PRIMARY KEY (reservation_id, pool_id, zone),
    FOREIGN KEY (pool_id, zone) REFERENCES pool_capacity (pool_id, zone)
);

CREATE TABLE desired_states (
    cluster_id    text NOT NULL REFERENCES clusters (id),
    generation    bigint NOT NULL,
    cell_id       text NOT NULL REFERENCES cells (id),
    cell_revision bigint NOT NULL,
    bundle        jsonb,
    bundle_digest text NOT NULL,
    tombstone     boolean NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster_id, generation)
);
CREATE UNIQUE INDEX desired_states_cell_rev ON desired_states (cell_id, cell_revision);

CREATE TABLE observed_status (
    cluster_id          text PRIMARY KEY REFERENCES clusters (id),
    cell_id             text NOT NULL REFERENCES cells (id),
    observed_generation bigint NOT NULL,
    observed            jsonb NOT NULL,
    reported_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE operations (
    id           text PRIMARY KEY,
    tenant_id    text NOT NULL REFERENCES tenants (id),
    cluster_id   text NOT NULL REFERENCES clusters (id),
    type         text NOT NULL,
    state        text NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    step         text,
    request      jsonb NOT NULL,
    error        jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);
CREATE INDEX operations_cluster ON operations (cluster_id, created_at);

CREATE TABLE idempotency_keys (
    tenant_id    text NOT NULL REFERENCES tenants (id),
    key          text NOT NULL,
    request_hash text NOT NULL,
    operation_id text NOT NULL REFERENCES operations (id),
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, key)
);

CREATE TABLE outbox (
    id            bigserial PRIMARY KEY,
    kind          text NOT NULL,
    payload       jsonb NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    dispatched_at timestamptz,
    attempts      int NOT NULL DEFAULT 0,
    last_error    text
);
CREATE INDEX outbox_pending ON outbox (id) WHERE dispatched_at IS NULL;

CREATE TABLE audit_log (
    id             bigserial PRIMARY KEY,
    at             timestamptz NOT NULL DEFAULT now(),
    actor          text NOT NULL,
    tenant_id      text,
    action         text NOT NULL,
    resource       text NOT NULL,
    request_digest text,
    result         text NOT NULL
);

-- +goose Down
DROP TABLE audit_log, outbox, idempotency_keys, operations, observed_status, desired_states,
    reservation_items, reservations, clusters, pool_capacity, node_pools, tenants, cells CASCADE;
