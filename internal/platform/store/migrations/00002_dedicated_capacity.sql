-- +goose Up
-- Dedicated pools are budgets of whole nodes of one instance type per zone; each
-- dedicated resource reserves `nodes` of them (one pod per node). node_cpu_milli and
-- node_mem_mib are the allocatable resources of one node of the pool, used to check
-- that a pod fits on its node shape.
ALTER TABLE node_pools
    ADD COLUMN node_cpu_milli bigint NOT NULL DEFAULT 0,
    ADD COLUMN node_mem_mib   bigint NOT NULL DEFAULT 0;
ALTER TABLE reservation_items
    ADD COLUMN nodes int NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE reservation_items DROP COLUMN nodes;
ALTER TABLE node_pools DROP COLUMN node_cpu_milli, DROP COLUMN node_mem_mib;
