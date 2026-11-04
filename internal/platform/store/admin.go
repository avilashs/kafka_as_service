package store

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// CellSpec registers a cell with its pools (provider admin path).
type CellSpec struct {
	ID             string          `json:"id"`
	Region         string          `json:"region"`
	Cloud          string          `json:"cloud"`
	State          string          `json:"state"`
	OperatorShards int             `json:"operatorShards"`
	Limits         json.RawMessage `json:"limits,omitempty"`
	Env            engine.CellEnv  `json:"env"`
	AgentToken     string          `json:"agentToken"`
	Pools          []PoolSpec      `json:"pools"`
}

// PoolSpec is a node pool with per-zone capacity.
type PoolSpec struct {
	ID           string         `json:"id"`
	Kind         string         `json:"kind"`
	InstanceType string         `json:"instanceType"`
	MaxNodes     int            `json:"maxNodes"`
	Zones        []ZoneCapacity `json:"zones"`
}

// ZoneCapacity is the total capacity of a pool in one zone.
type ZoneCapacity struct {
	Zone      string `json:"zone"`
	CPUMilli  int64  `json:"cpuMilli"`
	MemoryMiB int64  `json:"memoryMiB"`
	EBSMBps   int64  `json:"ebsMBps"`
	Attach    int64  `json:"attach"`
	Nodes     int    `json:"nodes"`
}

// UpsertCell registers or updates a cell, its pools and capacity in one transaction.
func (s *Store) UpsertCell(ctx context.Context, c CellSpec) error {
	if c.State == "" {
		c.State = "ACTIVE"
	}
	if c.OperatorShards == 0 {
		c.OperatorShards = 1
	}
	if len(c.Limits) == 0 {
		c.Limits = json.RawMessage(`{}`)
	}
	env, err := json.Marshal(c.Env)
	if err != nil {
		return err
	}
	return s.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO cells (id, region, cloud, state, limits, env, operator_shards, agent_token_sha256)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (id) DO UPDATE SET region=EXCLUDED.region, cloud=EXCLUDED.cloud, state=EXCLUDED.state,
				limits=EXCLUDED.limits, env=EXCLUDED.env, operator_shards=EXCLUDED.operator_shards,
				agent_token_sha256=EXCLUDED.agent_token_sha256`,
			c.ID, c.Region, c.Cloud, c.State, []byte(c.Limits), env, c.OperatorShards, HashToken(c.AgentToken)); err != nil {
			return err
		}
		for _, p := range c.Pools {
			zones := make([]string, 0, len(p.Zones))
			for _, z := range p.Zones {
				zones = append(zones, z.Zone)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO node_pools (id, cell_id, kind, instance_type, zones, max_nodes)
				VALUES ($1,$2,$3,$4,$5,$6)
				ON CONFLICT (id) DO UPDATE SET kind=EXCLUDED.kind, instance_type=EXCLUDED.instance_type,
					zones=EXCLUDED.zones, max_nodes=EXCLUDED.max_nodes`,
				p.ID, c.ID, p.Kind, p.InstanceType, zones, p.MaxNodes); err != nil {
				return err
			}
			for _, z := range p.Zones {
				if _, err := tx.Exec(ctx, `INSERT INTO pool_capacity (pool_id, zone, cpu_m_total, mem_mib_total, ebs_mbps_total, attach_total, nodes_total)
					VALUES ($1,$2,$3,$4,$5,$6,$7)
					ON CONFLICT (pool_id, zone) DO UPDATE SET cpu_m_total=EXCLUDED.cpu_m_total, mem_mib_total=EXCLUDED.mem_mib_total,
						ebs_mbps_total=EXCLUDED.ebs_mbps_total, attach_total=EXCLUDED.attach_total, nodes_total=EXCLUDED.nodes_total`,
					p.ID, z.Zone, z.CPUMilli, z.MemoryMiB, z.EBSMBps, z.Attach, z.Nodes); err != nil {
					return err
				}
			}
		}
		return audit(ctx, tx, "provider", "", "UpsertCell", "cell/"+c.ID, "succeeded")
	})
}

// UpsertTenant registers a tenant.
func (s *Store) UpsertTenant(ctx context.Context, id, name string, pinnedCell *string) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO tenants (id, name, pinned_cell_id) VALUES ($1,$2,$3)
		ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, pinned_cell_id=EXCLUDED.pinned_cell_id`, id, name, pinnedCell)
	return err
}
