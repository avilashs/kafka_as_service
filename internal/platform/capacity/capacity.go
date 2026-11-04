// Package capacity reserves cell capacity in a transaction before anything is
// provisioned, so concurrent creates cannot oversubscribe a pool (design §10).
package capacity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// ErrExhausted is terminal: no ACTIVE cell can fit the request.
var ErrExhausted = errors.New("no cell has capacity for this cluster")

// ErrUnsupported is terminal: the request needs accounting that is not built yet.
var ErrUnsupported = errors.New("dedicated-tier capacity accounting is not implemented yet (milestone M2)")

// Allocator chooses a cell and reserves capacity in it.
type Allocator struct {
	Store *store.Store
	TTL   time.Duration // HELD reservations expire after this without heartbeats
}

type item struct {
	pool, zone            string
	cpu, mem, ebs, attach int64
	pods                  int
	// maxGroupPods is the largest number of replicas of any single group in this
	// pool and zone; anti-affinity puts each of them on a distinct node.
	maxGroupPods int
	groupPods    map[string]int
}

// Reserve is idempotent per operation: a second call returns the first result.
func (a *Allocator) Reserve(ctx context.Context, opID, clusterID, tenantID string, req engine.PlacementRequest) (engine.Assignment, error) {
	if existing, err := a.existing(ctx, opID); err != nil || existing != nil {
		if existing != nil {
			return *existing, nil
		}
		return engine.Assignment{}, err
	}
	cells, err := a.candidates(ctx, tenantID)
	if err != nil {
		return engine.Assignment{}, err
	}
	for _, cellID := range cells {
		as, err := a.tryCell(ctx, cellID, opID, clusterID, req)
		if errors.Is(err, errNoFit) {
			continue
		}
		if err != nil {
			return engine.Assignment{}, err
		}
		return as, nil
	}
	return engine.Assignment{}, ErrExhausted
}

func (a *Allocator) existing(ctx context.Context, opID string) (*engine.Assignment, error) {
	var raw []byte
	var status string
	err := a.Store.Pool.QueryRow(ctx, `SELECT assignment, status FROM reservations WHERE operation_id=$1`, opID).Scan(&raw, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if status == "RELEASED" {
		return nil, fmt.Errorf("reservation for %s was already released (expired or compensated)", opID)
	}
	var as engine.Assignment
	return &as, json.Unmarshal(raw, &as)
}

// candidates lists ACTIVE cells, honouring tenant pinning, least-loaded first.
func (a *Allocator) candidates(ctx context.Context, tenantID string) ([]string, error) {
	rows, err := a.Store.Pool.Query(ctx, `
		SELECT c.id FROM cells c
		LEFT JOIN tenants t ON t.id = $1
		WHERE c.state = 'ACTIVE' AND (t.pinned_cell_id IS NULL OR t.pinned_cell_id = c.id)
		  AND (SELECT count(*) FROM clusters k WHERE k.cell_id = c.id AND k.deleted_at IS NULL)
		      < COALESCE((c.limits->>'maxClusters')::int, 200)
		ORDER BY (SELECT count(*) FROM clusters k WHERE k.cell_id = c.id AND k.deleted_at IS NULL), c.id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

var errNoFit = errors.New("does not fit")

func (a *Allocator) tryCell(ctx context.Context, cellID, opID, clusterID string, req engine.PlacementRequest) (engine.Assignment, error) {
	var as engine.Assignment
	err := a.Store.InTx(ctx, func(tx pgx.Tx) error {
		items, zones, err := demand(ctx, tx, cellID, req)
		if err != nil {
			return err
		}
		// Lock every capacity row we need, in a fixed order, then re-check under the lock.
		keys := make([]string, 0, len(items))
		for k := range items {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			it := items[k]
			var cpuT, memT, ebsT, attT int64
			var nodesT int
			if err := tx.QueryRow(ctx, `SELECT cpu_m_total, mem_mib_total, ebs_mbps_total, attach_total, nodes_total
				FROM pool_capacity WHERE pool_id=$1 AND zone=$2 FOR UPDATE`, it.pool, it.zone).Scan(&cpuT, &memT, &ebsT, &attT, &nodesT); err != nil {
				return err
			}
			var cpuU, memU, ebsU, attU int64
			if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(i.cpu_m),0), COALESCE(sum(i.mem_mib),0), COALESCE(sum(i.ebs_mbps),0), COALESCE(sum(i.attach),0)
				FROM reservation_items i JOIN reservations r ON r.id = i.reservation_id
				WHERE i.pool_id=$1 AND i.zone=$2 AND r.status <> 'RELEASED'`, it.pool, it.zone).Scan(&cpuU, &memU, &ebsU, &attU); err != nil {
				return err
			}
			if cpuU+it.cpu > cpuT || memU+it.mem > memT || ebsU+it.ebs > ebsT || attU+it.attach > attT || it.maxGroupPods > nodesT {
				return errNoFit
			}
		}
		shard, err := leastLoadedShard(ctx, tx, cellID)
		if err != nil {
			return err
		}
		as = engine.Assignment{CellID: cellID, Zones: zones, OperatorShard: shard}
		raw, _ := json.Marshal(as)
		resID := "res-" + uuid.NewString()
		ttl := a.TTL
		if ttl == 0 {
			ttl = 20 * time.Minute
		}
		if _, err := tx.Exec(ctx, `INSERT INTO reservations (id, operation_id, cluster_id, cell_id, status, assignment, expires_at)
			VALUES ($1,$2,$3,$4,'HELD',$5, now() + $6::interval)`, resID, opID, clusterID, cellID, raw, fmt.Sprintf("%d seconds", int(ttl.Seconds()))); err != nil {
			return err
		}
		for _, k := range keys {
			it := items[k]
			if _, err := tx.Exec(ctx, `INSERT INTO reservation_items (reservation_id, pool_id, zone, cpu_m, mem_mib, ebs_mbps, attach, pods)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, resID, it.pool, it.zone, it.cpu, it.mem, it.ebs, it.attach, it.pods); err != nil {
				return err
			}
		}
		return nil
	})
	return as, err
}

// demand spreads each group's replicas across the pool's zones and sums per (pool, zone).
func demand(ctx context.Context, tx pgx.Tx, cellID string, req engine.PlacementRequest) (map[string]*item, []string, error) {
	items := map[string]*item{}
	var allZones []string
	for _, g := range req.Groups {
		if g.Pool == engine.PoolDedicated {
			return nil, nil, ErrUnsupported
		}
		var poolID string
		err := tx.QueryRow(ctx, `SELECT id FROM node_pools WHERE cell_id=$1 AND kind=$2 ORDER BY id LIMIT 1`, cellID, string(g.Pool)).Scan(&poolID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, errNoFit
		}
		if err != nil {
			return nil, nil, err
		}
		rows, err := tx.Query(ctx, `SELECT zone FROM pool_capacity WHERE pool_id=$1 ORDER BY zone`, poolID)
		if err != nil {
			return nil, nil, err
		}
		var zones []string
		for rows.Next() {
			var z string
			if err := rows.Scan(&z); err != nil {
				rows.Close()
				return nil, nil, err
			}
			zones = append(zones, z)
		}
		rows.Close()
		if len(zones) < g.Zones {
			return nil, nil, errNoFit
		}
		zones = zones[:g.Zones]
		if allZones == nil {
			allZones = zones
		}
		for i := 0; i < g.Replicas; i++ {
			z := zones[i%len(zones)]
			k := poolID + "/" + z
			it := items[k]
			if it == nil {
				it = &item{pool: poolID, zone: z, groupPods: map[string]int{}}
				items[k] = it
			}
			it.cpu += g.PerPod.CPURequestMilli
			it.mem += g.PerPod.MemoryMiB
			it.ebs += g.PerPod.VolumeMBps
			it.attach += g.PerPod.Volumes
			it.pods++
			it.groupPods[g.Name]++
			it.maxGroupPods = max(it.maxGroupPods, it.groupPods[g.Name])
		}
	}
	return items, allZones, nil
}

func leastLoadedShard(ctx context.Context, tx pgx.Tx, cellID string) (int, error) {
	var shards int
	if err := tx.QueryRow(ctx, `SELECT operator_shards FROM cells WHERE id=$1`, cellID).Scan(&shards); err != nil {
		return 0, err
	}
	counts := make([]int, shards)
	rows, err := tx.Query(ctx, `SELECT (assignment->>'operatorShard')::int, count(*) FROM reservations
		WHERE cell_id=$1 AND status <> 'RELEASED' GROUP BY 1`, cellID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var s, n int
		if err := rows.Scan(&s, &n); err != nil {
			return 0, err
		}
		if s >= 0 && s < shards {
			counts[s] = n
		}
	}
	best := 0
	for i, n := range counts {
		if n < counts[best] {
			best = i
		}
	}
	return best, rows.Err()
}

// Heartbeat extends a HELD reservation while its workflow is still working.
func (a *Allocator) Heartbeat(ctx context.Context, opID string) error {
	ttl := a.TTL
	if ttl == 0 {
		ttl = 20 * time.Minute
	}
	_, err := a.Store.Pool.Exec(ctx, `UPDATE reservations SET heartbeat_at=now(), expires_at=now() + $2::interval
		WHERE operation_id=$1 AND status='HELD'`, opID, fmt.Sprintf("%d seconds", int(ttl.Seconds())))
	return err
}

// Sweep releases HELD reservations that expired and whose operation is no longer running.
func (a *Allocator) Sweep(ctx context.Context) (int64, error) {
	tag, err := a.Store.Pool.Exec(ctx, `UPDATE reservations r SET status='RELEASED', released_at=now()
		WHERE r.status='HELD' AND r.expires_at < now()
		  AND NOT EXISTS (SELECT 1 FROM operations o WHERE o.id = r.operation_id AND o.state IN ('PENDING','RUNNING'))`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
