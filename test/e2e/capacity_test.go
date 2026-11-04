//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/capacity"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/ids"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// Concurrent creates cannot oversubscribe a pool (preview of the M2 acceptance test).
func TestConcurrentReservationsDoNotOversubscribe(t *testing.T) {
	ctx := context.Background()
	const fits, attempts = 10, 50
	cellID, tenant := "cell-"+randHex(3), "tenant-"+randHex(3)
	zones := []string{"z1", "z2", "z3"}
	zc := func(memPerZone int64) (out []store.ZoneCapacity) {
		for _, z := range zones {
			out = append(out, store.ZoneCapacity{Zone: z, CPUMilli: 1 << 30, MemoryMiB: memPerZone, EBSMBps: 1 << 30, Attach: 1 << 20, Nodes: 1})
		}
		return
	}
	// s.small: 6144 MiB per broker, one broker per zone per cluster => exactly `fits` clusters.
	if err := st.UpsertCell(ctx, store.CellSpec{ID: cellID, Region: "r", Cloud: "kind", AgentToken: randHex(8),
		Env: engine.CellEnv{Domain: "d", Zones: zones},
		Pools: []store.PoolSpec{
			{ID: cellID + "-control", Kind: "control", InstanceType: "x", MaxNodes: 3, Zones: zc(1 << 30)},
			{ID: cellID + "-shared", Kind: "shared", InstanceType: "x", MaxNodes: 3, Zones: zc(fits * 6144)},
		}}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertTenant(ctx, tenant, tenant, &cellID); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(kafka.Spec{KafkaVersion: "4.3.1", BrokerType: "s.small", BrokerCount: 3, Storage: kafka.StorageSpec{SizeGiB: 10}})
	req, err := kafka.New().PlanPlacement(engine.ResourceSpec{Tier: engine.TierShared, Spec: raw})
	if err != nil {
		t.Fatal(err)
	}
	alloc := &capacity.Allocator{Store: st}
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, exhausted, other := 0, 0, []error{}
	for i := 0; i < attempts; i++ {
		clusterID, opID := ids.Cluster(), ids.Operation()
		if _, err := st.Pool.Exec(ctx, `INSERT INTO clusters (id, tenant_id, name, engine, tier, state, spec) VALUES ($1,$2,$3,'kafka','shared','CREATING',$4)`,
			clusterID, tenant, clusterID, raw); err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := alloc.Reserve(ctx, opID, clusterID, tenant, req)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, capacity.ErrExhausted):
				exhausted++
			default:
				other = append(other, err)
			}
		}()
	}
	wg.Wait()
	if len(other) > 0 {
		t.Fatalf("unexpected errors: %v", other)
	}
	if ok != fits || exhausted != attempts-fits {
		t.Fatalf("reservations: %d ok, %d exhausted; want %d ok, %d exhausted", ok, exhausted, fits, attempts-fits)
	}
	for _, z := range zones {
		used := countRows(t, `SELECT COALESCE(sum(i.mem_mib),0)::int FROM reservation_items i JOIN reservations r ON r.id=i.reservation_id
			WHERE i.pool_id=$1 AND i.zone=$2 AND r.status<>'RELEASED'`, cellID+"-shared", z)
		if used > fits*6144 {
			t.Errorf("zone %s oversubscribed: %d MiB > %d", z, used, fits*6144)
		}
	}
}
