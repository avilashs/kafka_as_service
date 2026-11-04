//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func (e *env) patch(id string, spec map[string]any) apiResp {
	e.t.Helper()
	return e.call("PATCH", "/v1/clusters/"+id, "patch-"+randHex(5), map[string]any{"spec": spec})
}

// mustPatch runs a change to completion.
func (e *env) mustPatch(id string, spec map[string]any) map[string]any {
	e.t.Helper()
	r := e.patch(id, spec)
	if r.Code != http.StatusAccepted {
		e.t.Fatalf("PATCH %v: %d %v", spec, r.Code, r.Body)
	}
	return e.waitOp(r.Body["id"].(string), "SUCCEEDED", 90*time.Second)
}

// roomForBrokers lets each zone's shared pool hold n nodes (the default test pool has one per zone).
func (e *env) shardSharedNodes(n int) {
	e.t.Helper()
	if _, err := st.Pool.Exec(context.Background(), `UPDATE pool_capacity SET nodes_total=$2 WHERE pool_id=$1`, e.cellID+"-shared", n); err != nil {
		e.t.Fatal(err)
	}
}

func activeReservations(t *testing.T, clusterID string) (n int, brokerPods int) {
	t.Helper()
	ctx := context.Background()
	if err := st.Pool.QueryRow(ctx, `SELECT count(*) FROM reservations WHERE cluster_id=$1 AND status<>'RELEASED'`, clusterID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	// Shared brokers are the only pods of this cluster in the shared pool.
	if err := st.Pool.QueryRow(ctx, `SELECT COALESCE(sum(i.pods),0) FROM reservation_items i JOIN reservations r ON r.id=i.reservation_id
		JOIN node_pools p ON p.id=i.pool_id WHERE r.cluster_id=$1 AND r.status<>'RELEASED' AND p.kind='shared'`, clusterID).Scan(&brokerPods); err != nil {
		t.Fatal(err)
	}
	return
}

func nodePoolSpec(t *testing.T, ns, name string) map[string]any {
	t.Helper()
	p, err := dyn.Resource(nodePoolGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m, _, _ := unstructured.NestedMap(p.Object, "spec")
	return m
}

func kafkaCR(t *testing.T, id string) *unstructured.Unstructured {
	t.Helper()
	k, err := dyn.Resource(kafkaGVR).Namespace(id).Get(context.Background(), id, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// Requests that must be refused, and why, before anything is locked or enqueued.
func TestM4ChangeValidation(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	id := e.activeCluster("m4valid")
	ops0 := countRows(t, `SELECT count(*) FROM operations WHERE cluster_id=$1`, id)
	bad := []struct {
		name string
		spec map[string]any
	}{
		{"changes nothing", map[string]any{"brokerCount": 3}},
		{"two kinds at once", map[string]any{"brokerCount": 6, "config": map[string]any{"log.retention.hours": "48"}}},
		{"storage shrink", map[string]any{"storage": map[string]any{"sizeGiB": 50}}},
		{"version downgrade", map[string]any{"kafkaVersion": "4.3.0"}},
		{"unsupported version", map[string]any{"kafkaVersion": "2.8.0"}},
		{"count not a multiple of 3", map[string]any{"brokerCount": 4}},
		{"scale below 3", map[string]any{"brokerCount": 0}},
		{"other tier's broker type", map[string]any{"brokerType": "d.xlarge"}},
		{"unknown broker type", map[string]any{"brokerType": "huge"}},
		{"forbidden config", map[string]any{"config": map[string]any{"ssl.keystore.location": "/x"}}},
		{"out-of-range config", map[string]any{"config": map[string]any{"log.retention.hours": "0"}}},
		{"unknown spec field", map[string]any{"nodeSelector": "x"}},
		{"users in a patch", map[string]any{"users": []any{map[string]any{"name": "x"}}}},
	}
	for _, tc := range bad {
		if r := e.patch(id, tc.spec); r.Code != http.StatusUnprocessableEntity {
			t.Errorf("%s: %d %v, want 422", tc.name, r.Code, r.Body)
		}
	}
	if r := e.call("PATCH", "/v1/clusters/"+id, "", map[string]any{"spec": map[string]any{"brokerCount": 6}}); r.Code != http.StatusBadRequest {
		t.Errorf("no idempotency key: %d", r.Code)
	}
	if r := e.call("PATCH", "/v1/clusters/"+id, "k-"+randHex(3), map[string]any{"nope": 1}); r.Code != http.StatusBadRequest {
		t.Errorf("unknown top-level field: %d", r.Code)
	}
	other := e.otherTenant()
	if r := e.callAs(other, "PATCH", "/v1/clusters/"+id, "k-"+randHex(3), map[string]any{"spec": map[string]any{"brokerCount": 6}}); r.Code != http.StatusNotFound {
		t.Errorf("other tenant: %d", r.Code)
	}
	if n := countRows(t, `SELECT count(*) FROM operations WHERE cluster_id=$1`, id); n != ops0 {
		t.Errorf("refused requests created %d operations", n-ops0)
	}
	// While one change runs a second is refused with 409.
	sim.setReadyDelay(2 * time.Second)
	defer sim.setReadyDelay(0)
	first := e.patch(id, map[string]any{"config": map[string]any{"log.retention.hours": "48"}})
	if first.Code != http.StatusAccepted {
		t.Fatalf("%d %v", first.Code, first.Body)
	}
	if r := e.patch(id, map[string]any{"config": map[string]any{"log.retention.hours": "72"}}); r.Code != http.StatusConflict {
		t.Errorf("concurrent change: %d %v", r.Code, r.Body)
	}
	e.waitOp(first.Body["id"].(string), "SUCCEEDED", 60*time.Second)
	// Same key and body replays; the config merge removes a key with null.
	key := "replay-" + randHex(3)
	body := map[string]any{"spec": map[string]any{"config": map[string]any{"log.retention.hours": nil}}}
	r1 := e.call("PATCH", "/v1/clusters/"+id, key, body)
	r2 := e.call("PATCH", "/v1/clusters/"+id, key, body)
	if r1.Code != 202 || r2.Code != 202 || r1.Body["id"] != r2.Body["id"] || r2.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %v / %d %v", r1.Code, r1.Body, r2.Code, r2.Body)
	}
	e.waitOp(r1.Body["id"].(string), "SUCCEEDED", 60*time.Second)
	c := e.call("GET", "/v1/clusters/"+id, "", nil).Body
	if strings.Contains(fmt.Sprint(c["spec"]), "log.retention.hours") {
		t.Errorf("null did not remove the config key: %v", c["spec"])
	}
}

// Each lifecycle operation reaches the cell as a new generation, the capacity accounting follows, and a
// change that cannot be placed is refused with the cluster untouched.
func TestM4LifecycleOperations(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	e.shardSharedNodes(3)
	id := e.activeCluster("m4ops")
	ctx := context.Background()
	gens := func() int {
		return countRows(t, `SELECT count(*) FROM desired_states WHERE cluster_id=$1 AND NOT tombstone`, id)
	}
	if n, pods := activeReservations(t, id); n != 1 || pods != 3 {
		t.Fatalf("after create: %d reservations, %d shared pods", n, pods)
	}

	t.Run("config", func(t *testing.T) {
		e.t = t
		g := gens()
		e.mustPatch(id, map[string]any{"config": map[string]any{"log.retention.hours": "48", "message.max.bytes": "2097152"}})
		cfg, _, _ := unstructured.NestedMap(kafkaCR(t, id).Object, "spec", "kafka", "config")
		if cfg["log.retention.hours"] != int64(48) || cfg["message.max.bytes"] != int64(2097152) {
			t.Errorf("config in the cell: %v", cfg)
		}
		if gens() != g+1 {
			t.Errorf("generations %d -> %d", g, gens())
		}
		if n, pods := activeReservations(t, id); n != 1 || pods != 3 {
			t.Errorf("a config change must not touch capacity: %d/%d", n, pods)
		}
	})

	t.Run("storage", func(t *testing.T) {
		e.t = t
		e.mustPatch(id, map[string]any{"storage": map[string]any{"sizeGiB": 200, "maxSizeGiB": 800, "autoExpand": true}})
		st, _, _ := unstructured.NestedMap(nodePoolSpec(t, id, "brokers"), "storage")
		vols := fmt.Sprint(st)
		if !strings.Contains(vols, "200Gi") {
			t.Errorf("broker storage in the cell: %s", vols)
		}
	})

	t.Run("size", func(t *testing.T) {
		e.t = t
		before := countRows(t, `SELECT COALESCE(sum(i.mem_mib),0)::int FROM reservation_items i JOIN reservations r ON r.id=i.reservation_id WHERE r.cluster_id=$1 AND r.status<>'RELEASED'`, id)
		e.mustPatch(id, map[string]any{"brokerType": "s.medium"})
		res, _, _ := unstructured.NestedMap(nodePoolSpec(t, id, "brokers"), "resources")
		if !strings.Contains(fmt.Sprint(res), "12288Mi") && !strings.Contains(fmt.Sprint(res), "12Gi") {
			t.Errorf("broker resources in the cell: %v", res)
		}
		after := countRows(t, `SELECT COALESCE(sum(i.mem_mib),0)::int FROM reservation_items i JOIN reservations r ON r.id=i.reservation_id WHERE r.cluster_id=$1 AND r.status<>'RELEASED'`, id)
		if n, _ := activeReservations(t, id); n != 1 || after <= before {
			t.Errorf("reservation after resize: %d reservations, memory %d -> %d MiB (must be one, larger)", n, before, after)
		}
	})

	t.Run("scale out", func(t *testing.T) {
		e.t = t
		e.mustPatch(id, map[string]any{"brokerCount": 6})
		if r, _, _ := unstructured.NestedInt64(nodePoolSpec(t, id, "brokers"), "replicas"); r != 6 {
			t.Errorf("replicas in the cell: %d", r)
		}
		if n, pods := activeReservations(t, id); n != 1 || pods != 6 {
			t.Errorf("reservation after scale out: %d reservations, %d shared pods", n, pods)
		}
	})

	t.Run("scale in", func(t *testing.T) {
		e.t = t
		e.mustPatch(id, map[string]any{"brokerCount": 3})
		if r, _, _ := unstructured.NestedInt64(nodePoolSpec(t, id, "brokers"), "replicas"); r != 3 {
			t.Errorf("replicas in the cell: %d", r)
		}
		if n, pods := activeReservations(t, id); n != 1 || pods != 3 {
			t.Errorf("reservation after scale in: %d reservations, %d shared pods (capacity must be released)", n, pods)
		}
	})

	t.Run("capacity exhausted", func(t *testing.T) {
		e.t = t
		// The pool can hold exactly what the cluster has; growing to 6 brokers needs more nodes per zone than exist.
		e.shardSharedNodes(1)
		defer e.shardSharedNodes(3)
		r := e.patch(id, map[string]any{"brokerCount": 6})
		if r.Code != http.StatusAccepted {
			t.Fatalf("%d %v", r.Code, r.Body)
		}
		op := e.call("GET", "/v1/operations/"+r.Body["id"].(string), "", nil)
		deadline := time.Now().Add(60 * time.Second)
		for op.Body["state"] != "FAILED" && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
			op = e.call("GET", "/v1/operations/"+r.Body["id"].(string), "", nil)
		}
		if op.Body["state"] != "FAILED" {
			t.Fatalf("operation: %v", op.Body)
		}
		c := e.call("GET", "/v1/clusters/"+id, "", nil).Body
		spec := c["spec"].(map[string]any)
		if spec["brokerCount"] != float64(3) || c["state"] != "ACTIVE" {
			t.Errorf("a change that cannot be placed must leave the cluster as it was: %v", c)
		}
		if n, pods := activeReservations(t, id); n != 1 || pods != 3 {
			t.Errorf("reservations after the refused change: %d/%d", n, pods)
		}
		if r, _, _ := unstructured.NestedInt64(nodePoolSpec(t, id, "brokers"), "replicas"); r != 3 {
			t.Errorf("replicas in the cell: %d (nothing may have been published)", r)
		}
	})
	_ = ctx
}

// The version path: create at 4.3.0, upgrade, no downgrade.
func TestM4VersionUpgrade(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	body := createBody("m4ver")
	body["spec"].(map[string]any)["kafkaVersion"] = "4.3.0"
	r := e.call("POST", "/v1/clusters", "ver-"+randHex(3), body)
	if r.Code != http.StatusAccepted {
		t.Fatalf("%d %v", r.Code, r.Body)
	}
	e.waitOp(r.Body["id"].(string), "SUCCEEDED", 60*time.Second)
	id := r.Body["clusterId"].(string)
	e.mustPatch(id, map[string]any{"kafkaVersion": "4.3.1"})
	if v, _, _ := unstructured.NestedString(kafkaCR(t, id).Object, "spec", "kafka", "version"); v != "4.3.1" {
		t.Errorf("version in the cell: %s", v)
	}
	if img, _, _ := unstructured.NestedString(kafkaCR(t, id).Object, "spec", "kafka", "image"); !strings.HasSuffix(img, ":4.3.1") {
		t.Errorf("the image must follow the version: %s", img)
	}
	if r := e.patch(id, map[string]any{"kafkaVersion": "4.3.0"}); r.Code != http.StatusUnprocessableEntity {
		t.Errorf("downgrade: %d", r.Code)
	}
}

// A worker killed at any point of an update resumes it without duplicating anything.
func TestM4WorkerCrashDuringScale(t *testing.T) {
	points := []string{"PrepareUpdate", "MarkStep:publish-desired-state", "RenderAndPublish", "MarkStep:wait-ready", "WaitForCondition:Ready", "FinishUpdate"}
	e := newEnv(t)
	e.shardSharedNodes(3)
	e.startWorker()
	id := e.activeCluster("m4crash")
	want := 3
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			e.t = t
			e.killWorkers()
			dir := t.TempDir()
			w1 := e.startWorker("STREAMCELL_FAULT_CRASH_AFTER="+point, "STREAMCELL_FAULT_DIR="+dir)
			gensBefore := countRows(t, `SELECT count(*) FROM desired_states WHERE cluster_id=$1`, id)
			want = 9 - want // 3 <-> 6
			r := e.patch(id, map[string]any{"brokerCount": want})
			if r.Code != http.StatusAccepted {
				t.Fatalf("%d %v", r.Code, r.Body)
			}
			select {
			case <-w1.done:
			case <-time.After(60 * time.Second):
				t.Fatalf("fault point %s did not fire", point)
			}
			if code := w1.waitExit(t, time.Second); code != 137 {
				t.Fatalf("exit code %d, want 137", code)
			}
			e.startWorker("STREAMCELL_FAULT_CRASH_AFTER="+point, "STREAMCELL_FAULT_DIR="+dir)
			e.waitOp(r.Body["id"].(string), "SUCCEEDED", 90*time.Second)
			if g := countRows(t, `SELECT count(*) FROM desired_states WHERE cluster_id=$1`, id); g != gensBefore+1 {
				t.Errorf("desired-state rows %d -> %d, want exactly one more", gensBefore, g)
			}
			if n, pods := activeReservations(t, id); n != 1 || pods != want {
				t.Errorf("reservations %d, shared pods %d, want 1 and %d", n, pods, want)
			}
			if h := countRows(t, `SELECT count(*) FROM reservations WHERE cluster_id=$1 AND status='HELD'`, id); h != 0 {
				t.Errorf("%d reservations still HELD", h)
			}
			if rr, _, _ := unstructured.NestedInt64(nodePoolSpec(t, id, "brokers"), "replicas"); int(rr) != want {
				t.Errorf("replicas in the cell: %d", rr)
			}
		})
	}
}
