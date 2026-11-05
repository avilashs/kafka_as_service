//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// assertClusterObjects checks the cell contains exactly one set of the cluster's objects.
func assertClusterObjects(t *testing.T, clusterID string) {
	t.Helper()
	ctx := context.Background()
	sel := metav1.ListOptions{LabelSelector: "streamcell.dev/inventory=" + clusterID}
	nss, err := kube.CoreV1().Namespaces().List(ctx, sel)
	if err != nil || len(nss.Items) != 1 {
		t.Fatalf("want 1 namespace for %s, got %d (%v)", clusterID, len(nss.Items), err)
	}
	ks, err := dyn.Resource(kafkaGVR).Namespace(clusterID).List(ctx, sel)
	if err != nil || len(ks.Items) != 1 {
		t.Fatalf("want 1 Kafka, got %d (%v)", len(ks.Items), err)
	}
	pools, err := dyn.Resource(nodePoolGVR).Namespace(clusterID).List(ctx, sel)
	if err != nil || len(pools.Items) != 2 {
		t.Fatalf("want 2 KafkaNodePools, got %d (%v)", len(pools.Items), err)
	}
	nps, err := kube.NetworkingV1().NetworkPolicies(clusterID).List(ctx, sel)
	if err != nil || len(nps.Items) != 6 {
		t.Fatalf("want 6 NetworkPolicies, got %d (%v)", len(nps.Items), err)
	}
	for _, p := range pools.Items {
		if p.GetLabels()["strimzi.io/cluster"] != clusterID {
			t.Errorf("node pool %s not bound to cluster", p.GetName())
		}
	}
}

// assertNoDuplicates checks every side effect of one create happened exactly once.
func assertNoDuplicates(t *testing.T, e *env, opID, clusterID string) {
	t.Helper()
	checks := []struct {
		what string
		q    string
		args []any
		want int
	}{
		{"clusters", `SELECT count(*) FROM clusters WHERE id=$1`, []any{clusterID}, 1},
		{"create operations", `SELECT count(*) FROM operations WHERE cluster_id=$1 AND type='CreateCluster'`, []any{clusterID}, 1},
		{"outbox entries", `SELECT count(*) FROM outbox WHERE payload->>'operationId'=$1`, []any{opID}, 1},
		{"reservations", `SELECT count(*) FROM reservations WHERE cluster_id=$1`, []any{clusterID}, 1},
		{"reservation items (2 pools x 3 zones)", `SELECT count(*) FROM reservation_items i JOIN reservations r ON r.id=i.reservation_id WHERE r.cluster_id=$1`, []any{clusterID}, 6},
		{"desired-state generations", `SELECT count(*) FROM desired_states WHERE cluster_id=$1 AND NOT tombstone`, []any{clusterID}, 1},
		{"cell revision bumps == desired rows in cell", `SELECT (SELECT revision FROM cells WHERE id=$1) - (SELECT count(*) FROM desired_states WHERE cell_id=$1)`, []any{e.cellID}, 0},
		{"committed reservation", `SELECT count(*) FROM reservations WHERE cluster_id=$1 AND status='COMMITTED'`, []any{clusterID}, 1},
	}
	for _, c := range checks {
		if got := countRows(t, c.q, c.args...); got != c.want {
			t.Errorf("%s: got %d, want %d", c.what, got, c.want)
		}
	}
	assertClusterObjects(t, clusterID)
}

func assertDeleted(t *testing.T, e *env, clusterID string) {
	t.Helper()
	if r := e.call("GET", "/v1/clusters/"+clusterID, "", nil); r.Code != http.StatusNotFound {
		t.Errorf("GET deleted cluster: %d, want 404", r.Code)
	}
	if _, err := kube.CoreV1().Namespaces().Get(context.Background(), clusterID, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("namespace %s still exists (err=%v)", clusterID, err)
	}
	if n := countRows(t, `SELECT count(*) FROM reservations WHERE cluster_id=$1 AND status<>'RELEASED'`, clusterID); n != 0 {
		t.Errorf("%d reservations still hold capacity", n)
	}
	if n := countRows(t, `SELECT count(*) FROM desired_states WHERE cluster_id=$1 AND tombstone`, clusterID); n != 1 {
		t.Errorf("tombstones: got %d, want 1", n)
	}
}

// M1 acceptance (1): create and delete a shared cluster via the API.
func TestCreateAndDeleteSharedCluster(t *testing.T) {
	e := newEnv(t)
	e.startWorker()

	opID, clusterID := e.createCluster("orders", "create-orders-1")

	// Idempotency: same key + same body replays the same operation.
	r := e.call("POST", "/v1/clusters", "create-orders-1", createBody("orders"))
	if r.Code != http.StatusAccepted || r.Body["id"] != opID || r.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("idempotent replay: %d %v", r.Code, r.Body)
	}
	// Same key, different body: rejected.
	if r := e.call("POST", "/v1/clusters", "create-orders-1", createBody("other")); r.Code != http.StatusUnprocessableEntity {
		t.Fatalf("key reuse with different body: %d %v", r.Code, r.Body)
	}
	// Per-resource lock: delete while creating is a conflict.
	if r := e.call("DELETE", "/v1/clusters/"+clusterID, "early-delete", nil); r.Code != http.StatusConflict {
		t.Fatalf("delete during create: %d %v, want 409", r.Code, r.Body)
	}

	e.waitOp(opID, "SUCCEEDED", 90*time.Second)
	c := e.call("GET", "/v1/clusters/"+clusterID, "", nil)
	if c.Body["state"] != "ACTIVE" || c.Body["cellId"] != e.cellID {
		t.Fatalf("cluster after create: %v", c.Body)
	}
	assertNoDuplicates(t, e, opID, clusterID)

	bs := e.call("GET", "/v1/clusters/"+clusterID+"/bootstrap-brokers", "", nil)
	eps, _ := bs.Body["endpoints"].(map[string]any)
	wantBS := fmt.Sprintf("bootstrap.%s.%s.streams.example.net:443", clusterID, e.cellID)
	if bs.Code != http.StatusOK || eps["scram-sha-512"] != wantBS {
		t.Fatalf("bootstrap-brokers: %d %v, want %s", bs.Code, bs.Body, wantBS)
	}
	list := e.call("GET", "/v1/clusters", "", nil)
	if cs, _ := list.Body["clusters"].([]any); len(cs) != 1 {
		t.Fatalf("list: %v", list.Body)
	}

	d := e.call("DELETE", "/v1/clusters/"+clusterID, "delete-orders-1", nil)
	if d.Code != http.StatusAccepted {
		t.Fatalf("delete: %d %v", d.Code, d.Body)
	}
	e.waitOp(d.Body["id"].(string), "SUCCEEDED", 90*time.Second)
	assertDeleted(t, e, clusterID)
}

// M1 acceptance (2): kill the worker mid-create; it resumes without duplicates.
// Each crash point exits the worker right AFTER the activity's side effects are
// committed and BEFORE Temporal learns it completed, so Temporal must re-run it.
func TestWorkerCrashDuringCreateResumesWithoutDuplicates(t *testing.T) {
	points := []string{
		"ReserveCapacity",
		"MarkStep:publish-desired-state",
		"RenderAndPublish",
		"MarkStep:wait-ready",
		"WaitForCondition:Ready",
		"FinishCreate",
	}
	e := newEnv(t)
	for i, point := range points {
		t.Run(point, func(t *testing.T) {
			e.t = t
			e.killWorkers() // only the crashing worker may pick up this workflow
			dir := t.TempDir()
			w1 := e.startWorker("STREAMCELL_FAULT_CRASH_AFTER="+point, "STREAMCELL_FAULT_DIR="+dir)
			opID, clusterID := e.createCluster(fmt.Sprintf("crash-%d", i), "crash-key-"+point)
			select {
			case <-w1.done:
			case <-time.After(60 * time.Second):
				t.Fatalf("fault point %s did not fire within 60s; operation: %v", point, e.call("GET", "/v1/operations/"+opID, "", nil).Body)
			}
			if code := w1.waitExit(t, time.Second); code != 137 {
				t.Fatalf("worker exit code %d, want 137 (fault point %s did not fire); log:\n%s", code, point, w1.log.String())
			}
			// Restart with the same fault configuration: the marker file makes the
			// point fire only once, exactly like a real crash followed by a restart.
			e.startWorker("STREAMCELL_FAULT_CRASH_AFTER="+point, "STREAMCELL_FAULT_DIR="+dir)
			e.waitOp(opID, "SUCCEEDED", 90*time.Second)
			assertNoDuplicates(t, e, opID, clusterID)
		})
	}
}

// Kill the worker with SIGKILL at an arbitrary moment (while waiting for Ready).
func TestWorkerSIGKILLWhileWaitingForReady(t *testing.T) {
	e := newEnv(t)
	sim.setReadyDelay(3 * time.Second)
	defer sim.setReadyDelay(0)
	w1 := e.startWorker()
	opID, clusterID := e.createCluster("sigkill", "sigkill-1")
	e.waitStep(opID, "wait-ready", 30*time.Second)
	w1.kill()
	if code := w1.exitCode(); code != -1 && code != 137 {
		t.Logf("worker exit code after SIGKILL: %d", code)
	}
	time.Sleep(time.Second)
	e.startWorker()
	e.waitOp(opID, "SUCCEEDED", 90*time.Second)
	assertNoDuplicates(t, e, opID, clusterID)
}

// Crash during delete as well.
func TestWorkerCrashDuringDelete(t *testing.T) {
	e := newEnv(t)
	for i, point := range []string{"PublishTombstone", "WaitForCondition:Deleted", "FinishDelete"} {
		t.Run(point, func(t *testing.T) {
			e.t = t
			e.startWorker()
			opID, clusterID := e.createCluster(fmt.Sprintf("del-%d", i), "del-create-"+point)
			e.waitOp(opID, "SUCCEEDED", 90*time.Second)
			// Replace the healthy worker with one that crashes at the fault point.
			dir := t.TempDir()
			e.killWorkers()
			w1 := e.startWorker("STREAMCELL_FAULT_CRASH_AFTER="+point, "STREAMCELL_FAULT_DIR="+dir)
			d := e.call("DELETE", "/v1/clusters/"+clusterID, "del-"+point, nil)
			if d.Code != http.StatusAccepted {
				t.Fatalf("delete: %d %v", d.Code, d.Body)
			}
			if code := w1.waitExit(t, 60*time.Second); code != 137 {
				t.Fatalf("worker exit code %d, want 137; log:\n%s", code, w1.log.String())
			}
			e.startWorker("STREAMCELL_FAULT_CRASH_AFTER="+point, "STREAMCELL_FAULT_DIR="+dir)
			e.waitOp(d.Body["id"].(string), "SUCCEEDED", 90*time.Second)
			assertDeleted(t, e, clusterID)
			if n := countRows(t, `SELECT count(*) FROM operations WHERE cluster_id=$1 AND type='DeleteCluster'`, clusterID); n != 1 {
				t.Errorf("delete operations: %d, want 1", n)
			}
			e.killWorkers()
		})
	}
}
