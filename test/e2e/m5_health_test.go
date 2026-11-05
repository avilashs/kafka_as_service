//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/health"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

func (e *env) postCanary(rs []store.CanaryResult) int {
	e.t.Helper()
	b, _ := json.Marshal(map[string]any{"results": rs})
	req, _ := http.NewRequest("POST", e.cellURL+"/cell/v1/canary", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+e.cellTok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct{ Accepted int }
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.Accepted
}

func clusterState(t *testing.T, id string) (state, reason string) {
	t.Helper()
	var r *string
	if err := st.Pool.QueryRow(context.Background(), `SELECT state, state_reason FROM clusters WHERE id=$1`, id).Scan(&state, &r); err != nil {
		t.Fatal(err)
	}
	if r != nil {
		reason = *r
	}
	return
}

// M5 acceptance: "the canary detects an injected outage". Here the platform side of it: the health
// evaluator turns canary reports and broker pod readiness into DEGRADED, only after the configured
// durations, and back to ACTIVE when the signals stop. (The canary itself is tested against a real
// broker in internal/canary and on a real cell in the kind scenario.)
func TestM5HealthDegradesAndRecovers(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	id := e.activeCluster("health")
	ctx := context.Background()
	base := renderPods(t, "shared", "dev.tiny", id)["brokers"]

	var offset atomic.Int64 // seconds the evaluator's clock is ahead of real time
	now := func() time.Time { return time.Now().Add(time.Duration(offset.Load()) * time.Second) }
	ev := &health.Evaluator{Store: st, Engines: engine.NewRegistry(kafka.New()), Log: logger, Cfg: health.Config{
		Window: 5 * time.Minute, MinAttempts: 20, StaleAfter: 6 * time.Hour,
		For: map[string]time.Duration{"brokerDown": 2 * time.Minute, "underMinIsr": time.Minute}, Now: now}}
	eval := func() {
		t.Helper()
		if err := ev.EvaluateOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	wantState := func(want string) {
		t.Helper()
		if got, reason := clusterState(t, id); got != want {
			t.Fatalf("state = %s (%s), want %s", got, reason, want)
		}
	}
	waitObserved := func() { // the agent's next report is in the database
		t.Helper()
		var before time.Time
		_ = st.Pool.QueryRow(ctx, `SELECT reported_at FROM observed_status WHERE cluster_id=$1`, id).Scan(&before)
		waitFor(t, "a fresh observation", 10*time.Second, func() bool {
			var at time.Time
			return st.Pool.QueryRow(ctx, `SELECT reported_at FROM observed_status WHERE cluster_id=$1`, id).Scan(&at) == nil && at.After(before)
		})
	}

	eval()
	wantState("ACTIVE") // no pods reported, no canary data: no verdict

	// ---- a broker that is down: not at once, but after two minutes ----
	applyMeterState(t, id, base, meterState{sizes: map[string]int{"dev.tiny": 3}, gib: 1, pvcs: 3}) // pods exist, none is Ready
	waitObserved()
	eval()
	wantState("ACTIVE")
	offset.Store(60)
	eval()
	wantState("ACTIVE") // one minute is not enough
	offset.Store(130)
	eval()
	wantState("DEGRADED")
	if _, reason := clusterState(t, id); !strings.Contains(reason, "broker pods are ready") {
		t.Errorf("reason = %q", reason)
	}
	if n := countRows(t, `SELECT count(*) FROM audit_log WHERE action='Health' AND resource=$1`, "cluster/"+id); n != 1 {
		t.Errorf("%d audit entries for the transition, want 1", n)
	}
	eval()
	if n := countRows(t, `SELECT count(*) FROM audit_log WHERE action='Health' AND resource=$1`, "cluster/"+id); n != 1 {
		t.Errorf("evaluating again wrote another audit entry (%d)", n)
	}

	// the brokers come back: Ready
	pods, _ := kube.CoreV1().Pods(id).List(ctx, metav1.ListOptions{LabelSelector: "strimzi.io/broker-role=true"})
	for i := range pods.Items {
		p := &pods.Items[i]
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		if _, err := kube.CoreV1().Pods(id).UpdateStatus(ctx, p, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	waitObserved()
	eval()
	wantState("ACTIVE")

	// ---- the canary: an outage on the public path shows as failed round trips ----
	// too few probes to judge
	if e.postCanary([]store.CanaryResult{{ClusterID: id, At: now().Add(-30 * time.Second), Attempts: 5, Successes: 0}}) != 1 {
		t.Fatal("canary report not accepted")
	}
	eval()
	wantState("ACTIVE")
	// enough probes, all failing
	e.postCanary([]store.CanaryResult{{ClusterID: id, At: now().Add(-20 * time.Second), Attempts: 30, Successes: 0}})
	eval()
	wantState("DEGRADED")
	if _, reason := clusterState(t, id); !strings.Contains(reason, "canary availability") {
		t.Errorf("reason = %q", reason)
	}
	// recovery: failures age out of the window and healthy probes arrive
	offset.Store(130 + 400)
	e.postCanary([]store.CanaryResult{{ClusterID: id, At: now().Add(-10 * time.Second), Attempts: 60, Successes: 60}})
	eval()
	wantState("ACTIVE")
	// a flaky path that stays above the threshold is not an outage
	e.postCanary([]store.CanaryResult{{ClusterID: id, At: now().Add(-5 * time.Second), Attempts: 100, Successes: 100}, {ClusterID: id, At: now(), Attempts: 100, Successes: 99}})
	eval()
	wantState("ACTIVE")

	// a path that is cut makes every round take its timeout, so few attempts reach the window: a window with
	// nothing but failures is judged from two rounds on (found on the kind cell: 15 attempts in 90 s, no verdict)
	offset.Store(130 + 400 + 400)
	e.postCanary([]store.CanaryResult{{ClusterID: id, At: now().Add(-5 * time.Second), Attempts: 6, Successes: 0}})
	eval()
	wantState("DEGRADED")
	offset.Store(130 + 400 + 400 + 400)
	e.postCanary([]store.CanaryResult{{ClusterID: id, At: now(), Attempts: 30, Successes: 30}})
	eval()
	wantState("ACTIVE")

	// ---- what the evaluator must leave alone ----
	// reports for a cluster of another cell are dropped
	if e.postCanary([]store.CanaryResult{{ClusterID: "kc-not-ours", At: now(), Attempts: 99, Successes: 0}}) != 0 {
		t.Error("a report for a cluster that is not in the cell was accepted")
	}
	// impossible reports are dropped
	if e.postCanary([]store.CanaryResult{{ClusterID: id, At: now(), Attempts: 3, Successes: 9}}) != 0 {
		t.Error("a report with more successes than attempts was accepted")
	}
	// a cluster in the middle of an operation is not judged, even with a failing canary
	sim.setReadyDelay(3 * time.Second)
	defer sim.setReadyDelay(0)
	r := e.patch(id, map[string]any{"config": map[string]any{"log.retention.hours": "48"}})
	if r.Code != http.StatusAccepted {
		t.Fatalf("%d %v", r.Code, r.Body)
	}
	e.postCanary([]store.CanaryResult{{ClusterID: id, At: now(), Attempts: 100, Successes: 0}})
	eval()
	if got, _ := clusterState(t, id); got != "UPDATING" {
		t.Errorf("a cluster being changed was judged: %s", got)
	}
	e.waitOp(r.Body["id"].(string), "SUCCEEDED", 60*time.Second)
	// and a stale observation is not judged either
	ev.Cfg.StaleAfter = time.Nanosecond
	eval()
	wantState("ACTIVE")
}
