//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/avilashs/kafka_as_service/internal/agent"
	"github.com/avilashs/kafka_as_service/internal/metering"
)

// meterState is the scripted truth of what runs at a tick: broker pods per size and provisioned GiB per
// broker volume. The test makes the cell look like this and computes the expected totals from the same script.
type meterState struct {
	sizes map[string]int // size -> pods
	gib   int64          // GiB per broker volume
	pvcs  int
}

func (s meterState) pods() (n int) {
	for _, c := range s.sizes {
		n += c
	}
	return
}

// applyMeterState makes the namespace's broker pods and volume claims match s.
func applyMeterState(t *testing.T, id string, base *corev1.Pod, s meterState) {
	t.Helper()
	ctx := context.Background()
	// pods: delete everything and recreate, so a change of size is a replacement, as in a roll
	want := map[string]string{} // name -> size
	n := 0
	var sizes []string
	for sz := range s.sizes {
		sizes = append(sizes, sz)
	}
	sort.Strings(sizes)
	for _, sz := range sizes {
		for i := 0; i < s.sizes[sz]; i++ {
			want[fmt.Sprintf("%s-brokers-%d", id, n)] = sz
			n++
		}
	}
	have, err := kube.CoreV1().Pods(id).List(ctx, metav1.ListOptions{LabelSelector: "strimzi.io/broker-role=true"})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range have.Items {
		if want[p.Name] != p.Labels["streamcell.dev/broker-type"] {
			zero := int64(0)
			_ = kube.CoreV1().Pods(id).Delete(ctx, p.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}
	for name, sz := range want {
		if p, err := kube.CoreV1().Pods(id).Get(ctx, name, metav1.GetOptions{}); err == nil && p.Labels["streamcell.dev/broker-type"] == sz && p.DeletionTimestamp == nil {
			continue
		}
		p := base.DeepCopy()
		p.Name, p.Namespace, p.ResourceVersion = name, id, ""
		p.Labels["strimzi.io/kind"], p.Labels["strimzi.io/broker-role"], p.Labels["streamcell.dev/broker-type"] = "Kafka", "true", sz
		waitFor(t, "old pod "+name+" gone", 10*time.Second, func() bool {
			_, err := kube.CoreV1().Pods(id).Get(ctx, name, metav1.GetOptions{})
			return err != nil
		})
		created, err := kube.CoreV1().Pods(id).Create(ctx, p, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
		created.Status.Phase = corev1.PodRunning
		if _, err := kube.CoreV1().Pods(id).UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// volume claims
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("data-%s-brokers-%d", id, i)
		if i >= s.pvcs {
			removePVC(t, id, name)
			continue
		}
		q := resource.MustParse(fmt.Sprintf("%dGi", s.gib))
		pvc, err := kube.CoreV1().PersistentVolumeClaims(id).Get(ctx, name, metav1.GetOptions{})
		if err == nil && !pvc.Spec.Resources.Requests.Storage().Equal(q) {
			// envtest has no volume controller to expand a claim: model the growth as a replacement
			removePVC(t, id, name)
			pvc, err = nil, fmt.Errorf("replaced")
		}
		if err != nil {
			pvc, err = kube.CoreV1().PersistentVolumeClaims(id).Create(ctx, &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: id},
				Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: q}}},
			}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}
		pvc.Status.Phase = corev1.ClaimBound
		pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: q}
		if _, err := kube.CoreV1().PersistentVolumeClaims(id).UpdateStatus(ctx, pvc, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

// removePVC deletes a claim; envtest has no controller to remove the protection finalizer, so it is cleared.
func removePVC(t *testing.T, ns, name string) {
	t.Helper()
	ctx := context.Background()
	if _, err := kube.CoreV1().PersistentVolumeClaims(ns).Get(ctx, name, metav1.GetOptions{}); err != nil {
		return
	}
	_ = kube.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, name, metav1.DeleteOptions{})
	_, _ = kube.CoreV1().PersistentVolumeClaims(ns).Patch(ctx, name, "application/merge-patch+json", []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
	waitFor(t, "claim "+name+" gone", 10*time.Second, func() bool {
		_, err := kube.CoreV1().PersistentVolumeClaims(ns).Get(ctx, name, metav1.GetOptions{})
		return err != nil
	})
}

type dbRecord struct {
	ID       string
	Meter    string
	Labels   map[string]string
	Start    time.Time
	End      time.Time
	Quantity int64
	Unit     string
	Cell     string
	Cluster  string
	Schema   int
}

func loadRecords(t *testing.T, clusterID string) []dbRecord {
	t.Helper()
	rows, err := st.Pool.Query(context.Background(), `SELECT record_id, meter, labels, interval_start, interval_end, quantity, unit, cell_id, cluster_id, schema_version
		FROM metering_records WHERE cluster_id=$1 ORDER BY interval_start, meter, record_id`, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []dbRecord
	for rows.Next() {
		var r dbRecord
		var lb []byte
		if err := rows.Scan(&r.ID, &r.Meter, &lb, &r.Start, &r.End, &r.Quantity, &r.Unit, &r.Cell, &r.Cluster, &r.Schema); err != nil {
			t.Fatal(err)
		}
		_ = json.Unmarshal(lb, &r.Labels)
		out = append(out, r)
	}
	return out
}

func (e *env) postMetering(recs []metering.Record) (int, map[string]any) {
	b, _ := json.Marshal(map[string]any{"records": recs})
	req, _ := http.NewRequest("POST", e.cellURL+"/cell/v1/metering", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+e.cellTok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// M5 acceptance: "metering reconciles under retries". The cell is made to look like a scripted timeline
// (broker pods of two sizes, a scale out, a scale in, a volume expansion). The agent samples it with a fake
// clock while the control plane goes down, acknowledgements get lost, and the agent restarts. At the end the
// stored records must equal, to the unit, an independent per-second integration of the script.
func TestM5MeteringReconcilesUnderFaults(t *testing.T) {
	e := newEnvOpts(t, envOptions{Metering: &agent.MeteringConfig{Interval: 10 * time.Minute, MaxGap: 20 * time.Minute}})
	e.startWorker()
	id := e.activeCluster("meter")
	base := renderPods(t, "shared", "dev.tiny", id)["brokers"]
	ctx := context.Background()

	T0 := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	const tickSec, ticks = 60, 46
	state := func(i int) meterState {
		switch {
		case i < 10:
			return meterState{sizes: map[string]int{"dev.tiny": 3}, gib: 100, pvcs: 3}
		case i < 20: // scale out to 6, three of them already on the new size (a rolling resize)
			return meterState{sizes: map[string]int{"dev.tiny": 3, "dev.small": 3}, gib: 100, pvcs: 6}
		case i < 30: // scale in to 3, all on the new size
			return meterState{sizes: map[string]int{"dev.small": 3}, gib: 100, pvcs: 3}
		case i < 40: // volumes grow
			return meterState{sizes: map[string]int{"dev.small": 3}, gib: 200, pvcs: 3}
		default: // everything gone
			return meterState{sizes: map[string]int{}, gib: 0, pvcs: 0}
		}
	}

	ag := func() *agent.Agent { return e.agent }
	tick := func(i int) {
		applyMeterState(t, id, base, state(i))
		if err := ag().MeterTick(ctx, T0.Add(time.Duration(i*tickSec)*time.Second)); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		_ = ag().MeterDeliver(ctx) // may fail during the injected outage; the spool keeps the records
	}

	for i := 0; i < ticks; i++ {
		switch i {
		case 12:
			e.fault.down.Store(true) // control plane unreachable for 6 ticks
		case 18:
			e.fault.down.Store(false)
			e.fault.lose.Store(2) // the next two deliveries are processed but the answer is lost
		case 25:
			// agent restart: the agent is down for the duration of one tick (24 -> 25 happened; 26 is skipped)
			e.restartAgent()
			waitFor(t, "agent to reload the bundle", 30*time.Second, func() bool { return ag().HasBundle(id) })
		case 26:
			continue // the agent was not there for this tick; the state did not change around it
		}
		tick(i)
	}
	// Everything gone, cluster deleted: the agent closes the open intervals.
	delOp := e.call("DELETE", "/v1/clusters/"+id, "del-"+randHex(3), nil).Body["id"].(string)
	e.waitOp(delOp, "SUCCEEDED", 90*time.Second)
	waitFor(t, "agent to forget the cluster", 20*time.Second, func() bool { return !ag().HasBundle(id) })
	_ = ag().MeterTick(ctx, T0.Add(time.Duration((ticks+1)*tickSec)*time.Second))
	waitFor(t, "spool to empty", 20*time.Second, func() bool {
		_ = ag().MeterDeliver(ctx)
		return len(loadRecords(t, id)) > 0 && ag().MeterDeliver(ctx) == nil
	})

	// ---- the oracle: integrate the script second by second, independently of the meter ----
	type key struct {
		meter, size string
		iv          int64
	}
	truth := map[key]int64{}
	for s := int64(0); s < int64(ticks)*tickSec; s++ {
		cur := state(int(s / tickSec))
		iv := s / 600
		for sz, n := range cur.sizes {
			truth[key{"broker_hours", sz, iv}] += int64(n)
		}
		truth[key{"storage_gb_hours", "", iv}] += cur.gib * int64(cur.pvcs)
	}
	got := map[key]int64{}
	recs := loadRecords(t, id)
	ids := map[string]bool{}
	for _, r := range recs {
		if ids[r.ID] {
			t.Fatalf("record %s stored twice", r.ID)
		}
		ids[r.ID] = true
		got[key{r.Meter, r.Labels["size"], r.Start.Sub(T0).Milliseconds() / 600000}] += r.Quantity
		if r.Unit == "" || r.Cell != e.cellID {
			t.Errorf("record %+v lacks its unit or cell", r)
		}
	}
	for k, want := range truth {
		if want != 0 && got[k] != want {
			t.Errorf("%s{%s} interval %d: stored %d, truth %d", k.meter, k.size, k.iv, got[k], want)
		}
	}
	for k, g := range got {
		if g != 0 && truth[k] != g {
			t.Errorf("%s{%s} interval %d: stored %d but the script says %d", k.meter, k.size, k.iv, g, truth[k])
		}
	}
	// fixed points of the script, computed by hand: 3 tiny brokers for 10 minutes
	if q := got[key{"broker_hours", "dev.tiny", 0}]; q != 3*600 {
		t.Errorf("interval 0 tiny: %d, want %d", q, 3*600)
	}
	if q := got[key{"storage_gb_hours", "", 3}]; q != 3*200*600 {
		t.Errorf("interval 3 storage: %d, want %d", q, 3*200*600)
	}
	if e.fault.seen.Load() < 4 {
		t.Errorf("the control plane saw %d deliveries; the faults were not exercised", e.fault.seen.Load())
	}
	if n := countRows(t, `SELECT count(*) FROM metering_conflicts`); n != 0 {
		t.Errorf("%d conflicts after only honest retries", n)
	}

	// ---- what the control plane does with bad deliveries ----
	var some []metering.Record
	for _, r := range recs[:5] {
		some = append(some, metering.Record{ID: r.ID, Cell: r.Cell, Cluster: r.Cluster, Meter: r.Meter, Labels: r.Labels, Start: r.Start, End: r.End, Quantity: r.Quantity, Unit: r.Unit, Schema: r.Schema})
	}
	before := len(recs)
	if code, body := e.postMetering(some); code != 200 || len(body["acked"].([]any)) != 5 {
		t.Fatalf("duplicate delivery: %d %v", code, body)
	}
	if after := len(loadRecords(t, id)); after != before {
		t.Errorf("a duplicate delivery changed the row count: %d -> %d", before, after)
	}
	changed := some[0]
	changed.Quantity += 999 // same identity, different quantity
	if code, body := e.postMetering([]metering.Record{changed}); code != 200 || len(body["acked"].([]any)) != 1 {
		t.Fatalf("conflicting delivery: %d %v", code, body)
	}
	if q := loadRecords(t, id)[0].Quantity; q != recs[0].Quantity {
		t.Errorf("a conflicting delivery overwrote the stored quantity: %d -> %d", recs[0].Quantity, q)
	}
	if n := countRows(t, `SELECT count(*) FROM metering_conflicts WHERE record_id=$1`, changed.ID); n != 1 {
		t.Errorf("conflicts recorded: %d, want 1", n)
	}
	forged := some[1]
	forged.Meter = "something_else" // the ID no longer matches the content
	other := some[2]
	other.Cell = "another-cell"
	other.ID = metering.RecordID(other.Cell, other.Cluster, other.Meter, other.Labels, other.Start, other.End, other.Schema)
	code, body := e.postMetering([]metering.Record{forged, other})
	acked, _ := body["acked"].([]any)
	rejected, _ := body["rejected"].([]any)
	if code != 200 || len(acked) != 0 || len(rejected) != 2 {
		t.Errorf("forged or foreign records must be rejected, not acknowledged: %d %v", code, body)
	}
	if after := len(loadRecords(t, id)); after != before {
		t.Errorf("rejected records were stored: %d -> %d", before, after)
	}
}
