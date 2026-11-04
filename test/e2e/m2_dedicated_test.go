//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/avilashs/kafka_as_service/internal/placement"
)

func dedicatedBody(name string) map[string]any {
	return map[string]any{"name": name, "tier": "dedicated", "spec": map[string]any{
		"kafkaVersion": "4.3.1", "brokerType": "dev.ded", "brokerCount": 3,
		"storage": map[string]any{"sizeGiB": 1},
	}}
}

// addSpares creates the spare nodes (one per zone) the kind provider can turn into a dedicated pool.
func (e *env) addSpares(perZone int) []string {
	e.t.Helper()
	var names []string
	for _, z := range []string{"zone-a", "zone-b", "zone-c"} {
		for i := 0; i < perZone; i++ {
			name := fmt.Sprintf("spare-%s-%s-%d", e.cellID, z, i)
			_, err := kube.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
				"streamcell.dev/pool": "spare", "topology.kubernetes.io/zone": z, "test-cell": e.cellID}},
				// Spares are closed to general workloads, as in the kind cell (deploy/kind/cluster.yaml).
				Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "streamcell.dev/spare", Value: "true", Effect: corev1.TaintEffectNoSchedule}}}}, metav1.CreateOptions{})
			if err != nil {
				e.t.Fatal(err)
			}
			names = append(names, name)
		}
	}
	return names
}

func (e *env) nodesOf(selector string) []corev1.Node {
	e.t.Helper()
	l, err := kube.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		e.t.Fatal(err)
	}
	return l.Items
}

func (e *env) createDedicated(name, key string) (opID, clusterID string) {
	e.t.Helper()
	r := e.call("POST", "/v1/clusters", key, dedicatedBody(name))
	if r.Code != http.StatusAccepted {
		e.t.Fatalf("create dedicated: %d %v", r.Code, r.Body)
	}
	return r.Body["id"].(string), r.Body["clusterId"].(string)
}

// schedulePods plays the scheduler for one cluster's rendered pods: each broker goes to the claimed node of
// its zone. Everything it creates passes through the real admission policy.
func schedulePods(t *testing.T, e *env, clusterID string, nodes []corev1.Node) {
	t.Helper()
	b, err := st.LatestLiveBundle(context.Background(), clusterID)
	if err != nil || b == nil {
		t.Fatalf("no live bundle for %s: %v", clusterID, err)
	}
	tenantNamespace(t, clusterID) // already created by the agent; idempotent
	tmpl := podTemplates(t, b, clusterID)["brokers"]
	zones := []string{"zone-a", "zone-b", "zone-c"}
	for i, z := range zones {
		var node string
		for _, n := range nodes {
			if n.Labels["topology.kubernetes.io/zone"] == z {
				node = n.Name
			}
		}
		p := newPod(t, tmpl, fmt.Sprintf("%s-brokers-%d", clusterID, i))
		if err := bind(p, node); err != nil {
			t.Fatalf("bind %s to %s: %v", p.Name, node, err)
		}
	}
}

// M2: the dedicated tier end to end — API → workflow → capacity → agent → node pool → rendered
// placement → admission → pods on exactly the claimed nodes → audit → delete → nodes returned.
func TestDedicatedClusterLifecycle(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	spares := e.addSpares(1)

	opID, cid := e.createDedicated("ded1", "ded-create-1")
	e.waitOp(opID, "SUCCEEDED", 90*time.Second)

	// --- the node pool: exactly one node per zone, labelled and tainted for this cluster alone ---
	claimed := e.nodesOf("streamcell.dev/nodepool=ded-" + cid)
	if len(claimed) != 3 {
		t.Fatalf("claimed %d nodes, want 3 (one per broker)", len(claimed))
	}
	zones := map[string]bool{}
	for _, n := range claimed {
		zones[n.Labels["topology.kubernetes.io/zone"]] = true
		if n.Labels["streamcell.dev/pool"] != "dedicated" || n.Labels["streamcell.dev/dedicated-owner"] != cid {
			t.Errorf("node %s labels = %v", n.Name, n.Labels)
		}
		var tainted, stillSpare bool
		for _, ta := range n.Spec.Taints {
			if ta.Key == "streamcell.dev/dedicated" && ta.Value == cid && ta.Effect == corev1.TaintEffectNoSchedule {
				tainted = true
			}
			if ta.Key == "streamcell.dev/spare" {
				stillSpare = true
			}
		}
		if !tainted || stillSpare {
			t.Errorf("node %s: dedicated taint present=%v, spare taint still there=%v: %v", n.Name, tainted, stillSpare, n.Spec.Taints)
		}
	}
	if len(zones) != 3 {
		t.Errorf("claimed nodes span zones %v, want 3 zones", zones)
	}
	if got := len(e.nodesOf("streamcell.dev/pool=spare,test-cell=" + e.cellID)); got != 0 {
		t.Errorf("%d spare nodes left, want 0 (all three claimed)", got)
	}

	// --- accounting: whole nodes reserved, one per zone ---
	if n := countRows(t, `SELECT COALESCE(sum(i.nodes),0)::int FROM reservation_items i JOIN reservations r ON r.id=i.reservation_id
		JOIN node_pools p ON p.id=i.pool_id WHERE r.cluster_id=$1 AND p.kind='dedicated' AND r.status='COMMITTED'`, cid); n != 3 {
		t.Errorf("reserved dedicated nodes = %d, want 3", n)
	}

	// --- the scheduler places the brokers: through admission, onto the claimed nodes, one each ---
	schedulePods(t, e, cid, claimed)
	sel := "test-cell=" + e.cellID + ",streamcell.dev/pool=dedicated"
	snap, err := placement.Collect(context.Background(), kube, placement.CollectOptions{NodeSelector: sel})
	if err != nil {
		t.Fatal(err)
	}
	if vs := placement.Check(snap, placement.Options{}); len(vs) != 0 {
		t.Fatalf("a correctly placed dedicated cluster has violations: %v", vs)
	}
	if len(snap.Pods) != 3 {
		t.Fatalf("audit saw %d pods, want 3", len(snap.Pods))
	}
	_ = spares

	// --- the cell has no capacity for a second dedicated cluster: refused cleanly, nothing claimed ---
	op2, cid2 := e.createDedicated("ded2", "ded-create-2")
	r := e.call("GET", "/v1/operations/"+op2, "", nil)
	deadline := time.Now().Add(60 * time.Second)
	for r.Body["state"] != "FAILED" && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		r = e.call("GET", "/v1/operations/"+op2, "", nil)
	}
	if r.Body["state"] != "FAILED" {
		t.Fatalf("second dedicated cluster should fail for lack of capacity: %v", r.Body)
	}
	if msg, _ := json.Marshal(r.Body["error"]); !strings.Contains(string(msg), "no cell has capacity") {
		t.Errorf("error = %s", msg)
	}
	if c := e.call("GET", "/v1/clusters/"+cid2, "", nil); c.Body["state"] != "FAILED" {
		t.Errorf("second cluster state = %v", c.Body["state"])
	}
	if got := len(e.nodesOf("streamcell.dev/nodepool=ded-" + cid2)); got != 0 {
		t.Errorf("failed cluster claimed %d nodes", got)
	}
	if got := len(e.nodesOf("streamcell.dev/nodepool=ded-" + cid)); got != 3 {
		t.Errorf("first cluster lost nodes: %d", got)
	}

	// --- delete: pods go, nodes return to the spare pool without keeping owner labels or taints ---
	d := e.call("DELETE", "/v1/clusters/"+cid, "ded-delete-1", nil)
	if d.Code != http.StatusAccepted {
		t.Fatalf("delete: %d %v", d.Code, d.Body)
	}
	e.waitOp(d.Body["id"].(string), "SUCCEEDED", 90*time.Second)
	if got := len(e.nodesOf("streamcell.dev/nodepool=ded-" + cid)); got != 0 {
		t.Errorf("%d nodes still claimed after delete", got)
	}
	back := e.nodesOf("streamcell.dev/pool=spare,test-cell=" + e.cellID)
	if len(back) != 3 {
		t.Fatalf("%d spare nodes after delete, want 3", len(back))
	}
	for _, n := range back {
		_, owned := n.Labels["streamcell.dev/dedicated-owner"]
		_, pooled := n.Labels["streamcell.dev/nodepool"]
		var dedicated, spare bool
		for _, ta := range n.Spec.Taints { // system taints (e.g. not-ready) are not ours to remove
			dedicated = dedicated || ta.Key == "streamcell.dev/dedicated"
			spare = spare || ta.Key == "streamcell.dev/spare"
		}
		if owned || pooled || dedicated || !spare {
			t.Errorf("released node %s kept its claim: labels=%v taints=%v", n.Name, n.Labels, n.Spec.Taints)
		}
	}
	if n := countRows(t, `SELECT count(*) FROM reservations WHERE cluster_id=$1 AND status<>'RELEASED'`, cid); n != 0 {
		t.Errorf("%d reservations still hold capacity", n)
	}

	// --- and the freed capacity is reusable ---
	op3, cid3 := e.createDedicated("ded3", "ded-create-3")
	e.waitOp(op3, "SUCCEEDED", 90*time.Second)
	if got := len(e.nodesOf("streamcell.dev/nodepool=ded-" + cid3)); got != 3 {
		t.Errorf("third cluster claimed %d nodes", got)
	}
}

// Shared and dedicated tenants in one cell never touch each other's nodes.
func TestSharedClusterDoesNotUseDedicatedCapacity(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	e.addSpares(1)
	opID, cid := e.createCluster("plain", "shared-with-spares")
	e.waitOp(opID, "SUCCEEDED", 90*time.Second)
	if got := len(e.nodesOf("streamcell.dev/nodepool,test-cell=" + e.cellID)); got != 0 {
		t.Fatalf("a shared cluster claimed %d dedicated nodes", got)
	}
	if n := countRows(t, `SELECT COALESCE(sum(i.nodes),0)::int FROM reservation_items i JOIN reservations r ON r.id=i.reservation_id WHERE r.cluster_id=$1`, cid); n != 0 {
		t.Fatalf("shared cluster reserved %d dedicated nodes", n)
	}
}

// ---------- the auditor ----------

func writeKubeconfig(t *testing.T) string {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["envtest"] = &clientcmdapi.Cluster{Server: restCfg.Host, CertificateAuthorityData: restCfg.CAData}
	cfg.AuthInfos["admin"] = &clientcmdapi.AuthInfo{ClientCertificateData: restCfg.CertData, ClientKeyData: restCfg.KeyData, Token: restCfg.BearerToken}
	cfg.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "admin"}
	cfg.CurrentContext = "envtest"
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*cfg, path); err != nil {
		t.Fatal(err)
	}
	return path
}

type auditResult struct {
	Violations []placement.Violation `json:"violations"`
	Nodes      int                   `json:"nodes"`
	Pods       int                   `json:"pods"`
}

func runAudit(t *testing.T, kubeconfig, nodeSelector string, extra ...string) (auditResult, int) {
	t.Helper()
	args := append([]string{"--kubeconfig", kubeconfig, "--node-selector", nodeSelector, "--output", "json", "--surge", "0"}, extra...)
	cmd := exec.Command(auditBin, args...)
	var stdout strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	var res auditResult
	if e := json.Unmarshal([]byte(stdout.String()), &res); e != nil {
		t.Fatalf("audit output %q: %v", stdout.String(), e)
	}
	return res, code
}

// M2 rule 6: the audit reads actual pod and node state, and fails (exit 1) when an invariant is broken —
// including violations that bypassed admission entirely (pods outside tenant namespaces, or created
// before the policy existed).
func TestAuditCLIDetectsViolationsAndPassesCleanState(t *testing.T) {
	kc := writeKubeconfig(t)
	suffix := randHex(3)
	cell := "audit-" + suffix
	label := map[string]string{"test-cell": cell}
	mk := func(name, zone, pool, owner string) {
		l := map[string]string{"topology.kubernetes.io/zone": zone, "streamcell.dev/pool": pool, "test-cell": cell}
		if owner != "" {
			l["streamcell.dev/dedicated-owner"] = owner
		}
		if _, err := kube.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name + "-" + suffix, Labels: l}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	_ = label
	mk("s-a", "zone-a", "shared", "")
	mk("s-b", "zone-b", "shared", "")
	mk("s-c", "zone-c", "shared", "")
	mk("d-a", "zone-a", "dedicated", "kc-aud"+suffix)
	mk("d-b", "zone-b", "dedicated", "kc-aud"+suffix)
	mk("d-c", "zone-c", "dedicated", "kc-aud"+suffix)
	sel := "test-cell=" + cell

	// Pods are created in an UNLABELLED namespace: they are outside the admission policy's scope,
	// which is exactly the "admission was bypassed" case the audit exists for.
	ns := "audit-ns-" + suffix
	if _, err := kube.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := func(name, node string, labels map[string]string) {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
			Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "c", Image: "x"}}}}
		if _, err := kube.CoreV1().Pods(ns).Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	broker := func(cluster, pool, excl string) map[string]string {
		return map[string]string{"streamcell.dev/cluster": cluster, "streamcell.dev/role": "kafka-broker", "streamcell.dev/required-pool": pool, "streamcell.dev/exclusive": excl}
	}
	own := "kc-aud" + suffix
	for i, z := range []string{"a", "b", "c"} {
		pod("ok-"+z, "d-"+z+"-"+suffix, broker(own, "dedicated", "whole-node"))
		pod("sh-"+z, "s-"+z+"-"+suffix, broker("kc-shared"+suffix, "shared", "per-resource"))
		_ = i
	}

	res, code := runAudit(t, kc, sel)
	if code != 0 || len(res.Violations) != 0 {
		t.Fatalf("clean cell: exit %d, violations %v", code, res.Violations)
	}
	if res.Nodes != 6 || res.Pods != 6 {
		t.Fatalf("audited %d nodes / %d pods, want 6 / 6", res.Nodes, res.Pods)
	}

	// Now break every invariant the audit knows about, behind admission's back.
	pod("dup", "s-a-"+suffix, broker("kc-shared"+suffix, "shared", "per-resource"))                                                                                                     // rule 1
	pod("stray", "d-a-"+suffix, broker("kc-other"+suffix, "shared", "per-resource"))                                                                                                    // rules 3 + 4
	pod("anon", "d-b-"+suffix, nil)                                                                                                                                                     // rule 3: unlabelled pod on a dedicated node
	pod("second", "d-c-"+suffix, broker(own, "dedicated", "whole-node"))                                                                                                                // rule 3: two brokers on one node
	pod("offpool", "s-b-"+suffix, map[string]string{"streamcell.dev/cluster": "kc-ctl" + suffix, "streamcell.dev/role": "kafka-controller", "streamcell.dev/required-pool": "control"}) // rule 4
	res, code = runAudit(t, kc, sel)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1 with violations present", code)
	}
	got := map[string]bool{}
	for _, v := range res.Violations {
		got[v.Rule] = true
	}
	var rules []string
	for r := range got {
		rules = append(rules, r)
	}
	sort.Strings(rules)
	for _, want := range []string{placement.RuleSameGroupOnNode, placement.RuleDedicatedForeignPod, placement.RuleDedicatedMultiExclusive, placement.RulePodOffPool} {
		if !got[want] {
			t.Errorf("audit did not report %s; reported %v", want, rules)
		}
	}

	// Fixing the cell makes the audit pass again (it reads live state, not history).
	for _, n := range []string{"dup", "stray", "anon", "second", "offpool"} {
		zero := int64(0)
		if err := kube.CoreV1().Pods(ns).Delete(context.Background(), n, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			t.Fatal(err)
		}
	}
	if res, code = runAudit(t, kc, sel); code != 0 || len(res.Violations) != 0 {
		t.Fatalf("after cleanup: exit %d, violations %v", code, res.Violations)
	}

	// Zone skew and a leaked dedicated pool.
	zero := int64(0)
	_ = kube.CoreV1().Pods(ns).Delete(context.Background(), "sh-c", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	pod("sh-a2", "s-a-"+suffix, map[string]string{"streamcell.dev/cluster": "kc-shared" + suffix, "streamcell.dev/role": "kafka-broker", "streamcell.dev/required-pool": "shared", "streamcell.dev/exclusive": "per-resource", "x": "other-name"})
	_ = kube.CoreV1().Pods(ns).Delete(context.Background(), "ok-a", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	_ = kube.CoreV1().Pods(ns).Delete(context.Background(), "ok-b", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	_ = kube.CoreV1().Pods(ns).Delete(context.Background(), "ok-c", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	res, code = runAudit(t, kc, sel)
	got = map[string]bool{}
	for _, v := range res.Violations {
		got[v.Rule] = true
	}
	if code != 1 || !got[placement.RuleDedicatedNodeCount] {
		t.Errorf("a dedicated pool with no pods must be reported as leaked: exit %d, %v", code, res.Violations)
	}
}
