package aws

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

func spec() engine.NodePoolSpec {
	return engine.NodePoolSpec{
		ID: "ded-kc-0123456789", Owner: "kc-0123456789", InstanceType: "m7i.xlarge",
		NodesPerZone: map[string]int{"us-east-1c": 1, "us-east-1a": 1, "us-east-1b": 2}, Surge: 1,
		Labels: map[string]string{"streamcell.dev/pool": "dedicated", "streamcell.dev/dedicated-owner": "kc-0123456789"},
		Taints: []engine.Taint{{Key: "streamcell.dev/dedicated", Value: "kc-0123456789", Effect: "NoSchedule"}},
	}
}

// These assertions encode design §10.2 rule 3 for AWS. They check the manifest only: it has not
// been applied to a real Karpenter yet (M6).
func TestKarpenterNodePoolEnforcesDedicatedRules(t *testing.T) {
	k := &Karpenter{NodeClassName: "streamcell-default"}
	np := k.NodePool(spec())
	if np.GetAPIVersion() != "karpenter.sh/v1" || np.GetKind() != "NodePool" || np.GetName() != "ded-kc-0123456789" {
		t.Fatalf("identity: %s %s %s", np.GetAPIVersion(), np.GetKind(), np.GetName())
	}
	tmplLabels, _, _ := unstructured.NestedStringMap(np.Object, "spec", "template", "metadata", "labels")
	if tmplLabels["streamcell.dev/dedicated-owner"] != "kc-0123456789" || tmplLabels["streamcell.dev/pool"] != "dedicated" || tmplLabels["streamcell.dev/nodepool"] != "ded-kc-0123456789" {
		t.Errorf("node labels = %v", tmplLabels)
	}
	taints, _, _ := unstructured.NestedSlice(np.Object, "spec", "template", "spec", "taints")
	if len(taints) != 1 {
		t.Fatalf("taints = %v", taints)
	}
	tt := taints[0].(map[string]any)
	if tt["key"] != "streamcell.dev/dedicated" || tt["value"] != "kc-0123456789" || tt["effect"] != "NoSchedule" {
		t.Errorf("taint = %v", tt)
	}
	limits, _, _ := unstructured.NestedStringMap(np.Object, "spec", "limits")
	if limits["nodes"] != "5" { // 4 brokers + 1 surge
		t.Errorf("limits.nodes = %q, want 5 (4 steady-state + 1 surge)", limits["nodes"])
	}
	reqs, _, _ := unstructured.NestedSlice(np.Object, "spec", "template", "spec", "requirements")
	got := map[string][]any{}
	for _, r := range reqs {
		m := r.(map[string]any)
		got[m["key"].(string)] = m["values"].([]any)
	}
	if !reflect.DeepEqual(got["node.kubernetes.io/instance-type"], []any{"m7i.xlarge"}) {
		t.Errorf("instance type requirement = %v", got["node.kubernetes.io/instance-type"])
	}
	if !reflect.DeepEqual(got["topology.kubernetes.io/zone"], []any{"us-east-1a", "us-east-1b", "us-east-1c"}) {
		t.Errorf("zones = %v", got["topology.kubernetes.io/zone"])
	}
	if !reflect.DeepEqual(got["karpenter.sh/capacity-type"], []any{"on-demand"}) {
		t.Errorf("capacity type = %v", got["karpenter.sh/capacity-type"])
	}
	ref, _, _ := unstructured.NestedStringMap(np.Object, "spec", "template", "spec", "nodeClassRef")
	if ref["name"] != "streamcell-default" || ref["kind"] != "EC2NodeClass" || ref["group"] != "karpenter.k8s.aws" {
		t.Errorf("nodeClassRef = %v", ref)
	}
	if v, _, _ := unstructured.NestedString(np.Object, "spec", "template", "spec", "expireAfter"); v != "Never" {
		t.Errorf("expireAfter = %q", v)
	}
	if v, _, _ := unstructured.NestedString(np.Object, "spec", "disruption", "consolidationPolicy"); v != "WhenEmpty" {
		t.Errorf("consolidationPolicy = %q: nodes holding brokers must never be consolidated away", v)
	}
	budgets, _, _ := unstructured.NestedSlice(np.Object, "spec", "disruption", "budgets")
	if len(budgets) != 1 || budgets[0].(map[string]any)["nodes"] != "1" {
		t.Errorf("disruption budgets = %v, want one node at a time", budgets)
	}
}
