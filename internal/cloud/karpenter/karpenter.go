// Package karpenter holds the Karpenter implementation of the cell's node pool provider: one
// Karpenter NodePool per dedicated resource (ADR-0008). Karpenter's NodePool is the same on every cloud; only the
// node class it refers to differs (AWS: EC2NodeClass, Azure: AKSNodeClass of Node Auto Provisioning).
//
// STATUS: the manifests below are unit-tested but have NOT been run against a real Karpenter, EKS or AKS
// cluster; that is the part of M6 that needs a cloud account.
package karpenter

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/labels"
)

var nodePoolGVR = schema.GroupVersionResource{Group: "karpenter.sh", Version: "v1", Resource: "nodepools"}

// Well-known node class kinds.
const (
	AWSNodeClassGroup   = "karpenter.k8s.aws"
	AWSNodeClassKind    = "EC2NodeClass"
	AzureNodeClassGroup = "karpenter.azure.com"
	AzureNodeClassKind  = "AKSNodeClass"
)

// Karpenter provisions dedicated pools as Karpenter NodePools.
type Karpenter struct {
	Dyn dynamic.Interface
	// NodeClassName is the cell-wide node class (image, subnets, root volume, metadata options). It is
	// installed with the cell (deploy/azure; the AWS equivalent was removed, see ADR-0019).
	NodeClassName string
	// NodeClassGroup and NodeClassKind identify its type; they default to the AWS ones.
	NodeClassGroup, NodeClassKind string
	// TerminationGracePeriod bounds how long Karpenter drains a node before forcing it.
	TerminationGracePeriod time.Duration
}

// NodePool renders the Karpenter NodePool for spec.
//
//   - template.metadata.labels / template.spec.taints give the nodes the owner label and the
//     per-resource NoSchedule taint (design §10.2 rule 3a);
//   - limits.nodes = steady-state nodes + surge, so Karpenter can never create more than the
//     resource may hold (rule 3c);
//   - disruption: only empty nodes are consolidated, and at most one node is disrupted at a time;
//   - expireAfter Never: node replacement is driven by our rotation workflow, not by node age.
func (k *Karpenter) NodePool(spec engine.NodePoolSpec) *unstructured.Unstructured {
	zones := make([]string, 0, len(spec.NodesPerZone))
	for z := range spec.NodesPerZone {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	taints := []any{}
	for _, t := range spec.Taints {
		m := map[string]any{"key": t.Key, "effect": t.Effect}
		if t.Value != "" {
			m["value"] = t.Value
		}
		taints = append(taints, m)
	}
	nodeLabels := map[string]any{labels.NodePoolID: spec.ID}
	for key, v := range spec.Labels {
		nodeLabels[key] = v
	}
	grace := k.TerminationGracePeriod
	if grace == 0 {
		grace = 12 * time.Hour
	}
	toAny := func(ss ...string) []any {
		out := make([]any, len(ss))
		for i, s := range ss {
			out[i] = s
		}
		return out
	}
	ncGroup, ncKind := k.NodeClassGroup, k.NodeClassKind
	if ncGroup == "" {
		ncGroup = AWSNodeClassGroup
	}
	if ncKind == "" {
		ncKind = AWSNodeClassKind
	}
	obj := map[string]any{
		"apiVersion": "karpenter.sh/v1",
		"kind":       "NodePool",
		"metadata": map[string]any{
			"name":   spec.ID,
			"labels": map[string]any{labels.Inventory: spec.Owner, labels.NodePoolID: spec.ID},
		},
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{"labels": nodeLabels},
				"spec": map[string]any{
					"nodeClassRef": map[string]any{"group": ncGroup, "kind": ncKind, "name": k.NodeClassName},
					"taints":       taints,
					"requirements": []any{
						map[string]any{"key": "node.kubernetes.io/instance-type", "operator": "In", "values": toAny(spec.InstanceType)},
						map[string]any{"key": labels.Zone, "operator": "In", "values": toAny(zones...)},
						map[string]any{"key": "karpenter.sh/capacity-type", "operator": "In", "values": toAny("on-demand")},
					},
					"expireAfter":            "Never",
					"terminationGracePeriod": grace.String(),
				},
			},
			"limits": map[string]any{"nodes": fmt.Sprint(spec.TotalNodes() + spec.Surge)},
			"disruption": map[string]any{
				"consolidationPolicy": "WhenEmpty",
				"consolidateAfter":    "5m",
				"budgets":             []any{map[string]any{"nodes": "1"}},
			},
		},
	}
	// Round-trip so numbers become int64 like the API machinery produces.
	raw, _ := json.Marshal(obj)
	u := &unstructured.Unstructured{}
	_ = u.UnmarshalJSON(raw)
	return u
}

// Ensure applies the NodePool (server-side apply; idempotent).
func (k *Karpenter) Ensure(ctx context.Context, spec engine.NodePoolSpec) error {
	data, err := json.Marshal(k.NodePool(spec).Object)
	if err != nil {
		return err
	}
	force := true
	_, err = k.Dyn.Resource(nodePoolGVR).Patch(ctx, spec.ID, types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: "streamcell-agent", Force: &force})
	return err
}

// Release deletes the NodePool; Karpenter drains and terminates its nodes. done is true once
// the NodePool object (which holds a finalizer until its NodeClaims are gone) has disappeared.
func (k *Karpenter) Release(ctx context.Context, spec engine.NodePoolSpec) (bool, error) {
	err := k.Dyn.Resource(nodePoolGVR).Delete(ctx, spec.ID, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	_, err = k.Dyn.Resource(nodePoolGVR).Get(ctx, spec.ID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	return false, err
}
