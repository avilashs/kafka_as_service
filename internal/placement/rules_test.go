package placement

import (
	"reflect"
	"sort"
	"testing"
)

func nodes(ns ...Node) map[string]Node {
	m := map[string]Node{}
	for _, n := range ns {
		m[n.Name] = n
	}
	return m
}

func broker(cluster, name, node string) Pod {
	return Pod{Namespace: cluster, Name: name, Node: node, Cluster: cluster, Role: "broker", RequiredPool: "shared", Exclusive: "per-resource"}
}

func dedBroker(cluster, name, node string) Pod {
	return Pod{Namespace: cluster, Name: name, Node: node, Cluster: cluster, Role: "broker", RequiredPool: "dedicated", Exclusive: "whole-node"}
}

func ruleSet(vs []Violation) []string {
	seen := map[string]bool{}
	for _, v := range vs {
		seen[v.Rule] = true
	}
	var out []string
	for r := range seen {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

var (
	sharedA = Node{Name: "s-a", Zone: "a", Pool: "shared"}
	sharedB = Node{Name: "s-b", Zone: "b", Pool: "shared"}
	sharedC = Node{Name: "s-c", Zone: "c", Pool: "shared"}
	ctrlA   = Node{Name: "c-a", Zone: "a", Pool: "control"}
)

func dedNodes(owner string) []Node {
	return []Node{
		{Name: owner + "-a", Zone: "a", Pool: "dedicated", Owner: owner},
		{Name: owner + "-b", Zone: "b", Pool: "dedicated", Owner: owner},
		{Name: owner + "-c", Zone: "c", Pool: "dedicated", Owner: owner},
	}
}

func TestEachRuleFiresOnItsViolationAndNotOnCompliantState(t *testing.T) {
	type tc struct {
		name string
		snap Snapshot
		opt  Options
		want []string // sorted rule names; nil means no violations
	}
	d1 := dedNodes("kc-d1")
	cases := []tc{
		{
			name: "compliant shared cluster: one broker per node, one per zone",
			snap: Snapshot{Nodes: nodes(sharedA, sharedB, sharedC),
				Pods: []Pod{broker("kc-1", "b0", "s-a"), broker("kc-1", "b1", "s-b"), broker("kc-1", "b2", "s-c")}},
		},
		{
			name: "two tenants sharing nodes is fine in the shared tier",
			snap: Snapshot{Nodes: nodes(sharedA, sharedB, sharedC),
				Pods: []Pod{broker("kc-1", "b0", "s-a"), broker("kc-1", "b1", "s-b"), broker("kc-1", "b2", "s-c"),
					broker("kc-2", "b0", "s-a"), broker("kc-2", "b1", "s-b"), broker("kc-2", "b2", "s-c")}},
		},
		{
			name: "rule 1: two brokers of one cluster on a node",
			snap: Snapshot{Nodes: nodes(sharedA, sharedB, sharedC),
				Pods: []Pod{broker("kc-1", "b0", "s-a"), broker("kc-1", "b1", "s-a"), broker("kc-1", "b2", "s-c")}},
			want: []string{RuleSameGroupOnNode, RuleZoneSkew},
		},
		{
			name: "rule 2: zone skew of 2 (3 brokers in 2 zones)",
			snap: Snapshot{Nodes: nodes(sharedA, sharedB, sharedC, Node{Name: "s-a2", Zone: "a", Pool: "shared"}),
				Pods: []Pod{broker("kc-1", "b0", "s-a"), broker("kc-1", "b1", "s-a2"), broker("kc-1", "b2", "s-b")}},
			want: []string{RuleZoneSkew},
		},
		{
			name: "rule 2: 6 brokers as 2/2/2 is fine, 4/1/1 is not",
			snap: Snapshot{Nodes: nodes(sharedA, sharedB, sharedC, Node{Name: "s-a2", Zone: "a", Pool: "shared"}, Node{Name: "s-a3", Zone: "a", Pool: "shared"}, Node{Name: "s-a4", Zone: "a", Pool: "shared"}),
				Pods: []Pod{broker("kc-1", "b0", "s-a"), broker("kc-1", "b1", "s-a2"), broker("kc-1", "b2", "s-a3"), broker("kc-1", "b3", "s-a4"), broker("kc-1", "b4", "s-b"), broker("kc-1", "b5", "s-c")}},
			want: []string{RuleZoneSkew},
		},
		{
			name: "pending pods (no node) do not count towards skew",
			snap: Snapshot{Nodes: nodes(sharedA, sharedB, sharedC),
				Pods: []Pod{broker("kc-1", "b0", "s-a"), broker("kc-1", "b1", "s-b"), broker("kc-1", "b2", "")}},
		},
		{
			name: "compliant dedicated cluster",
			snap: Snapshot{Nodes: nodes(d1...),
				Pods: []Pod{dedBroker("kc-d1", "b0", "kc-d1-a"), dedBroker("kc-d1", "b1", "kc-d1-b"), dedBroker("kc-d1", "b2", "kc-d1-c")}},
		},
		{
			name: "rule 3: a shared broker (and an unlabelled pod) on a dedicated node",
			snap: Snapshot{Nodes: nodes(d1...),
				Pods: []Pod{dedBroker("kc-d1", "b0", "kc-d1-a"), dedBroker("kc-d1", "b1", "kc-d1-b"), dedBroker("kc-d1", "b2", "kc-d1-c"),
					broker("kc-1", "b0", "kc-d1-a"), {Namespace: "default", Name: "random", Node: "kc-d1-b"}}},
			want: []string{RuleDedicatedForeignPod, RulePodOffPool},
		},
		{
			name: "rule 3: DaemonSet pods may share a dedicated node",
			snap: Snapshot{Nodes: nodes(d1...),
				Pods: []Pod{dedBroker("kc-d1", "b0", "kc-d1-a"), dedBroker("kc-d1", "b1", "kc-d1-b"), dedBroker("kc-d1", "b2", "kc-d1-c"),
					{Namespace: "kube-system", Name: "kindnet-x", Node: "kc-d1-a", System: true}}},
		},
		{
			name: "rule 3: two dedicated brokers on one node",
			snap: Snapshot{Nodes: nodes(d1...),
				Pods: []Pod{dedBroker("kc-d1", "b0", "kc-d1-a"), dedBroker("kc-d1", "b1", "kc-d1-a"), dedBroker("kc-d1", "b2", "kc-d1-c")}},
			want: []string{RuleDedicatedMultiExclusive, RuleSameGroupOnNode, RuleZoneSkew},
		},
		{
			name: "rule 3: a dedicated broker of another cluster on this cluster's node",
			snap: Snapshot{Nodes: nodes(append(dedNodes("kc-d1"), dedNodes("kc-d2")...)...),
				Pods: []Pod{dedBroker("kc-d1", "b0", "kc-d1-a"), dedBroker("kc-d1", "b1", "kc-d1-b"), dedBroker("kc-d1", "b2", "kc-d1-c"),
					dedBroker("kc-d2", "b0", "kc-d1-a"), dedBroker("kc-d2", "b1", "kc-d2-b"), dedBroker("kc-d2", "b2", "kc-d2-c")}},
			// kc-d2's brokers are still one per zone (a, b, c), so no skew: the node itself is the problem.
			want: []string{RuleDedicatedForeignPod, RuleDedicatedMultiExclusive},
		},
		{
			name: "rule 3: leaked dedicated nodes after the pods are gone",
			snap: Snapshot{Nodes: nodes(d1...), Pods: nil},
			want: []string{RuleDedicatedNodeCount},
		},
		{
			name: "rule 3: node count equals broker count (+surge): 4 nodes, 3 brokers, surge 1 is fine",
			snap: Snapshot{Nodes: nodes(append(d1, Node{Name: "kc-d1-a2", Zone: "a", Pool: "dedicated", Owner: "kc-d1"})...),
				Pods: []Pod{dedBroker("kc-d1", "b0", "kc-d1-a"), dedBroker("kc-d1", "b1", "kc-d1-b"), dedBroker("kc-d1", "b2", "kc-d1-c")}},
			opt: Options{Surge: 1},
		},
		{
			name: "rule 3: 4 nodes for 3 brokers without surge allowance",
			snap: Snapshot{Nodes: nodes(append(d1, Node{Name: "kc-d1-a2", Zone: "a", Pool: "dedicated", Owner: "kc-d1"})...),
				Pods: []Pod{dedBroker("kc-d1", "b0", "kc-d1-a"), dedBroker("kc-d1", "b1", "kc-d1-b"), dedBroker("kc-d1", "b2", "kc-d1-c")}},
			want: []string{RuleDedicatedNodeCount},
		},
		{
			name: "rule 4: a shared broker on a control node and on an unlabelled node",
			snap: Snapshot{Nodes: nodes(ctrlA, sharedB, sharedC, Node{Name: "bare", Zone: "a"}),
				Pods: []Pod{broker("kc-1", "b0", "c-a"), broker("kc-1", "b1", "s-b"), broker("kc-1", "b2", "bare")}},
			want: []string{RulePodOffPool},
		},
		{
			name: "controllers belong on the control pool",
			snap: Snapshot{Nodes: nodes(ctrlA, sharedA),
				Pods: []Pod{{Namespace: "kc-1", Name: "ctl-0", Node: "c-a", Cluster: "kc-1", Role: "controller", RequiredPool: "control"},
					{Namespace: "kc-1", Name: "ctl-1", Node: "s-a", Cluster: "kc-1", Role: "controller", RequiredPool: "control"}}},
			want: []string{RulePodOffPool},
		},
	}
	covered := map[string]bool{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ruleSet(Check(c.snap, c.opt))
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("rules fired = %v, want %v\n%v", got, c.want, Check(c.snap, c.opt))
			}
		})
		for _, r := range c.want {
			covered[r] = true
		}
	}
	for _, r := range AllRules {
		if !covered[r] {
			t.Errorf("rule %s has no violating test case", r)
		}
	}
}

func TestCheckBindingOnlyReportsTheCandidate(t *testing.T) {
	n := Node{Name: "s-a", Zone: "a", Pool: "shared"}
	existing := []Pod{broker("kc-1", "b0", "s-a"), broker("kc-1", "b1", "s-a")} // already violating: not our problem
	if vs := CheckBinding(broker("kc-2", "b0", ""), n, existing); len(vs) != 0 {
		t.Fatalf("an unrelated pod must not be blocked by an existing violation: %v", vs)
	}
	if vs := CheckBinding(broker("kc-1", "b2", ""), n, existing); len(vs) == 0 || vs[0].Rule != RuleSameGroupOnNode {
		t.Fatalf("a third kc-1 broker must be refused: %v", vs)
	}
	// re-binding the same pod (retry) must not count it twice
	if vs := CheckBinding(broker("kc-1", "b0", ""), Node{Name: "s-b", Zone: "b", Pool: "shared"}, []Pod{broker("kc-1", "b0", "s-b")}); len(vs) != 0 {
		t.Fatalf("idempotent re-bind refused: %v", vs)
	}
}

func TestCheckBindingTable(t *testing.T) {
	ded := Node{Name: "d-a", Zone: "a", Pool: "dedicated", Owner: "kc-d1"}
	cases := []struct {
		name string
		pod  Pod
		node Node
		on   []Pod
		want []string
	}{
		{"shared broker to empty shared node", broker("kc-1", "b0", ""), sharedA, nil, nil},
		{"shared broker next to another tenant's broker", broker("kc-1", "b0", ""), sharedA, []Pod{broker("kc-2", "b0", "s-a")}, nil},
		{"shared broker onto dedicated node", broker("kc-1", "b0", ""), ded, nil, []string{RuleDedicatedForeignPod, RulePodOffPool}},
		{"dedicated broker onto its own empty node", dedBroker("kc-d1", "b0", ""), ded, nil, nil},
		{"dedicated broker onto a shared node", dedBroker("kc-d1", "b0", ""), sharedA, nil, []string{RulePodOffPool}},
		{"dedicated broker onto another cluster's node", dedBroker("kc-d2", "b0", ""), ded, nil, []string{RuleDedicatedForeignPod}},
		{"second broker onto an occupied dedicated node", dedBroker("kc-d1", "b1", ""), ded, []Pod{dedBroker("kc-d1", "b0", "d-a")}, []string{RuleDedicatedMultiExclusive, RuleSameGroupOnNode}},
		{"DaemonSet pod on a dedicated node", Pod{Namespace: "kube-system", Name: "ds", System: true}, ded, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ruleSet(CheckBinding(c.pod, c.node, c.on)); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}
