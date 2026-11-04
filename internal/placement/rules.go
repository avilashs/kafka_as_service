// Package placement holds the single definition of what a placement violation is.
// The binding webhook (would this bind create a violation?), the continuous
// auditor and the streamcell-audit CLI (does the cell currently contain one?) all
// call the same functions, so the three layers cannot drift apart.
//
// The rules speak only the engine-neutral labels in internal/platform/labels; they
// know nothing about Kafka.
package placement

import (
	"fmt"
	"sort"
	"strings"

	"github.com/avilashs/kafka_as_service/internal/platform/labels"
)

// Rule names (the `rule` label of streamcell_placement_violations).
const (
	// RuleSameGroupOnNode: two pods of one resource and role (e.g. two brokers of one
	// cluster) share a node. Design rule 1, both tiers.
	RuleSameGroupOnNode = "same_group_on_node"
	// RuleZoneSkew: the pods of one resource and role are spread unevenly across zones.
	// Design rule 2.
	RuleZoneSkew = "zone_skew"
	// RuleDedicatedForeignPod: a pod that does not belong to the node's owner (or any
	// non-DaemonSet pod without a placement label) runs on a dedicated node. Design rule 3.
	RuleDedicatedForeignPod = "dedicated_node_foreign_pod"
	// RuleDedicatedMultiExclusive: more than one whole-node pod on a dedicated node
	// (e.g. two brokers). Design rule 3.
	RuleDedicatedMultiExclusive = "dedicated_node_multi_exclusive"
	// RuleDedicatedNodeCount: a resource holds more dedicated nodes than it has pods
	// plus the allowed surge, i.e. a leaked or oversized pool. Design rule 3.
	RuleDedicatedNodeCount = "dedicated_node_count_mismatch"
	// RulePodOffPool: a pod runs on a node of a different pool kind than it requires
	// (e.g. a shared broker on a dedicated or control node). Design rule 4.
	RulePodOffPool = "pod_off_pool"
)

// AllRules lists every rule, for documentation and tests.
var AllRules = []string{RuleSameGroupOnNode, RuleZoneSkew, RuleDedicatedForeignPod,
	RuleDedicatedMultiExclusive, RuleDedicatedNodeCount, RulePodOffPool}

// Pod is the placement-relevant view of a pod.
type Pod struct {
	Namespace string
	Name      string
	Node      string // empty while Pending
	// Managed pods carry the platform placement labels.
	Cluster      string
	Role         string
	RequiredPool string
	Exclusive    string
	// System pods (DaemonSet-owned or static/mirror pods) may share any node.
	System bool
}

// Managed reports whether the pod belongs to a platform resource.
func (p Pod) Managed() bool { return p.Cluster != "" }

// Ref is "namespace/name".
func (p Pod) Ref() string { return p.Namespace + "/" + p.Name }

// Node is the placement-relevant view of a node.
type Node struct {
	Name  string
	Zone  string
	Pool  string // value of the streamcell.dev/pool label ("" if unlabelled)
	Owner string // value of streamcell.dev/dedicated-owner
}

// Snapshot is the actual state of a cell.
type Snapshot struct {
	Nodes map[string]Node
	Pods  []Pod
}

// Options tunes the cluster-level rules.
type Options struct {
	// Surge is the number of dedicated nodes a resource may hold beyond its pod count (rotation).
	Surge int
}

// Violation is one broken rule.
type Violation struct {
	Rule    string   `json:"rule"`
	Cluster string   `json:"cluster,omitempty"`
	Node    string   `json:"node,omitempty"`
	Pods    []string `json:"pods,omitempty"`
	Detail  string   `json:"detail"`
}

func (v Violation) String() string {
	return fmt.Sprintf("%s cluster=%s node=%s pods=%v: %s", v.Rule, v.Cluster, v.Node, v.Pods, v.Detail)
}

// CheckNode applies the node-local rules to the pods running on one node.
// It is what the binding webhook evaluates for a hypothetical bind.
func CheckNode(n Node, pods []Pod) []Violation {
	var out []Violation
	groups := map[string][]Pod{}
	var wholeNode []Pod
	for _, p := range pods {
		if p.System {
			continue
		}
		if !p.Managed() {
			if n.Pool == labels.PoolDedicated {
				out = append(out, Violation{Rule: RuleDedicatedForeignPod, Node: n.Name, Pods: []string{p.Ref()},
					Detail: fmt.Sprintf("pod %s has no placement labels but runs on dedicated node %s (owner %q)", p.Ref(), n.Name, n.Owner)})
			}
			continue
		}
		if p.RequiredPool != n.Pool {
			out = append(out, Violation{Rule: RulePodOffPool, Cluster: p.Cluster, Node: n.Name, Pods: []string{p.Ref()},
				Detail: fmt.Sprintf("pod %s requires a %q node but is on %s, a %q node", p.Ref(), p.RequiredPool, n.Name, n.Pool)})
		}
		if n.Pool == labels.PoolDedicated && n.Owner != p.Cluster {
			out = append(out, Violation{Rule: RuleDedicatedForeignPod, Cluster: p.Cluster, Node: n.Name, Pods: []string{p.Ref()},
				Detail: fmt.Sprintf("pod %s belongs to %s but runs on %s, a dedicated node owned by %q", p.Ref(), p.Cluster, n.Name, n.Owner)})
		}
		if p.Exclusive != "" {
			k := p.Cluster + "/" + p.Role
			groups[k] = append(groups[k], p)
		}
		if p.Exclusive == labels.ExclusiveWholeNode {
			wholeNode = append(wholeNode, p)
		}
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if g := groups[k]; len(g) > 1 {
			out = append(out, Violation{Rule: RuleSameGroupOnNode, Cluster: g[0].Cluster, Node: n.Name, Pods: refs(g),
				Detail: fmt.Sprintf("%d %s pods of %s share node %s", len(g), g[0].Role, g[0].Cluster, n.Name)})
		}
	}
	if len(wholeNode) > 1 {
		out = append(out, Violation{Rule: RuleDedicatedMultiExclusive, Node: n.Name, Pods: refs(wholeNode),
			Detail: fmt.Sprintf("%d whole-node pods share node %s", len(wholeNode), n.Name)})
	}
	return out
}

// CheckBinding evaluates whether binding candidate to node would break a node-local
// rule, given the pods already on the node. Only violations involving the candidate
// are returned, so pre-existing problems elsewhere do not block unrelated pods.
func CheckBinding(candidate Pod, n Node, onNode []Pod) []Violation {
	candidate.Node = n.Name
	pods := make([]Pod, 0, len(onNode)+1)
	for _, p := range onNode {
		if p.Ref() != candidate.Ref() {
			pods = append(pods, p)
		}
	}
	pods = append(pods, candidate)
	var out []Violation
	for _, v := range CheckNode(n, pods) {
		for _, ref := range v.Pods {
			if ref == candidate.Ref() {
				out = append(out, v)
				break
			}
		}
	}
	return out
}

// Check applies every rule to a snapshot of the cell.
func Check(s Snapshot, opt Options) []Violation {
	byNode := map[string][]Pod{}
	for _, p := range s.Pods {
		if p.Node != "" {
			byNode[p.Node] = append(byNode[p.Node], p)
		}
	}
	names := make([]string, 0, len(byNode))
	for n := range byNode {
		names = append(names, n)
	}
	sort.Strings(names)
	var out []Violation
	for _, name := range names {
		node, ok := s.Nodes[name]
		if !ok {
			node = Node{Name: name} // unknown node: no pool label
		}
		out = append(out, CheckNode(node, byNode[name])...)
	}
	out = append(out, checkZoneSkew(s)...)
	out = append(out, checkDedicatedNodeCount(s, opt)...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Rule != b.Rule {
			return a.Rule < b.Rule
		}
		if a.Cluster != b.Cluster {
			return a.Cluster < b.Cluster
		}
		return a.Node < b.Node
	})
	return out
}

// checkZoneSkew: for every resource and role whose pods are spread (Exclusive set),
// the number of placed pods per candidate zone may differ by at most 1. Candidate
// zones are those of the nodes of the pool the pods require (the owner's nodes for
// dedicated pools).
func checkZoneSkew(s Snapshot) []Violation {
	type key struct{ cluster, role, pool string }
	placed := map[key]map[string]int{}
	for _, p := range s.Pods {
		if p.System || !p.Managed() || p.Exclusive == "" || p.Node == "" {
			continue
		}
		n, ok := s.Nodes[p.Node]
		if !ok || n.Zone == "" {
			continue
		}
		k := key{p.Cluster, p.Role, p.RequiredPool}
		if placed[k] == nil {
			placed[k] = map[string]int{}
		}
		placed[k][n.Zone]++
	}
	var out []Violation
	for k, perZone := range placed {
		zones := map[string]bool{}
		for _, n := range s.Nodes {
			if n.Zone == "" || n.Pool != k.pool {
				continue
			}
			if k.pool == labels.PoolDedicated && n.Owner != k.cluster {
				continue
			}
			zones[n.Zone] = true
		}
		lo, hi := -1, 0
		for z := range zones {
			c := perZone[z]
			if lo < 0 || c < lo {
				lo = c
			}
			hi = max(hi, c)
		}
		if lo >= 0 && hi-lo > 1 {
			out = append(out, Violation{Rule: RuleZoneSkew, Cluster: k.cluster,
				Detail: fmt.Sprintf("%s pods of %s per zone: %s (max skew 1)", k.role, k.cluster, fmtZones(perZone, zones))})
		}
	}
	return out
}

func fmtZones(perZone map[string]int, zones map[string]bool) string {
	names := make([]string, 0, len(zones))
	for z := range zones {
		names = append(names, z)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, z := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", z, perZone[z]))
	}
	return strings.Join(parts, " ")
}

// checkDedicatedNodeCount: a resource may hold at most (its whole-node pods) + surge dedicated nodes.
func checkDedicatedNodeCount(s Snapshot, opt Options) []Violation {
	nodes := map[string]int{}
	for _, n := range s.Nodes {
		if n.Pool == labels.PoolDedicated && n.Owner != "" {
			nodes[n.Owner]++
		}
	}
	pods := map[string]int{}
	for _, p := range s.Pods { // Pending pods count: they are the demand for nodes
		if !p.System && p.Managed() && p.Exclusive == labels.ExclusiveWholeNode {
			pods[p.Cluster]++
		}
	}
	var out []Violation
	for owner, n := range nodes {
		if n > pods[owner]+opt.Surge {
			out = append(out, Violation{Rule: RuleDedicatedNodeCount, Cluster: owner,
				Detail: fmt.Sprintf("%s holds %d dedicated nodes but has %d whole-node pods (+%d surge allowed)", owner, n, pods[owner], opt.Surge)})
		}
	}
	return out
}

func refs(ps []Pod) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.Ref())
	}
	sort.Strings(out)
	return out
}
