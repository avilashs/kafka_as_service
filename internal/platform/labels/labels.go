// Package labels defines the engine-neutral Kubernetes label and taint keys the
// platform uses to express placement. Engines put them on the pods they render;
// the admission policy, binding webhook and auditor read them. Nothing here is
// specific to one engine.
package labels

// Prefix is the label/annotation/taint key prefix (working name; see design §1).
const Prefix = "streamcell.dev/"

// Labels on managed objects and pods.
const (
	Cluster   = Prefix + "cluster"   // resource ID
	Tenant    = Prefix + "tenant"    // tenant ID
	Tier      = Prefix + "tier"      // shared | dedicated
	Role      = Prefix + "role"      // pod group name within the resource (engine-defined)
	Inventory = Prefix + "inventory" // set on every object of a resource; agent prune scope
	Shard     = Prefix + "operator-shard"

	// RequiredPool is the kind of node pool a pod must run on: shared | dedicated | control.
	RequiredPool = Prefix + "required-pool"
	// Exclusive describes node sharing: "per-resource" (at most one pod of the same
	// resource and role per node) or "whole-node" (no other tenant workload on the node,
	// at most one such pod).
	Exclusive = Prefix + "exclusive"
)

// Values of RequiredPool and of the node label Pool.
const (
	PoolShared    = "shared"
	PoolDedicated = "dedicated"
	PoolControl   = "control"
	PoolSpare     = "spare" // unclaimed nodes that the kind provider can turn into dedicated pools
)

// Values of Exclusive.
const (
	ExclusivePerResource = "per-resource"
	ExclusiveWholeNode   = "whole-node"
)

// Node labels and taints.
const (
	NodePool       = Prefix + "pool"            // pool kind of the node
	DedicatedOwner = Prefix + "dedicated-owner" // resource ID a dedicated node is reserved for
	NodePoolID     = Prefix + "nodepool"        // ID of the NodePoolSpec that claimed the node
	TaintDedicated = Prefix + "dedicated"       // value = owner resource ID, effect NoSchedule
	TaintControl   = Prefix + "control"         // control pool taint
	TaintSpare     = Prefix + "spare"           // keeps general workloads off unclaimed spare nodes (kind)
	Zone           = "topology.kubernetes.io/zone"
	Hostname       = "kubernetes.io/hostname"
)
