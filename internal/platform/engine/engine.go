// Package engine defines the contract between the engine-neutral platform core and
// data-engine implementations (Kafka today, ClickHouse next). Nothing in this
// package, or anywhere under internal/platform, may import an engine package.
package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Tier is the placement tier of a resource.
type Tier string

const (
	TierShared    Tier = "shared"
	TierDedicated Tier = "dedicated"
)

// PoolKind identifies a class of node pool in a cell.
type PoolKind string

const (
	PoolShared    PoolKind = "shared"
	PoolDedicated PoolKind = "dedicated"
	PoolControl   PoolKind = "control"
)

// ResourceSpec is the engine-neutral envelope around an engine-specific spec.
type ResourceSpec struct {
	ID     string          `json:"id"`
	Name   string          `json:"name"`
	Tenant string          `json:"tenant"`
	Engine string          `json:"engine"`
	Tier   Tier            `json:"tier"`
	Spec   json.RawMessage `json:"spec"` // opaque to the platform
}

// Resources is the per-pod demand used for capacity accounting.
type Resources struct {
	CPURequestMilli int64 `json:"cpuRequestMilli"`
	CPULimitMilli   int64 `json:"cpuLimitMilli"`
	MemoryMiB       int64 `json:"memoryMiB"`
	Volumes         int64 `json:"volumes"`    // attachment slots
	VolumeMBps      int64 `json:"volumeMBps"` // provisioned throughput, counted against node EBS bandwidth
	VolumeGiB       int64 `json:"volumeGiB"`
}

// Exclusivity describes how a pod group shares nodes.
type Exclusivity string

const (
	// ExclusivePerGroupPerNode: at most one pod of this group per node (others may share).
	ExclusivePerGroupPerNode Exclusivity = "per-group-per-node"
	// ExclusiveWholeNode: the node hosts no other pod of this kind from any tenant.
	ExclusiveWholeNode Exclusivity = "whole-node"
)

// PodGroup is a set of identical replicas with placement constraints.
type PodGroup struct {
	Name      string      `json:"name"`
	Replicas  int         `json:"replicas"`
	Pool      PoolKind    `json:"pool"`
	PerPod    Resources   `json:"perPod"`
	Zones     int         `json:"zones"` // spread evenly across this many zones (maxSkew 1)
	Exclusive Exclusivity `json:"exclusive"`
	// InstanceType selects the node shape for dedicated groups (one pod per node).
	InstanceType string `json:"instanceType,omitempty"`
}

// SpreadReplicas assigns replica i to zones[i%len(zones)] and returns the count per zone.
// The allocator and the engines both use it so that capacity accounting and node pools agree.
func SpreadReplicas(replicas int, zones []string) map[string]int {
	out := map[string]int{}
	for i := 0; i < replicas && len(zones) > 0; i++ {
		out[zones[i%len(zones)]]++
	}
	return out
}

// PlacementRequest is what an engine asks the capacity allocator for.
type PlacementRequest struct {
	Groups []PodGroup `json:"groups"`
}

// Assignment is the allocator's answer: where the resource lives.
type Assignment struct {
	CellID string `json:"cellId"`
	// Zones lists the zone names used, in order; replica i of a group goes to Zones[i % len].
	Zones []string `json:"zones"`
	// GroupZones lists, per pod group name, the zones that group was placed in.
	GroupZones map[string][]string `json:"groupZones,omitempty"`
	// OperatorShard is the operator shard label value chosen for this resource.
	OperatorShard int `json:"operatorShard"`
}

// CellEnv is the per-cell environment an engine renders against.
type CellEnv struct {
	CellID       string   `json:"cellId"`
	Domain       string   `json:"domain"`
	Zones        []string `json:"zones"`
	StorageClass string   `json:"storageClass"`
	// LocalStorageClass provisions local NVMe volumes (dedicated NVMe sizes only).
	LocalStorageClass string `json:"localStorageClass,omitempty"`
	// APIServerCIDRs are allowed as egress targets (Strimzi init container, operators).
	APIServerCIDRs []string          `json:"apiServerCIDRs,omitempty"`
	GatewayName    string            `json:"gatewayName"`
	GatewayNS      string            `json:"gatewayNamespace"`
	LabelPrefix    string            `json:"labelPrefix"`
	EngineImages   map[string]string `json:"engineImages,omitempty"`
	OperatorShards int               `json:"operatorShards"`
	// DedicatedSurge is the number of extra nodes a dedicated pool may temporarily hold during rotation.
	DedicatedSurge int `json:"dedicatedSurge,omitempty"`
}

// Taint is a node taint.
type Taint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"`
}

// NodePoolSpec asks the cell's node pool provider for nodes reserved for one resource.
type NodePoolSpec struct {
	ID           string            `json:"id"`
	Owner        string            `json:"owner"` // resource ID the nodes are reserved for
	InstanceType string            `json:"instanceType"`
	NodesPerZone map[string]int    `json:"nodesPerZone"`
	Surge        int               `json:"surge,omitempty"` // extra nodes allowed during rotation
	Labels       map[string]string `json:"labels"`
	Taints       []Taint           `json:"taints"`
}

// TotalNodes is the number of steady-state nodes in the pool.
func (n NodePoolSpec) TotalNodes() int {
	t := 0
	for _, c := range n.NodesPerZone {
		t += c
	}
	return t
}

// Bundle is the complete rendered desired state of one resource in one cell.
// Objects are applied in slice order and deleted in reverse order.
type Bundle struct {
	Objects []*unstructured.Unstructured `json:"objects"`
	// NodePools are provisioned before Objects are applied and released, after all
	// pods are gone, when the resource is deleted.
	NodePools []NodePoolSpec `json:"nodePools,omitempty"`
	// DeleteGraceSeconds, if > 0, is the termination grace the agent gives the
	// resource's pods when it is deleted (the data is being destroyed anyway, so
	// waiting out a long graceful shutdown only slows deletion down).
	DeleteGraceSeconds int64 `json:"deleteGraceSeconds,omitempty"`
	// Bounds are pre-authorised limits within which the cell agent may act
	// locally (for example storage auto-expansion) while the control plane is down.
	Bounds map[string]int64 `json:"bounds,omitempty"`
}

// Condition is a wait target for a Step.
type Condition struct {
	Type  string `json:"type"`
	Value string `json:"value,omitempty"`
}

// Step is one publish-then-wait unit of an engine plan.
type Step struct {
	Name    string        `json:"name"`
	Bundle  *Bundle       `json:"bundle,omitempty"` // nil with Tombstone=true means delete everything
	Tomb    bool          `json:"tombstone"`
	WaitFor Condition     `json:"waitFor"`
	Timeout time.Duration `json:"timeout"`
}

// ObjectStatus is what the cell agent reports for one object of a bundle.
type ObjectStatus struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Namespace  string          `json:"namespace,omitempty"`
	Name       string          `json:"name"`
	Exists     bool            `json:"exists"`
	Generation int64           `json:"generation,omitempty"`
	Status     json.RawMessage `json:"status,omitempty"`
}

// Observed is the raw observation of a resource, as reported by the cell agent.
type Observed struct {
	Generation int64          `json:"generation"` // bundle generation the agent last applied
	Deleted    bool           `json:"deleted"`    // tombstone fully applied
	Objects    []ObjectStatus `json:"objects"`
	Probe      map[string]any `json:"probe,omitempty"`
	ApplyError string         `json:"applyError,omitempty"`
}

// Status is the engine's interpretation of an observation.
type Status struct {
	Ready   bool              `json:"ready"`
	Deleted bool              `json:"deleted"`
	Reason  string            `json:"reason,omitempty"`
	Values  map[string]string `json:"values,omitempty"` // e.g. MetadataVersion
}

// Satisfies reports whether status meets the wait condition.
func (s Status) Satisfies(c Condition) bool {
	switch c.Type {
	case "Ready":
		return s.Ready
	case "Deleted":
		return s.Deleted
	default:
		return s.Values != nil && s.Values[c.Type] == c.Value
	}
}

// Engine is implemented by each data engine. All methods are pure and deterministic.
type Engine interface {
	Name() string
	Validate(spec ResourceSpec, prior *ResourceSpec) error
	PlanPlacement(spec ResourceSpec) (PlacementRequest, error)
	Render(spec ResourceSpec, a Assignment, env CellEnv) (*Bundle, error)
	PlanChange(from, to ResourceSpec, a Assignment, env CellEnv) ([]Step, error)
	PlanDelete(spec ResourceSpec, a Assignment) ([]Step, error)
	ObserveStatus(spec ResourceSpec, obs Observed) (Status, error)
}

// ValidationError is returned by Validate for user-correctable problems.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// Invalid builds a ValidationError.
func Invalid(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

// Registry holds the engines compiled into a binary.
type Registry struct {
	mu      sync.RWMutex
	engines map[string]Engine
}

// NewRegistry returns a registry containing the given engines.
func NewRegistry(es ...Engine) *Registry {
	r := &Registry{engines: map[string]Engine{}}
	for _, e := range es {
		r.engines[e.Name()] = e
	}
	return r
}

// Get looks up an engine by name.
func (r *Registry) Get(name string) (Engine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.engines[name]
	if !ok {
		return nil, Invalid("unknown engine %q", name)
	}
	return e, nil
}

// Names lists registered engines.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.engines))
	for n := range r.engines {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
