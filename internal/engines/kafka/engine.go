// Package kafka implements the platform Engine for Apache Kafka on Strimzi
// (kafka.strimzi.io/v1, KRaft, node pools).
package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// Name is the engine identifier.
const Name = "kafka"

// Spec is the tenant-facing Kafka cluster specification (engine-opaque to the platform).
type Spec struct {
	KafkaVersion string            `json:"kafkaVersion"`
	BrokerType   string            `json:"brokerType"`
	BrokerCount  int               `json:"brokerCount"`
	Storage      StorageSpec       `json:"storage"`
	Config       map[string]string `json:"config,omitempty"`
}

// StorageSpec is per-broker storage.
type StorageSpec struct {
	SizeGiB    int64 `json:"sizeGiB"`
	MaxSizeGiB int64 `json:"maxSizeGiB,omitempty"`
	AutoExpand bool  `json:"autoExpand,omitempty"`
}

// Engine is the Kafka engine. It is stateless.
type Engine struct{}

// New returns the Kafka engine.
func New() *Engine { return &Engine{} }

// Name implements engine.Engine.
func (*Engine) Name() string { return Name }

// ParseSpec decodes the engine spec from a ResourceSpec.
func ParseSpec(rs engine.ResourceSpec) (Spec, error) {
	var s Spec
	if len(rs.Spec) == 0 {
		return s, engine.Invalid("spec is required")
	}
	if err := strictUnmarshal(rs.Spec, &s); err != nil {
		return s, engine.Invalid("invalid kafka spec: %v", err)
	}
	return s, nil
}

var nameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$`)

// Validate implements engine.Engine.
func (e *Engine) Validate(rs engine.ResourceSpec, prior *engine.ResourceSpec) error {
	if !nameRE.MatchString(rs.Name) {
		return engine.Invalid("name must be 1-40 chars of [a-z0-9-], starting and ending alphanumeric")
	}
	s, err := ParseSpec(rs)
	if err != nil {
		return err
	}
	if !slices.Contains(SupportedVersions, s.KafkaVersion) {
		return engine.Invalid("kafkaVersion %q is not supported; supported: %v", s.KafkaVersion, SupportedVersions)
	}
	bt, ok := LookupBrokerType(s.BrokerType)
	if !ok {
		return engine.Invalid("unknown brokerType %q", s.BrokerType)
	}
	if bt.Tier != rs.Tier {
		return engine.Invalid("brokerType %q belongs to tier %q, not %q", bt.Name, bt.Tier, rs.Tier)
	}
	if s.BrokerCount < 3 || s.BrokerCount%3 != 0 || s.BrokerCount > 48 {
		return engine.Invalid("brokerCount must be a multiple of 3 (one per zone), between 3 and 48")
	}
	if s.Storage.SizeGiB < bt.MinStorageGiB || s.Storage.SizeGiB > bt.MaxStorageGiB {
		return engine.Invalid("storage.sizeGiB must be between %d and %d for %s", bt.MinStorageGiB, bt.MaxStorageGiB, bt.Name)
	}
	if s.Storage.MaxSizeGiB != 0 && (s.Storage.MaxSizeGiB < s.Storage.SizeGiB || s.Storage.MaxSizeGiB > bt.MaxStorageGiB) {
		return engine.Invalid("storage.maxSizeGiB must be between sizeGiB and %d", bt.MaxStorageGiB)
	}
	if s.Storage.AutoExpand && bt.LocalNVMe {
		return engine.Invalid("storage.autoExpand is not available on local NVMe broker types")
	}
	if err := ValidateConfig(s.Config); err != nil {
		return err
	}
	if prior != nil {
		ps, err := ParseSpec(*prior)
		if err == nil && s.Storage.SizeGiB < ps.Storage.SizeGiB {
			return engine.Invalid("storage cannot shrink (%d < %d GiB)", s.Storage.SizeGiB, ps.Storage.SizeGiB)
		}
		if prior.Tier != rs.Tier {
			return engine.Invalid("tier changes use the tier migration operation")
		}
	}
	return nil
}

// PlanPlacement implements engine.Engine.
func (e *Engine) PlanPlacement(rs engine.ResourceSpec) (engine.PlacementRequest, error) {
	s, err := ParseSpec(rs)
	if err != nil {
		return engine.PlacementRequest{}, err
	}
	bt, ok := LookupBrokerType(s.BrokerType)
	if !ok {
		return engine.PlacementRequest{}, engine.Invalid("unknown brokerType %q", s.BrokerType)
	}
	ct := controllerFor(bt)
	brokerPool, excl := engine.PoolShared, engine.ExclusivePerGroupPerNode
	if rs.Tier == engine.TierDedicated {
		brokerPool, excl = engine.PoolDedicated, engine.ExclusiveWholeNode
	}
	vols := int64(1)
	if bt.LocalNVMe {
		vols = 0
	}
	return engine.PlacementRequest{Groups: []engine.PodGroup{
		{
			Name: "controllers", Replicas: 3, Pool: engine.PoolControl, Zones: 3,
			Exclusive: engine.ExclusivePerGroupPerNode,
			PerPod: engine.Resources{CPURequestMilli: ct.CPURequestMilli, CPULimitMilli: ct.CPULimitMilli,
				MemoryMiB: ct.MemoryMiB, Volumes: 1, VolumeGiB: ct.StorageGiB},
		},
		{
			// Cruise Control + Entity Operator: one pod each on the control pool.
			Name: "cluster-services", Replicas: 1, Pool: engine.PoolControl, Zones: 1,
			Exclusive: engine.ExclusivePerGroupPerNode, PerPod: clusterServices(bt),
		},
		{
			Name: "brokers", Replicas: s.BrokerCount, Pool: brokerPool, Zones: 3, Exclusive: excl,
			PerPod: engine.Resources{CPURequestMilli: bt.CPURequestMilli, CPULimitMilli: bt.CPULimitMilli,
				MemoryMiB: bt.MemoryMiB, Volumes: vols, VolumeMBps: bt.VolumeMBps, VolumeGiB: s.Storage.SizeGiB},
		},
	}}, nil
}

// PlanChange implements engine.Engine. Lifecycle changes arrive in M4.
func (e *Engine) PlanChange(from, to engine.ResourceSpec, a engine.Assignment, env engine.CellEnv) ([]engine.Step, error) {
	return nil, engine.Invalid("cluster updates are not implemented yet (milestone M4)")
}

// PlanDelete implements engine.Engine.
func (e *Engine) PlanDelete(rs engine.ResourceSpec, a engine.Assignment) ([]engine.Step, error) {
	return []engine.Step{{Name: "delete", Tomb: true, WaitFor: engine.Condition{Type: "Deleted"}, Timeout: 30 * time.Minute}}, nil
}

// ObserveStatus implements engine.Engine: the cluster is Ready when every rendered
// object exists and the Kafka CR reports Ready=True for its current generation.
func (e *Engine) ObserveStatus(rs engine.ResourceSpec, obs engine.Observed) (engine.Status, error) {
	if obs.Deleted {
		return engine.Status{Deleted: true}, nil
	}
	if obs.ApplyError != "" {
		return engine.Status{Reason: "apply error: " + obs.ApplyError}, nil
	}
	var kafkaObj *engine.ObjectStatus
	for i := range obs.Objects {
		o := &obs.Objects[i]
		if !o.Exists {
			return engine.Status{Reason: fmt.Sprintf("%s %s/%s not present yet", o.Kind, o.Namespace, o.Name)}, nil
		}
		if o.Kind == "Kafka" {
			kafkaObj = o
		}
	}
	if kafkaObj == nil {
		return engine.Status{Reason: "Kafka resource not reported"}, nil
	}
	var st struct {
		ObservedGeneration int64 `json:"observedGeneration"`
		Conditions         []struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"conditions"`
		KafkaVersion         string `json:"kafkaVersion"`
		KafkaMetadataVersion string `json:"kafkaMetadataVersion"`
		Listeners            []struct {
			Name             string `json:"name"`
			BootstrapServers string `json:"bootstrapServers"`
		} `json:"listeners"`
	}
	if len(kafkaObj.Status) > 0 {
		if err := json.Unmarshal(kafkaObj.Status, &st); err != nil {
			return engine.Status{Reason: "unparseable Kafka status"}, nil
		}
	}
	vals := map[string]string{"KafkaVersion": st.KafkaVersion, "MetadataVersion": st.KafkaMetadataVersion}
	for _, l := range st.Listeners {
		if l.Name == "external" {
			vals["endpoint.scram-sha-512"] = l.BootstrapServers
		}
	}
	if st.ObservedGeneration < kafkaObj.Generation {
		return engine.Status{Reason: "operator has not reconciled the latest generation", Values: vals}, nil
	}
	for _, c := range st.Conditions {
		if c.Type == "Ready" {
			if c.Status == "True" {
				return engine.Status{Ready: true, Values: vals}, nil
			}
			return engine.Status{Reason: fmt.Sprintf("Kafka not ready: %s %s", c.Reason, c.Message), Values: vals}, nil
		}
	}
	return engine.Status{Reason: "Kafka has no Ready condition yet", Values: vals}, nil
}

func strictUnmarshal(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
