// Package kafka implements the platform Engine for Apache Kafka on Strimzi
// (kafka.strimzi.io/v1, KRaft, node pools).
package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
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
	Users        []User            `json:"users,omitempty"`
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
	if err := validateUsers(s.Users); err != nil {
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
	brokerPool, excl, instance := engine.PoolShared, engine.ExclusivePerGroupPerNode, ""
	if rs.Tier == engine.TierDedicated {
		brokerPool, excl, instance = engine.PoolDedicated, engine.ExclusiveWholeNode, bt.InstanceType
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
			Name: "brokers", Replicas: s.BrokerCount, Pool: brokerPool, Zones: 3, Exclusive: excl, InstanceType: instance,
			PerPod: engine.Resources{CPURequestMilli: bt.CPURequestMilli, CPULimitMilli: bt.CPULimitMilli,
				MemoryMiB: bt.MemoryMiB, Volumes: vols, VolumeMBps: bt.VolumeMBps, VolumeGiB: s.Storage.SizeGiB},
		},
	}}, nil
}

// PlanChange implements engine.Engine. Every lifecycle change of the Kafka engine is one publish of the
// new bundle followed by a wait for Ready: Strimzi performs the safe sequence itself (one rolling
// restart at a time gated on minISR; auto-rebalance for scale), and ObserveStatus reports Ready only when
// that sequence has finished (see the gates there). The platform adds the reservation before and the
// accounting after.
func (e *Engine) PlanChange(from, to engine.ResourceSpec, a engine.Assignment, env engine.CellEnv) ([]engine.Step, error) {
	if _, err := e.ClassifyChange(from, to); err != nil {
		return nil, err
	}
	b, err := e.Render(to, a, env)
	if err != nil {
		return nil, err
	}
	return []engine.Step{{Name: "apply", Bundle: b, WaitFor: engine.Condition{Type: "Ready"}, Timeout: 45 * time.Minute}}, nil
}

var _ engine.Changer = (*Engine)(nil)

// ClassifyChange implements engine.Changer.
func (e *Engine) ClassifyChange(from, to engine.ResourceSpec) (engine.Change, error) {
	f, err := ParseSpec(from)
	if err != nil {
		return engine.Change{}, err
	}
	t, err := ParseSpec(to)
	if err != nil {
		return engine.Change{}, err
	}
	if from.Tier != to.Tier {
		return engine.Change{}, engine.Invalid("tier changes use the tier migration operation")
	}
	if !slicesEqualUsers(f.Users, t.Users) {
		return engine.Change{}, engine.Invalid("users are changed through the user endpoints")
	}
	var kinds []string
	if f.KafkaVersion != t.KafkaVersion {
		kinds = append(kinds, "version")
	}
	if f.BrokerType != t.BrokerType {
		kinds = append(kinds, "size")
	}
	if f.BrokerCount != t.BrokerCount {
		kinds = append(kinds, "scale")
	}
	if f.Storage != t.Storage {
		kinds = append(kinds, "storage")
	}
	if !mapsEqual(f.Config, t.Config) {
		kinds = append(kinds, "config")
	}
	switch len(kinds) {
	case 0:
		return engine.Change{}, engine.Invalid("the request changes nothing")
	case 1:
	default:
		return engine.Change{}, engine.Invalid("one kind of change per request; this one changes %v", kinds)
	}
	switch kinds[0] {
	case "version":
		fi, ti := slices.Index(SupportedVersions, f.KafkaVersion), slices.Index(SupportedVersions, t.KafkaVersion)
		if fi < 0 || ti < 0 || ti < fi {
			return engine.Change{}, engine.Invalid("kafkaVersion can only move forward (%s -> %s)", f.KafkaVersion, t.KafkaVersion)
		}
	case "size":
		fb, _ := LookupBrokerType(f.BrokerType)
		tb, _ := LookupBrokerType(t.BrokerType)
		if fb.LocalNVMe != tb.LocalNVMe {
			return engine.Change{}, engine.Invalid("changing between local-NVMe and network-volume broker types needs a node rotation, which is not available yet")
		}
		if t.Storage.SizeGiB > tb.MaxStorageGiB {
			return engine.Change{}, engine.Invalid("the current storage (%d GiB) exceeds the maximum of %s", t.Storage.SizeGiB, tb.Name)
		}
	case "scale":
		// Scale in is refused by Strimzi itself while brokers still hold partitions; the auto-rebalance
		// empties them first. A projected-disk-use pre-check needs broker metrics (M5).
	case "storage":
		if t.Storage.SizeGiB < f.Storage.SizeGiB {
			return engine.Change{}, engine.Invalid("storage cannot shrink (%d < %d GiB)", t.Storage.SizeGiB, f.Storage.SizeGiB)
		}
	}
	return engine.Change{Kind: kinds[0], NeedsCapacity: kinds[0] == "size" || kinds[0] == "scale"}, nil
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func slicesEqualUsers(a, b []User) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
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
	for i := range obs.Objects {
		if o := &obs.Objects[i]; o.Kind == "KafkaUser" {
			if ready, why := resourceReady(o); !ready {
				return engine.Status{Reason: fmt.Sprintf("KafkaUser %s: %s", o.Name, why)}, nil
			}
		}
	}
	// A certificate-authenticated user is usable only once the cell has signed its certificate.
	if sp, err := parseSpecJSON(rs.Spec); err == nil {
		for _, u := range sp.Users {
			if u.isTLS() && obs.Outputs[engine.CertOutputPrefix+u.Name] == "" {
				return engine.Status{Reason: fmt.Sprintf("certificate of user %s not issued yet", u.Name)}, nil
			}
		}
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
		AutoRebalance        struct {
			State string `json:"state"`
		} `json:"autoRebalance"`
		Listeners []struct {
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
		switch l.Name {
		case "external":
			vals["endpoint.scram-sha-512"] = l.BootstrapServers
		case "mtls":
			vals["endpoint.mtls"] = l.BootstrapServers
		}
	}
	if st.ObservedGeneration < kafkaObj.Generation {
		return engine.Status{Reason: "operator has not reconciled the latest generation", Values: vals}, nil
	}
	// A scale operation is finished only when Cruise Control has moved the partitions: while an
	// auto-rebalance runs the cluster is not "Ready" for our purposes, even if Strimzi says so.
	if s := st.AutoRebalance.State; s != "" && s != "Idle" {
		return engine.Status{Reason: "auto-rebalance in progress: " + s, Values: vals}, nil
	}
	// Changes to resources, storage or the number of brokers change only the KafkaNodePool, not the Kafka
	// resource, so the Kafka resource's own generation check above cannot see them (found in the first
	// lifecycle run: a resize was reported done in 5 s with the old limits still in place). Strimzi writes the
	// node pool status at the end of the reconciliation that handled the change, so a pool is settled when its
	// observedGeneration has caught up; the broker pool must also have the size that was asked for.
	sp, spErr := parseSpecJSON(rs.Spec)
	for i := range obs.Objects {
		o := &obs.Objects[i]
		if o.Kind != "KafkaNodePool" {
			continue
		}
		var ps struct {
			Replicas           int   `json:"replicas"`
			ObservedGeneration int64 `json:"observedGeneration"`
		}
		_ = json.Unmarshal(o.Status, &ps)
		if ps.ObservedGeneration < o.Generation {
			return engine.Status{Reason: fmt.Sprintf("node pool %s: operator has not reconciled the latest generation", o.Name), Values: vals}, nil
		}
		if spErr == nil && (o.Name == "brokers" || strings.HasSuffix(o.Name, "-brokers")) && ps.Replicas != sp.BrokerCount {
			return engine.Status{Reason: fmt.Sprintf("broker pool has %d of %d brokers", ps.Replicas, sp.BrokerCount), Values: vals}, nil
		}
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

// resourceReady reads a Strimzi resource's Ready condition for its current generation.
func resourceReady(o *engine.ObjectStatus) (bool, string) {
	var st struct {
		ObservedGeneration int64 `json:"observedGeneration"`
		Conditions         []struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"conditions"`
	}
	if len(o.Status) > 0 {
		if err := json.Unmarshal(o.Status, &st); err != nil {
			return false, "unparseable status"
		}
	}
	if st.ObservedGeneration < o.Generation {
		return false, "not yet reconciled"
	}
	for _, c := range st.Conditions {
		if c.Type == "Ready" {
			if c.Status == "True" {
				return true, ""
			}
			return false, c.Reason + " " + c.Message
		}
	}
	return false, "no Ready condition yet"
}
