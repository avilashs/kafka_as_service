package kafka

import "github.com/avilashs/kafka_as_service/internal/platform/engine"

// BrokerType is a sellable broker size. See docs/design.md §11.
type BrokerType struct {
	Name            string
	Tier            engine.Tier
	CPURequestMilli int64
	CPULimitMilli   int64
	MemoryMiB       int64
	HeapMiB         int64
	MinStorageGiB   int64
	MaxStorageGiB   int64
	VolumeMBps      int64
	VolumeIOPS      int64
	EgressMbit      int64
	MaxPartitions   int64 // partition replicas per broker
	LocalNVMe       bool
	// InstanceType is the node shape of dedicated sizes (one broker per node).
	InstanceType string
}

// ControllerType is the KRaft controller size used for a tier.
type ControllerType struct {
	Name            string
	CPURequestMilli int64
	CPULimitMilli   int64
	MemoryMiB       int64
	HeapMiB         int64
	StorageGiB      int64
}

var brokerTypes = map[string]BrokerType{
	// Shared tier: CPU request = nominal / 2.0 (ADR-0009 provisional default), memory request = limit.
	"s.small":  {Name: "s.small", Tier: engine.TierShared, CPURequestMilli: 1000, CPULimitMilli: 2000, MemoryMiB: 6 * 1024, HeapMiB: 2 * 1024, MinStorageGiB: 10, MaxStorageGiB: 2 * 1024, VolumeMBps: 125, VolumeIOPS: 3000, EgressMbit: 250, MaxPartitions: 300},
	"s.medium": {Name: "s.medium", Tier: engine.TierShared, CPURequestMilli: 2000, CPULimitMilli: 4000, MemoryMiB: 12 * 1024, HeapMiB: 4 * 1024, MinStorageGiB: 10, MaxStorageGiB: 4 * 1024, VolumeMBps: 250, VolumeIOPS: 4000, EgressMbit: 500, MaxPartitions: 1000},
	"s.large":  {Name: "s.large", Tier: engine.TierShared, CPURequestMilli: 4000, CPULimitMilli: 8000, MemoryMiB: 24 * 1024, HeapMiB: 6 * 1024, MinStorageGiB: 10, MaxStorageGiB: 8 * 1024, VolumeMBps: 500, VolumeIOPS: 6000, EgressMbit: 1000, MaxPartitions: 2000},
	// Dedicated tier: Guaranteed, whole node minus reservation.
	"d.xlarge":       {Name: "d.xlarge", Tier: engine.TierDedicated, InstanceType: "m7i.xlarge", CPURequestMilli: 3200, CPULimitMilli: 3200, MemoryMiB: 12800, HeapMiB: 4 * 1024, MinStorageGiB: 100, MaxStorageGiB: 16 * 1024, VolumeMBps: 250, VolumeIOPS: 6000, MaxPartitions: 2000},
	"d.2xlarge":      {Name: "d.2xlarge", Tier: engine.TierDedicated, InstanceType: "m7i.2xlarge", CPURequestMilli: 7000, CPULimitMilli: 7000, MemoryMiB: 27 * 1024, HeapMiB: 6 * 1024, MinStorageGiB: 100, MaxStorageGiB: 16 * 1024, VolumeMBps: 500, VolumeIOPS: 8000, MaxPartitions: 3000},
	"d.4xlarge":      {Name: "d.4xlarge", Tier: engine.TierDedicated, InstanceType: "m7i.4xlarge", CPURequestMilli: 15000, CPULimitMilli: 15000, MemoryMiB: 58 * 1024, HeapMiB: 8 * 1024, MinStorageGiB: 100, MaxStorageGiB: 16 * 1024, VolumeMBps: 1000, VolumeIOPS: 12000, MaxPartitions: 4000},
	"d.8xlarge":      {Name: "d.8xlarge", Tier: engine.TierDedicated, InstanceType: "m7i.8xlarge", CPURequestMilli: 31000, CPULimitMilli: 31000, MemoryMiB: 120 * 1024, HeapMiB: 10 * 1024, MinStorageGiB: 100, MaxStorageGiB: 16 * 1024, VolumeMBps: 1000, VolumeIOPS: 16000, MaxPartitions: 6000},
	"d.nvme.2xlarge": {Name: "d.nvme.2xlarge", Tier: engine.TierDedicated, InstanceType: "i4i.2xlarge", CPURequestMilli: 7000, CPULimitMilli: 7000, MemoryMiB: 58 * 1024, HeapMiB: 6 * 1024, MinStorageGiB: 1700, MaxStorageGiB: 1700, MaxPartitions: 3000, LocalNVMe: true},
	"d.nvme.4xlarge": {Name: "d.nvme.4xlarge", Tier: engine.TierDedicated, InstanceType: "i4i.4xlarge", CPURequestMilli: 15000, CPULimitMilli: 15000, MemoryMiB: 120 * 1024, HeapMiB: 8 * 1024, MinStorageGiB: 3400, MaxStorageGiB: 3400, MaxPartitions: 4000, LocalNVMe: true},
	// Development sizes used by the kind environment only (tiny heap).
	"dev.ded":  {Name: "dev.ded", Tier: engine.TierDedicated, InstanceType: "kind", CPURequestMilli: 500, CPULimitMilli: 500, MemoryMiB: 768, HeapMiB: 256, MinStorageGiB: 1, MaxStorageGiB: 10, MaxPartitions: 100},
	"dev.tiny": {Name: "dev.tiny", Tier: engine.TierShared, CPURequestMilli: 100, CPULimitMilli: 1000, MemoryMiB: 768, HeapMiB: 256, MinStorageGiB: 1, MaxStorageGiB: 10, VolumeMBps: 0, MaxPartitions: 100},
}

var controllerTypes = map[engine.Tier]ControllerType{
	engine.TierShared:    {Name: "c.small", CPURequestMilli: 500, CPULimitMilli: 1000, MemoryMiB: 2048, HeapMiB: 1024, StorageGiB: 20},
	engine.TierDedicated: {Name: "c.standard", CPURequestMilli: 1000, CPULimitMilli: 1000, MemoryMiB: 4096, HeapMiB: 2048, StorageGiB: 20},
}

var devController = ControllerType{Name: "c.dev", CPURequestMilli: 100, CPULimitMilli: 500, MemoryMiB: 512, HeapMiB: 256, StorageGiB: 1}

// SupportedVersions are the Kafka versions offered on Strimzi 1.3.0 (see
// third_party/strimzi/1.3.0/060-Deployment-strimzi-cluster-operator.yaml).
// 4.2.x is supported by Strimzi 1.3 but is dropped in Strimzi 1.4, so it is not offered.
var SupportedVersions = []string{"4.3.0", "4.3.1"}

// LookupBrokerType returns a broker type by name.
func LookupBrokerType(name string) (BrokerType, bool) {
	bt, ok := brokerTypes[name]
	return bt, ok
}

func controllerFor(bt BrokerType) ControllerType {
	if bt.Name == "dev.tiny" || bt.Name == "dev.ded" {
		return devController
	}
	return controllerTypes[bt.Tier]
}

func isDev(bt BrokerType) bool { return bt.Name == "dev.tiny" || bt.Name == "dev.ded" }

func cruiseControlResources(bt BrokerType) engine.Resources {
	if isDev(bt) {
		return engine.Resources{CPURequestMilli: 100, CPULimitMilli: 500, MemoryMiB: 768}
	}
	return engine.Resources{CPURequestMilli: 250, CPULimitMilli: 1000, MemoryMiB: 1536}
}

func entityOperatorResources(bt BrokerType) engine.Resources {
	if isDev(bt) {
		return engine.Resources{CPURequestMilli: 50, CPULimitMilli: 500, MemoryMiB: 384}
	}
	return engine.Resources{CPURequestMilli: 100, CPULimitMilli: 500, MemoryMiB: 384}
}

// clusterServices is the summed demand of Cruise Control and the Entity Operator.
func clusterServices(bt BrokerType) engine.Resources {
	cc, eo := cruiseControlResources(bt), entityOperatorResources(bt)
	return engine.Resources{CPURequestMilli: cc.CPURequestMilli + eo.CPURequestMilli,
		CPULimitMilli: cc.CPULimitMilli + eo.CPULimitMilli, MemoryMiB: cc.MemoryMiB + eo.MemoryMiB}
}
