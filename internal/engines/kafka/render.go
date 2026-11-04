package kafka

import (
	"encoding/json"
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kjson "k8s.io/apimachinery/pkg/util/json"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// Label keys. The prefix is fixed for now; see docs/design.md (working name).
const (
	LabelPrefix     = "streamcell.dev/"
	LabelCluster    = LabelPrefix + "cluster"
	LabelTenant     = LabelPrefix + "tenant"
	LabelTier       = LabelPrefix + "tier"
	LabelRole       = LabelPrefix + "role"
	LabelPool       = LabelPrefix + "pool"
	LabelInventory  = LabelPrefix + "inventory"
	LabelShard      = LabelPrefix + "operator-shard"
	LabelDedOwner   = LabelPrefix + "dedicated-owner"
	TaintDedicated  = LabelPrefix + "dedicated"
	TaintControl    = LabelPrefix + "control"
	RoleBroker      = "kafka-broker"
	RoleController  = "kafka-controller"
	ExternalPort    = 9094
	InternalTLSPort = 9093
)

// Render implements engine.Engine.
func (e *Engine) Render(rs engine.ResourceSpec, a engine.Assignment, env engine.CellEnv) (*engine.Bundle, error) {
	s, err := ParseSpec(rs)
	if err != nil {
		return nil, err
	}
	bt, ok := LookupBrokerType(s.BrokerType)
	if !ok {
		return nil, engine.Invalid("unknown brokerType %q", s.BrokerType)
	}
	if env.Domain == "" || env.CellID == "" {
		return nil, fmt.Errorf("cell env incomplete: domain and cellId are required")
	}
	ct := controllerFor(bt)
	ccRes, eoRes := cruiseControlResources(bt), entityOperatorResources(bt)
	ns := rs.ID
	common := map[string]any{
		LabelCluster:   rs.ID,
		LabelTenant:    rs.Tenant,
		LabelTier:      string(rs.Tier),
		LabelInventory: rs.ID,
	}
	host := func(prefix string) string {
		return fmt.Sprintf("%s.%s.%s.%s", prefix, rs.ID, env.CellID, env.Domain)
	}

	objs := []map[string]any{
		namespace(ns, common),
		netpolDefaultDeny(ns, common),
		netpolSameNamespace(ns, common),
		netpolDNS(ns, common),
		netpolGatewayIngress(ns, common, env),
	}
	if len(env.APIServerCIDRs) > 0 {
		objs = append(objs, netpolAPIServer(ns, common, env.APIServerCIDRs))
	}

	kafka := map[string]any{
		"apiVersion": "kafka.strimzi.io/v1",
		"kind":       "Kafka",
		"metadata": map[string]any{
			"name":      rs.ID,
			"namespace": ns,
			"labels":    merge(common, map[string]any{LabelShard: strconv.Itoa(a.OperatorShard)}),
		},
		"spec": map[string]any{
			"kafka": map[string]any{
				"version": s.KafkaVersion,
				"listeners": []any{
					map[string]any{
						"name": "tls", "port": InternalTLSPort, "type": "internal", "tls": true,
						"authentication": map[string]any{"type": "tls"},
					},
					map[string]any{
						"name": "external", "port": ExternalPort, "type": "tlsroute", "tls": true,
						"authentication": map[string]any{"type": "scram-sha-512"},
						"configuration": map[string]any{
							"bootstrap":    map[string]any{"host": host("bootstrap")},
							"hostTemplate": host("b{nodeId}"),
							"parentRefs": []any{map[string]any{
								"group": "gateway.networking.k8s.io", "kind": "Gateway",
								"name": env.GatewayName, "namespace": env.GatewayNS,
							}},
						},
					},
				},
				"authorization": map[string]any{
					"type":       "simple",
					"superUsers": []any{"CN=" + rs.ID + "-entity-user-operator", "CN=" + rs.ID + "-cruise-control"},
				},
				"rack":   map[string]any{"topologyKey": "topology.kubernetes.io/zone"},
				"config": effectiveConfig(s.Config),
				"template": map[string]any{
					"podDisruptionBudget": map[string]any{"maxUnavailable": 0},
				},
			},
			"entityOperator": map[string]any{
				"userOperator": map[string]any{
					"resources": res(eoRes.CPURequestMilli, eoRes.CPULimitMilli, eoRes.MemoryMiB),
				},
				"template": map[string]any{"pod": controlPoolPod(rs.ID, "entity-operator")},
			},
			"cruiseControl": map[string]any{
				"resources": res(ccRes.CPURequestMilli, ccRes.CPULimitMilli, ccRes.MemoryMiB),
				"autoRebalance": []any{
					map[string]any{"mode": "add-brokers"},
					map[string]any{"mode": "remove-brokers"},
				},
				"template": map[string]any{"pod": controlPoolPod(rs.ID, "cruise-control")},
			},
		},
	}
	if img, ok := env.EngineImages[s.KafkaVersion]; ok && img != "" {
		kafka["spec"].(map[string]any)["kafka"].(map[string]any)["image"] = img
	}

	controllers := nodePool(rs.ID, "controllers", common, []any{"controller"}, 3,
		res(ct.CPURequestMilli, ct.CPULimitMilli, ct.MemoryMiB), ct.HeapMiB,
		pvc(ct.StorageGiB, env.StorageClass),
		controllerPod(rs.ID, common))

	brokerPod := sharedBrokerPod(rs.ID, common)
	if rs.Tier == engine.TierDedicated {
		brokerPod = dedicatedBrokerPod(rs.ID, common)
	}
	brokerStorage := pvc(s.Storage.SizeGiB, env.StorageClass)
	if bt.LocalNVMe {
		brokerStorage = pvc(s.Storage.SizeGiB, env.LocalStorageClass)
	}
	brokers := nodePool(rs.ID, "brokers", common, []any{"broker"}, s.BrokerCount,
		res(bt.CPURequestMilli, bt.CPULimitMilli, bt.MemoryMiB), bt.HeapMiB, brokerStorage, brokerPod)
	if bt.EgressMbit > 0 {
		meta := brokers["spec"].(map[string]any)["template"].(map[string]any)["pod"].(map[string]any)["metadata"].(map[string]any)
		meta["annotations"] = map[string]any{"kubernetes.io/egress-bandwidth": fmt.Sprintf("%dM", bt.EgressMbit)}
	}

	objs = append(objs, kafka, controllers, brokers)

	b := &engine.Bundle{}
	for _, o := range objs {
		u, err := toUnstructured(o)
		if err != nil {
			return nil, err
		}
		b.Objects = append(b.Objects, u)
	}
	if s.Storage.AutoExpand {
		max := s.Storage.MaxSizeGiB
		if max == 0 {
			max = bt.MaxStorageGiB
		}
		b.Bounds = map[string]int64{"storage.brokers.maxGiB": max}
	}
	return b, nil
}

func toUnstructured(m map[string]any) (*unstructured.Unstructured, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := kjson.Unmarshal(raw, &out); err != nil { // yields int64 for integers
		return nil, err
	}
	return &unstructured.Unstructured{Object: out}, nil
}

func merge(ms ...map[string]any) map[string]any {
	out := map[string]any{}
	for _, m := range ms {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

func res(reqMilli, limMilli, memMiB int64) map[string]any {
	return map[string]any{
		"requests": map[string]any{"cpu": fmt.Sprintf("%dm", reqMilli), "memory": fmt.Sprintf("%dMi", memMiB)},
		"limits":   map[string]any{"cpu": fmt.Sprintf("%dm", limMilli), "memory": fmt.Sprintf("%dMi", memMiB)},
	}
}

func pvc(sizeGiB int64, class string) map[string]any {
	p := map[string]any{"type": "persistent-claim", "size": fmt.Sprintf("%dGi", sizeGiB), "deleteClaim": false}
	if class != "" {
		p["class"] = class
	}
	return p
}

func nodePool(cluster, name string, common map[string]any, roles []any, replicas int, resources map[string]any, heapMiB int64, storage, pod map[string]any) map[string]any {
	heap := fmt.Sprintf("%dm", heapMiB)
	return map[string]any{
		"apiVersion": "kafka.strimzi.io/v1",
		"kind":       "KafkaNodePool",
		"metadata": map[string]any{
			"name":      name,
			"namespace": cluster,
			"labels":    merge(common, map[string]any{"strimzi.io/cluster": cluster}),
		},
		"spec": map[string]any{
			"replicas":   replicas,
			"roles":      roles,
			"resources":  resources,
			"jvmOptions": map[string]any{"-Xms": heap, "-Xmx": heap},
			"storage":    storage,
			"template":   map[string]any{"pod": pod},
		},
	}
}

func zoneSpread(selector map[string]any) []any {
	return []any{map[string]any{
		"maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone", "whenUnsatisfiable": "DoNotSchedule",
		"labelSelector": map[string]any{"matchLabels": selector},
	}}
}

func hostnameAntiAffinity(selector map[string]any, allNamespaces bool) map[string]any {
	term := map[string]any{
		"topologyKey":   "kubernetes.io/hostname",
		"labelSelector": map[string]any{"matchLabels": selector},
	}
	if allNamespaces {
		term["namespaceSelector"] = map[string]any{}
	}
	return map[string]any{"requiredDuringSchedulingIgnoredDuringExecution": []any{term}}
}

func requirePool(pool string) map[string]any {
	return requireLabel(LabelPool, pool)
}

func requireLabel(key, value string) map[string]any {
	return map[string]any{"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{
		"nodeSelectorTerms": []any{map[string]any{"matchExpressions": []any{
			map[string]any{"key": key, "operator": "In", "values": []any{value}},
		}}},
	}}
}

func controlToleration() []any {
	return []any{map[string]any{"key": TaintControl, "operator": "Exists", "effect": "NoSchedule"}}
}

func controllerPod(cluster string, common map[string]any) map[string]any {
	sel := map[string]any{LabelCluster: cluster, LabelRole: RoleController}
	return map[string]any{
		"metadata":                      map[string]any{"labels": merge(common, map[string]any{LabelRole: RoleController})},
		"terminationGracePeriodSeconds": 120,
		"affinity": map[string]any{
			"nodeAffinity":    requirePool("control"),
			"podAntiAffinity": hostnameAntiAffinity(sel, true),
		},
		"tolerations":               controlToleration(),
		"topologySpreadConstraints": zoneSpread(sel),
	}
}

func controlPoolPod(cluster, component string) map[string]any {
	return map[string]any{
		"metadata":    map[string]any{"labels": map[string]any{LabelCluster: cluster, LabelRole: component}},
		"affinity":    map[string]any{"nodeAffinity": requirePool("control")},
		"tolerations": controlToleration(),
	}
}

// sharedBrokerPod: many tenants per node, never two brokers of this cluster on one node.
func sharedBrokerPod(cluster string, common map[string]any) map[string]any {
	sel := map[string]any{LabelCluster: cluster, LabelRole: RoleBroker}
	return map[string]any{
		"metadata":                      map[string]any{"labels": merge(common, map[string]any{LabelRole: RoleBroker})},
		"terminationGracePeriodSeconds": 300,
		"affinity": map[string]any{
			"nodeAffinity":    requirePool("shared"),
			"podAntiAffinity": hostnameAntiAffinity(sel, true),
		},
		"topologySpreadConstraints": zoneSpread(sel),
	}
}

// dedicatedBrokerPod: own node pool, own taint, and no other Kafka broker of any tenant on the node.
func dedicatedBrokerPod(cluster string, common map[string]any) map[string]any {
	return map[string]any{
		"metadata":                      map[string]any{"labels": merge(common, map[string]any{LabelRole: RoleBroker})},
		"terminationGracePeriodSeconds": 300,
		"affinity": map[string]any{
			"nodeAffinity":    requireLabel(LabelDedOwner, cluster),
			"podAntiAffinity": hostnameAntiAffinity(map[string]any{LabelRole: RoleBroker}, true),
		},
		"tolerations":               []any{map[string]any{"key": TaintDedicated, "operator": "Equal", "value": cluster, "effect": "NoSchedule"}},
		"topologySpreadConstraints": zoneSpread(map[string]any{LabelCluster: cluster, LabelRole: RoleBroker}),
	}
}

func meta(kind, apiVersion, name, ns string, labels map[string]any) map[string]any {
	m := map[string]any{"name": name, "labels": labels}
	if ns != "" {
		m["namespace"] = ns
	}
	return map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": m}
}

func namespace(ns string, common map[string]any) map[string]any {
	o := meta("Namespace", "v1", ns, "", merge(common, map[string]any{
		// Enforce baseline now; restricted is warned/audited until spike S-4 confirms Strimzi pods comply.
		"pod-security.kubernetes.io/enforce": "baseline",
		"pod-security.kubernetes.io/warn":    "restricted",
		"pod-security.kubernetes.io/audit":   "restricted",
	}))
	return o
}

func netpol(name, ns string, common map[string]any, spec map[string]any) map[string]any {
	o := meta("NetworkPolicy", "networking.k8s.io/v1", name, ns, common)
	o["spec"] = spec
	return o
}

func netpolDefaultDeny(ns string, common map[string]any) map[string]any {
	return netpol("default-deny", ns, common, map[string]any{
		"podSelector": map[string]any{}, "policyTypes": []any{"Ingress", "Egress"},
	})
}

func netpolSameNamespace(ns string, common map[string]any) map[string]any {
	peer := []any{map[string]any{"podSelector": map[string]any{}}}
	return netpol("allow-same-namespace", ns, common, map[string]any{
		"podSelector": map[string]any{}, "policyTypes": []any{"Ingress", "Egress"},
		"ingress": []any{map[string]any{"from": peer}},
		"egress":  []any{map[string]any{"to": peer}},
	})
}

func netpolDNS(ns string, common map[string]any) map[string]any {
	return netpol("allow-dns-egress", ns, common, map[string]any{
		"podSelector": map[string]any{}, "policyTypes": []any{"Egress"},
		"egress": []any{map[string]any{
			"to": []any{map[string]any{
				"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": "kube-system"}},
				"podSelector":       map[string]any{"matchLabels": map[string]any{"k8s-app": "kube-dns"}},
			}},
			"ports": []any{
				map[string]any{"protocol": "UDP", "port": 53},
				map[string]any{"protocol": "TCP", "port": 53},
			},
		}},
	})
}

func netpolGatewayIngress(ns string, common map[string]any, env engine.CellEnv) map[string]any {
	return netpol("allow-gateway-ingress", ns, common, map[string]any{
		"podSelector": map[string]any{"matchLabels": map[string]any{LabelRole: RoleBroker}},
		"policyTypes": []any{"Ingress"},
		"ingress": []any{map[string]any{
			"from": []any{map[string]any{"namespaceSelector": map[string]any{
				"matchLabels": map[string]any{"kubernetes.io/metadata.name": env.GatewayNS},
			}}},
			"ports": []any{map[string]any{"protocol": "TCP", "port": ExternalPort}},
		}},
	})
}

func netpolAPIServer(ns string, common map[string]any, cidrs []string) map[string]any {
	to := []any{}
	for _, c := range cidrs {
		to = append(to, map[string]any{"ipBlock": map[string]any{"cidr": c}})
	}
	return netpol("allow-apiserver-egress", ns, common, map[string]any{
		"podSelector": map[string]any{}, "policyTypes": []any{"Egress"},
		"egress": []any{map[string]any{"to": to, "ports": []any{
			map[string]any{"protocol": "TCP", "port": 443},
			map[string]any{"protocol": "TCP", "port": 6443},
		}}},
	})
}
