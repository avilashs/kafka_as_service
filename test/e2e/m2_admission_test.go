//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

func renderPods(t *testing.T, tier engine.Tier, brokerType string, clusterID string) map[string]*corev1.Pod {
	t.Helper()
	spec := kafka.Spec{KafkaVersion: "4.3.1", BrokerType: brokerType, BrokerCount: 3, Storage: kafka.StorageSpec{SizeGiB: 100}}
	if tier == engine.TierDedicated {
		spec.Storage.SizeGiB = 1000
	}
	raw, _ := json.Marshal(spec)
	zones := []string{"zone-a", "zone-b", "zone-c"}
	b, err := kafka.New().Render(engine.ResourceSpec{ID: clusterID, Name: "x", Tenant: "t", Engine: "kafka", Tier: tier, Spec: raw},
		engine.Assignment{CellID: "c", Zones: zones, GroupZones: map[string][]string{"controllers": zones, "brokers": zones}},
		engine.CellEnv{CellID: "c", Domain: "d.example", GatewayName: "gw", GatewayNS: "gw-ns"})
	if err != nil {
		t.Fatal(err)
	}
	return podTemplates(t, b, clusterID)
}

// submit creates the pod as a dry run (no state is kept) and returns the apiserver's answer.
func submit(p *corev1.Pod) error {
	_, err := kube.CoreV1().Pods(p.Namespace).Create(context.Background(), p, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	return err
}

func antiTerm(p *corev1.Pod) *corev1.PodAffinityTerm {
	return &p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
}

func nodeExpr(p *corev1.Pod) *corev1.NodeSelectorRequirement {
	return &p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0]
}

type podCase struct {
	name string
	pod  string // brokers | controllers | cruise-control | entity-operator
	mut  func(p *corev1.Pod)
	want string // "" = must be admitted; otherwise the rule tag that must refuse it
}

// M2 acceptance: "the admission policy rejects deliberately bad pods". Every case runs against the
// real kube-apiserver evaluating the shipped deploy/admission/policies/*.yaml.
func TestAdmissionPolicyRejectsBadPods(t *testing.T) {
	cases := func(tier engine.Tier) []podCase {
		own := "kc-0123456789"
		common := []podCase{
			// --- what the engine really renders is admitted ---
			{"rendered broker is admitted", "brokers", func(*corev1.Pod) {}, ""},
			{"rendered controller is admitted", "controllers", func(*corev1.Pod) {}, ""},
			{"rendered cruise-control is admitted", "cruise-control", func(*corev1.Pod) {}, ""},
			{"rendered entity-operator is admitted", "entity-operator", func(*corev1.Pod) {}, ""},

			// --- labels ---
			{"no platform labels at all", "brokers", func(p *corev1.Pod) { p.Labels = nil }, "[labels]"},
			{"cluster label differs from the namespace", "brokers", func(p *corev1.Pod) { p.Labels["streamcell.dev/cluster"] = "kc-someoneelse" }, "[labels]"},
			{"unknown required-pool", "brokers", func(p *corev1.Pod) { p.Labels["streamcell.dev/required-pool"] = "everywhere" }, "[labels]"},

			// --- rule 1: no two brokers of one cluster on a node (anti-affinity must be present and correct) ---
			{"broker without any anti-affinity", "brokers", func(p *corev1.Pod) { p.Spec.Affinity.PodAntiAffinity = nil }, "[anti-affinity]"},
			{"anti-affinity without namespaceSelector (namespace-scoped by default)", "brokers", func(p *corev1.Pod) { antiTerm(p).NamespaceSelector = nil }, "[anti-affinity]"},
			{"anti-affinity with a restricting namespaceSelector", "brokers", func(p *corev1.Pod) {
				antiTerm(p).NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"x": "y"}}
			}, "[anti-affinity]"},
			{"anti-affinity on the zone instead of the hostname", "brokers", func(p *corev1.Pod) { antiTerm(p).TopologyKey = "topology.kubernetes.io/zone" }, "[anti-affinity]"},
			{"anti-affinity selecting an unrelated label", "brokers", func(p *corev1.Pod) {
				antiTerm(p).LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "nothing"}}
			}, "[anti-affinity]"},
			{"anti-affinity only preferred, not required", "brokers", func(p *corev1.Pod) {
				term := *antiTerm(p)
				p.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{Weight: 100, PodAffinityTerm: term}}}
			}, "[anti-affinity]"},
			{"controller without anti-affinity", "controllers", func(p *corev1.Pod) { p.Spec.Affinity.PodAntiAffinity = nil }, "[anti-affinity]"},

			// --- rule 2: zone spread ---
			{"broker without zone spread", "brokers", func(p *corev1.Pod) { p.Spec.TopologySpreadConstraints = nil }, "[zone-spread]"},
			{"zone spread with maxSkew 2", "brokers", func(p *corev1.Pod) { p.Spec.TopologySpreadConstraints[0].MaxSkew = 2 }, "[zone-spread]"},
			{"zone spread that only ScheduleAnyway", "brokers", func(p *corev1.Pod) {
				p.Spec.TopologySpreadConstraints[0].WhenUnsatisfiable = corev1.ScheduleAnyway
			}, "[zone-spread]"},
			{"spread over hostname instead of zone", "brokers", func(p *corev1.Pod) { p.Spec.TopologySpreadConstraints[0].TopologyKey = "kubernetes.io/hostname" }, "[zone-spread]"},

			// --- rules 3 and 4: pools, taints, tolerations ---
			{"no node affinity at all", "brokers", func(p *corev1.Pod) { p.Spec.Affinity.NodeAffinity = nil }, "[pool]"},
			{"controller pinned to the shared pool", "controllers", func(p *corev1.Pod) { nodeExpr(p).Values = []string{"shared"} }, "[pool]"},
			{"cruise-control without control-pool affinity", "cruise-control", func(p *corev1.Pod) { p.Spec.Affinity = nil }, "[pool]"},
			{"wildcard toleration (tolerates every taint)", "brokers", func(p *corev1.Pod) {
				p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Operator: corev1.TolerationOpExists})
			}, "[tolerations]"},
			{"tolerates another cluster's dedicated taint", "brokers", func(p *corev1.Pod) {
				p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "streamcell.dev/dedicated", Operator: corev1.TolerationOpEqual, Value: "kc-victim", Effect: corev1.TaintEffectNoSchedule})
			}, "[tolerations]"},
			{"tolerates any dedicated taint via Exists", "brokers", func(p *corev1.Pod) {
				p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "streamcell.dev/dedicated", Operator: corev1.TolerationOpExists})
			}, "[tolerations]"},
			{"tolerates an arbitrary taint", "brokers", func(p *corev1.Pod) {
				p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
			}, "[tolerations]"},
			{"cruise-control tolerates the dedicated taint of its own cluster", "cruise-control", func(p *corev1.Pod) {
				p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "streamcell.dev/dedicated", Operator: corev1.TolerationOpEqual, Value: own, Effect: corev1.TaintEffectNoSchedule})
			}, "[tolerations]"},

			// --- bypassing the scheduler or the sandbox ---
			{"pod sets nodeName directly", "brokers", func(p *corev1.Pod) { p.Spec.NodeName = "some-node" }, "[node-name]"},
			{"hostNetwork", "brokers", func(p *corev1.Pod) { p.Spec.HostNetwork = true }, "[host-access]"},
			{"hostPID", "brokers", func(p *corev1.Pod) { p.Spec.HostPID = true }, "[host-access]"},
			{"hostPath volume", "brokers", func(p *corev1.Pod) {
				p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "h", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}})
			}, "[host-access]"},
			{"privileged container", "brokers", func(p *corev1.Pod) {
				p.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: ptr.To(true)}
			}, "[privileged]"},
			{"privileged init container", "brokers", func(p *corev1.Pod) {
				p.Spec.InitContainers = []corev1.Container{{Name: "i", Image: "x", SecurityContext: &corev1.SecurityContext{Privileged: ptr.To(true)}}}
			}, "[privileged]"},
			{"hostPort", "brokers", func(p *corev1.Pod) {
				p.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 9092, HostPort: 9092}}
			}, "[privileged]"},
		}
		if tier == engine.TierShared {
			return append(common, []podCase{
				{"shared broker pinned to the dedicated pool", "brokers", func(p *corev1.Pod) {
					nodeExpr(p).Key, nodeExpr(p).Values = "streamcell.dev/dedicated-owner", []string{own}
				}, "[pool]"},
				{"shared broker pinned to the control pool", "brokers", func(p *corev1.Pod) { nodeExpr(p).Values = []string{"control"} }, "[pool]"},
				{"shared broker tolerates its own cluster's dedicated taint", "brokers", func(p *corev1.Pod) {
					p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "streamcell.dev/dedicated", Operator: corev1.TolerationOpEqual, Value: own, Effect: corev1.TaintEffectNoSchedule})
				}, "[tolerations]"},
				{"shared broker tolerates the control taint", "brokers", func(p *corev1.Pod) {
					p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "streamcell.dev/control", Operator: corev1.TolerationOpExists})
				}, "[tolerations]"},
				{"shared anti-affinity selecting another cluster's brokers", "brokers", func(p *corev1.Pod) {
					antiTerm(p).LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"streamcell.dev/cluster": "kc-other", "streamcell.dev/role": kafka.RoleBroker}}
				}, "[anti-affinity]"},
				{"shared anti-affinity widened to every broker is also refused (must be exactly this cluster)", "brokers", func(p *corev1.Pod) {
					antiTerm(p).LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"streamcell.dev/role": kafka.RoleBroker}}
				}, "[anti-affinity]"},
			}...)
		}
		return append(common, []podCase{
			{"dedicated broker only avoids its own cluster's brokers (shared-style term)", "brokers", func(p *corev1.Pod) {
				antiTerm(p).LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"streamcell.dev/cluster": own, "streamcell.dev/role": kafka.RoleBroker}}
			}, "[anti-affinity]"},
			{"dedicated broker pinned to another cluster's nodes", "brokers", func(p *corev1.Pod) { nodeExpr(p).Values = []string{"kc-victim"} }, "[pool]"},
			{"dedicated broker pinned to the shared pool", "brokers", func(p *corev1.Pod) {
				nodeExpr(p).Key, nodeExpr(p).Values = "streamcell.dev/pool", []string{"shared"}
			}, "[pool]"},
			{"dedicated broker tolerates another cluster's taint instead of its own", "brokers", func(p *corev1.Pod) {
				for i := range p.Spec.Tolerations {
					if p.Spec.Tolerations[i].Key == "streamcell.dev/dedicated" {
						p.Spec.Tolerations[i].Value = "kc-victim"
					}
				}
			}, "[tolerations]"},
			{"dedicated broker tolerates its own taint with Exists (matches any owner)", "brokers", func(p *corev1.Pod) {
				for i := range p.Spec.Tolerations {
					if p.Spec.Tolerations[i].Key == "streamcell.dev/dedicated" {
						p.Spec.Tolerations[i].Operator, p.Spec.Tolerations[i].Value = corev1.TolerationOpExists, ""
					}
				}
			}, "[tolerations]"},
		}...)
	}

	for _, tc := range []struct {
		tier   engine.Tier
		broker string
		id     string
	}{
		{engine.TierShared, "s.small", "kc-0123456789"},
		{engine.TierDedicated, "d.xlarge", "kc-0123456789"},
	} {
		t.Run(string(tc.tier), func(t *testing.T) {
			tenantNamespace(t, tc.id)
			base := renderPods(t, tc.tier, tc.broker, tc.id)
			for _, c := range cases(tc.tier) {
				t.Run(c.name, func(t *testing.T) {
					p := base[c.pod].DeepCopy()
					c.mut(p)
					err := submit(p)
					switch {
					case c.want == "" && err != nil:
						t.Fatalf("expected the pod to be admitted, got: %v", err)
					case c.want != "" && err == nil:
						t.Fatalf("expected rejection by %s, but the pod was admitted", c.want)
					case c.want != "" && !strings.Contains(err.Error(), "streamcell placement policy "+c.want):
						t.Fatalf("expected rejection by %s, got: %v", c.want, err)
					}
				})
			}
		})
	}
}

// Pods outside tenant namespaces are not in scope, so the policy cannot disturb the rest of the cluster.
func TestAdmissionPolicyIgnoresOtherNamespaces(t *testing.T) {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "default"},
		Spec: corev1.PodSpec{HostNetwork: true, Containers: []corev1.Container{{Name: "c", Image: "x"}}}}
	if err := submit(p); err != nil {
		t.Fatalf("pod in a non-tenant namespace was refused: %v", err)
	}
}

// Placement labels cannot be changed on a running pod.
func TestAdmissionPolicyPlacementLabelsAreImmutable(t *testing.T) {
	id := "kc-immutable01"
	tenantNamespace(t, id)
	p := renderPods(t, engine.TierShared, "s.small", id)["brokers"]
	created, err := kube.CoreV1().Pods(id).Create(context.Background(), p, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created.Labels["streamcell.dev/required-pool"] = "control"
	if _, err := kube.CoreV1().Pods(id).Update(context.Background(), created, metav1.UpdateOptions{}); err == nil ||
		!strings.Contains(err.Error(), "[immutable-labels]") {
		t.Fatalf("changing required-pool on a live pod: %v", err)
	}
	fresh, _ := kube.CoreV1().Pods(id).Get(context.Background(), p.Name, metav1.GetOptions{})
	fresh.Labels["unrelated"] = "ok"
	if _, err := kube.CoreV1().Pods(id).Update(context.Background(), fresh, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("an unrelated label change must be allowed: %v", err)
	}
}

// Only provisioners may change node pool labels/taints, and the namespace label that scopes the pod policy.
func TestNodeAndNamespaceProtection(t *testing.T) {
	ctx := context.Background()
	intruder := asUser(t, "someone-else")
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "prot-node-" + randHex(3), Labels: map[string]string{"streamcell.dev/pool": "spare"}}}
	if _, err := kube.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	mutateNode := func(c kubernetes.Interface, fn func(n *corev1.Node)) error {
		n, err := c.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		fn(n)
		_, err = c.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
		return err
	}
	expect := func(what string, err error, tag string) {
		t.Helper()
		switch {
		case tag == "" && err != nil:
			t.Fatalf("%s: expected success, got %v", what, err)
		case tag != "" && (err == nil || !strings.Contains(err.Error(), tag)):
			t.Fatalf("%s: expected %q, got %v", what, tag, err)
		}
	}
	expect("intruder claims a node for a dedicated owner", mutateNode(intruder, func(n *corev1.Node) {
		n.Labels["streamcell.dev/dedicated-owner"] = "kc-victim"
	}), "node protection [labels]")
	expect("intruder re-pools a node", mutateNode(intruder, func(n *corev1.Node) { n.Labels["streamcell.dev/pool"] = "shared" }), "node protection [labels]")
	expect("intruder removes the taint that keeps tenants off", mutateNode(kube, func(n *corev1.Node) {
		n.Spec.Taints = []corev1.Taint{{Key: "streamcell.dev/dedicated", Value: "kc-x", Effect: corev1.TaintEffectNoSchedule}}
	}), "") // the provisioner (this test process) may set it
	expect("intruder strips the dedicated taint", mutateNode(intruder, func(n *corev1.Node) { n.Spec.Taints = nil }), "node protection [taints]")
	expect("intruder adds a streamcell taint", mutateNode(intruder, func(n *corev1.Node) {
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: "streamcell.dev/control", Effect: corev1.TaintEffectNoSchedule})
	}), "node protection [taints]")
	expect("intruder changes an unrelated label and taint", mutateNode(intruder, func(n *corev1.Node) {
		n.Labels["team"] = "x"
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: "maintenance", Effect: corev1.TaintEffectNoSchedule})
	}), "")
	expect("the provisioner re-pools the node", mutateNode(kube, func(n *corev1.Node) { n.Labels["streamcell.dev/dedicated-owner"] = "kc-x" }), "")

	nsName := "prot-ns-" + randHex(3)
	tenantNamespace(t, nsName)
	mutateNS := func(c kubernetes.Interface, fn func(ns *corev1.Namespace)) error {
		ns, err := c.CoreV1().Namespaces().Get(ctx, nsName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		fn(ns)
		_, err = c.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
		return err
	}
	expect("intruder removes the tenant-namespace label (would exempt the namespace from the pod policy)",
		mutateNS(intruder, func(ns *corev1.Namespace) { delete(ns.Labels, "streamcell.dev/inventory") }), "namespace protection")
	expect("intruder labels an unrelated thing", mutateNS(intruder, func(ns *corev1.Namespace) { ns.Labels["x"] = "y" }), "")
	expect("the provisioner may remove it", mutateNS(kube, func(ns *corev1.Namespace) { delete(ns.Labels, "streamcell.dev/inventory") }), "")
}
