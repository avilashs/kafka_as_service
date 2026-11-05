package kafka

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

func spec(t *testing.T, tier engine.Tier, s Spec) engine.ResourceSpec {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return engine.ResourceSpec{ID: "kc-0123456789", Name: "orders", Tenant: "tenant-a", Engine: Name, Tier: tier, Spec: raw}
}

func okSpec() Spec {
	return Spec{KafkaVersion: "4.3.1", BrokerType: "s.small", BrokerCount: 3, Storage: StorageSpec{SizeGiB: 100, MaxSizeGiB: 500, AutoExpand: true}}
}

var env = engine.CellEnv{CellID: "cell-1", Domain: "streams.example.net", Zones: []string{"a", "b", "c"},
	StorageClass: "gp3", GatewayName: "kafka-gateway", GatewayNS: "streamcell-gateway", GatewayDataPlaneNS: "envoy-gateway-system",
	EngineImages: map[string]string{"4.3.0": "registry.example/kafka:4.3.0", "4.3.1": "registry.example/kafka:4.3.1"}, APIServerCIDRs: []string{"10.0.0.1/32"}}

func TestValidate(t *testing.T) {
	e := New()
	cases := []struct {
		name    string
		tier    engine.Tier
		mut     func(*Spec)
		wantErr string
	}{
		{"ok", engine.TierShared, func(*Spec) {}, ""},
		{"bad version", engine.TierShared, func(s *Spec) { s.KafkaVersion = "3.9.0" }, "not supported"},
		{"unknown type", engine.TierShared, func(s *Spec) { s.BrokerType = "m5.large" }, "unknown brokerType"},
		{"tier mismatch", engine.TierShared, func(s *Spec) { s.BrokerType = "d.xlarge"; s.Storage.SizeGiB = 200 }, "belongs to tier"},
		{"count not multiple of 3", engine.TierShared, func(s *Spec) { s.BrokerCount = 4 }, "multiple of 3"},
		{"too small storage", engine.TierShared, func(s *Spec) { s.Storage.SizeGiB = 1 }, "storage.sizeGiB"},
		{"max below size", engine.TierShared, func(s *Spec) { s.Storage.MaxSizeGiB = 50 }, "maxSizeGiB"},
		{"forbidden config", engine.TierShared, func(s *Spec) { s.Config = map[string]string{"listeners": "PLAINTEXT://:9092"} }, "not customer-settable"},
		{"plugin config", engine.TierShared, func(s *Spec) { s.Config = map[string]string{"authorizer.class.name": "x"} }, "not customer-settable"},
		{"out of bounds", engine.TierShared, func(s *Spec) { s.Config = map[string]string{"message.max.bytes": "999999999"} }, "must be an integer"},
		{"minisr >= rf", engine.TierShared, func(s *Spec) {
			s.Config = map[string]string{"min.insync.replicas": "2", "default.replication.factor": "2"}
		}, "must be lower"},
		{"allowed config", engine.TierShared, func(s *Spec) { s.Config = map[string]string{"log.retention.hours": "72", "compression.type": "zstd"} }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := okSpec()
			tc.mut(&s)
			err := e.Validate(spec(t, tc.tier, s), nil)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
	t.Run("unknown spec field", func(t *testing.T) {
		rs := spec(t, engine.TierShared, okSpec())
		rs.Spec = json.RawMessage(`{"kafkaVersion":"4.3.1","brokerType":"s.small","brokerCount":3,"storage":{"sizeGiB":100},"listeners":[]}`)
		if err := e.Validate(rs, nil); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("want unknown field error, got %v", err)
		}
	})
	t.Run("no shrink", func(t *testing.T) {
		prior := spec(t, engine.TierShared, okSpec())
		s := okSpec()
		s.Storage.SizeGiB = 50
		if err := e.Validate(spec(t, engine.TierShared, s), &prior); err == nil || !strings.Contains(err.Error(), "cannot shrink") {
			t.Fatalf("want shrink error, got %v", err)
		}
	})
}

func render(t *testing.T, tier engine.Tier, s Spec) *engine.Bundle {
	t.Helper()
	b, err := New().Render(spec(t, tier, s), engine.Assignment{CellID: "cell-1", Zones: env.Zones,
		GroupZones: map[string][]string{"controllers": env.Zones, "brokers": env.Zones}, OperatorShard: 2}, env)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func find(b *engine.Bundle, kind, name string) *unstructured.Unstructured {
	for _, o := range b.Objects {
		if o.GetKind() == kind && o.GetName() == name {
			return o
		}
	}
	return nil
}

func TestRenderDeterministic(t *testing.T) {
	a, _ := json.Marshal(render(t, engine.TierShared, okSpec()))
	for i := 0; i < 20; i++ {
		b, _ := json.Marshal(render(t, engine.TierShared, okSpec()))
		if string(a) != string(b) {
			t.Fatal("render is not deterministic")
		}
	}
}

func TestRenderObjectsAndLabels(t *testing.T) {
	b := render(t, engine.TierShared, okSpec())
	want := []string{"Namespace/kc-0123456789", "NetworkPolicy/default-deny", "NetworkPolicy/allow-same-namespace",
		"NetworkPolicy/allow-dns-egress", "NetworkPolicy/allow-gateway-ingress", "NetworkPolicy/allow-apiserver-egress",
		"Kafka/kc-0123456789", "KafkaNodePool/controllers", "KafkaNodePool/brokers"}
	var got []string
	for _, o := range b.Objects {
		got = append(got, o.GetKind()+"/"+o.GetName())
		if o.GetLabels()[LabelInventory] != "kc-0123456789" || o.GetLabels()[LabelTenant] != "tenant-a" {
			t.Errorf("%s/%s missing inventory/tenant labels: %v", o.GetKind(), o.GetName(), o.GetLabels())
		}
		if o.GetKind() != "Namespace" && o.GetNamespace() != "kc-0123456789" {
			t.Errorf("%s/%s in namespace %q", o.GetKind(), o.GetName(), o.GetNamespace())
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("objects:\n got %v\nwant %v", got, want)
	}
	k := find(b, "Kafka", "kc-0123456789")
	if v, _, _ := unstructured.NestedString(k.Object, "metadata", "labels", LabelShard); v != "2" {
		t.Errorf("operator shard label = %q, want 2", v)
	}
	if v, _, _ := unstructured.NestedInt64(k.Object, "spec", "kafka", "template", "podDisruptionBudget", "maxUnavailable"); v != 0 {
		t.Errorf("PDB maxUnavailable = %d, want 0 (Drain Cleaner)", v)
	}
	cfg, _, _ := unstructured.NestedMap(k.Object, "spec", "kafka", "config")
	if cfg["min.insync.replicas"] != int64(2) || cfg["default.replication.factor"] != int64(3) {
		t.Errorf("replication defaults wrong: %v", cfg)
	}
	if b.Bounds["storage.brokers.maxGiB"] != 500 {
		t.Errorf("bounds = %v", b.Bounds)
	}
}

func podTemplate(t *testing.T, b *engine.Bundle, pool string) map[string]any {
	t.Helper()
	np := find(b, "KafkaNodePool", pool)
	if np == nil {
		t.Fatalf("no node pool %s", pool)
	}
	p, _, _ := unstructured.NestedMap(np.Object, "spec", "template", "pod")
	return p
}

func antiAffinityTerm(t *testing.T, pod map[string]any) map[string]any {
	t.Helper()
	terms, _, _ := unstructured.NestedSlice(pod, "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution")
	if len(terms) != 1 {
		t.Fatalf("want 1 required anti-affinity term, got %d", len(terms))
	}
	return terms[0].(map[string]any)
}

func TestRenderSharedPlacement(t *testing.T) {
	b := render(t, engine.TierShared, okSpec())
	pod := podTemplate(t, b, "brokers")
	term := antiAffinityTerm(t, pod)
	if term["topologyKey"] != "kubernetes.io/hostname" {
		t.Errorf("topologyKey = %v", term["topologyKey"])
	}
	sel, _, _ := unstructured.NestedStringMap(term, "labelSelector", "matchLabels")
	if sel[LabelCluster] != "kc-0123456789" || sel[LabelRole] != RoleBroker {
		t.Errorf("shared anti-affinity must target this cluster's brokers, got %v", sel)
	}
	if _, ok := term["namespaceSelector"]; !ok {
		t.Errorf("anti-affinity must set namespaceSelector")
	}
	if _, ok := pod["tolerations"]; ok {
		t.Errorf("shared brokers must not tolerate any taint: %v", pod["tolerations"])
	}
	vals, _, _ := unstructured.NestedSlice(pod, "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
	expr := vals[0].(map[string]any)["matchExpressions"].([]any)[0].(map[string]any)
	if expr["key"] != LabelPool || expr["values"].([]any)[0] != "shared" {
		t.Errorf("shared brokers must select the shared pool, got %v", expr)
	}
	tsc, _, _ := unstructured.NestedSlice(pod, "topologySpreadConstraints")
	if len(tsc) != 1 || tsc[0].(map[string]any)["topologyKey"] != "topology.kubernetes.io/zone" || tsc[0].(map[string]any)["whenUnsatisfiable"] != "DoNotSchedule" {
		t.Errorf("zone spread wrong: %v", tsc)
	}
}

func TestRenderDedicatedPlacement(t *testing.T) {
	s := okSpec()
	s.BrokerType, s.Storage = "d.xlarge", StorageSpec{SizeGiB: 1000}
	b := render(t, engine.TierDedicated, s)
	pod := podTemplate(t, b, "brokers")
	term := antiAffinityTerm(t, pod)
	sel, _, _ := unstructured.NestedStringMap(term, "labelSelector", "matchLabels")
	if len(sel) != 1 || sel[LabelRole] != RoleBroker {
		t.Errorf("dedicated anti-affinity must match ALL brokers, got %v", sel)
	}
	if ns, ok := term["namespaceSelector"].(map[string]any); !ok || len(ns) != 0 {
		t.Errorf("dedicated anti-affinity needs namespaceSelector: {}, got %v", term["namespaceSelector"])
	}
	tol, _, _ := unstructured.NestedSlice(pod, "tolerations")
	if len(tol) != 1 || tol[0].(map[string]any)["key"] != TaintDedicated || tol[0].(map[string]any)["value"] != "kc-0123456789" {
		t.Errorf("dedicated toleration must be for own taint only: %v", tol)
	}
}

func TestRenderPlacementLabelsAndNodePool(t *testing.T) {
	// Shared: pods carry the engine-neutral placement labels, and no node pool is requested.
	sb := render(t, engine.TierShared, okSpec())
	if len(sb.NodePools) != 0 {
		t.Fatalf("shared tier must not request node pools: %+v", sb.NodePools)
	}
	want := map[string]map[string]string{
		"brokers":     {"streamcell.dev/required-pool": "shared", "streamcell.dev/exclusive": "per-resource", "streamcell.dev/role": RoleBroker},
		"controllers": {"streamcell.dev/required-pool": "control", "streamcell.dev/exclusive": "per-resource", "streamcell.dev/role": RoleController},
	}
	for pool, labels := range want {
		got, _, _ := unstructured.NestedStringMap(podTemplate(t, sb, pool), "metadata", "labels")
		for k, v := range labels {
			if got[k] != v {
				t.Errorf("shared %s pod label %s = %q, want %q", pool, k, got[k], v)
			}
		}
	}
	if sb.DeleteGraceSeconds != 20 {
		t.Errorf("DeleteGraceSeconds = %d", sb.DeleteGraceSeconds)
	}

	// Dedicated: one node per broker, labelled and tainted for this cluster alone.
	s := okSpec()
	s.BrokerType, s.BrokerCount, s.Storage = "d.xlarge", 6, StorageSpec{SizeGiB: 1000}
	db := render(t, engine.TierDedicated, s)
	if len(db.NodePools) != 1 {
		t.Fatalf("want 1 node pool, got %+v", db.NodePools)
	}
	np := db.NodePools[0]
	if np.ID != "ded-kc-0123456789" || np.Owner != "kc-0123456789" || np.InstanceType != "m7i.xlarge" || np.TotalNodes() != 6 {
		t.Errorf("node pool = %+v", np)
	}
	for _, z := range env.Zones {
		if np.NodesPerZone[z] != 2 {
			t.Errorf("zone %s has %d nodes, want 2", z, np.NodesPerZone[z])
		}
	}
	if np.Labels["streamcell.dev/dedicated-owner"] != "kc-0123456789" || np.Labels["streamcell.dev/pool"] != "dedicated" {
		t.Errorf("node labels = %v", np.Labels)
	}
	if len(np.Taints) != 1 || np.Taints[0].Key != "streamcell.dev/dedicated" || np.Taints[0].Value != "kc-0123456789" || np.Taints[0].Effect != "NoSchedule" {
		t.Errorf("taints = %+v", np.Taints)
	}
	got, _, _ := unstructured.NestedStringMap(podTemplate(t, db, "brokers"), "metadata", "labels")
	if got["streamcell.dev/required-pool"] != "dedicated" || got["streamcell.dev/exclusive"] != "whole-node" {
		t.Errorf("dedicated broker labels = %v", got)
	}
}

func TestPlanPlacementDedicated(t *testing.T) {
	s := okSpec()
	s.BrokerType, s.BrokerCount, s.Storage = "d.2xlarge", 3, StorageSpec{SizeGiB: 500}
	req, err := New().PlanPlacement(spec(t, engine.TierDedicated, s))
	if err != nil {
		t.Fatal(err)
	}
	b := req.Groups[len(req.Groups)-1]
	if b.Name != "brokers" || b.Pool != engine.PoolDedicated || b.Exclusive != engine.ExclusiveWholeNode || b.InstanceType != "m7i.2xlarge" || b.Replicas != 3 {
		t.Fatalf("dedicated broker group = %+v", b)
	}
	if b.PerPod.CPURequestMilli != b.PerPod.CPULimitMilli {
		t.Errorf("dedicated pods must be Guaranteed: %+v", b.PerPod)
	}
}

// Every pod the engine can create must carry the placement labels the admission policy requires.
// (Found by the e2e admission suite: Cruise Control and the Entity Operator were missing one.)
func TestEveryRenderedPodTemplateHasPlacementLabels(t *testing.T) {
	for _, tier := range []engine.Tier{engine.TierShared, engine.TierDedicated} {
		sp := okSpec()
		if tier == engine.TierDedicated {
			sp.BrokerType, sp.Storage = "d.xlarge", StorageSpec{SizeGiB: 1000}
		}
		b := render(t, tier, sp)
		k := find(b, "Kafka", "kc-0123456789")
		var templates []map[string]any
		for _, path := range [][]string{{"spec", "cruiseControl", "template", "pod"}, {"spec", "entityOperator", "template", "pod"}} {
			m, _, _ := unstructured.NestedMap(k.Object, path...)
			templates = append(templates, m)
		}
		templates = append(templates, podTemplate(t, b, "brokers"), podTemplate(t, b, "controllers"))
		for i, tmpl := range templates {
			l, _, _ := unstructured.NestedStringMap(tmpl, "metadata", "labels")
			for _, key := range []string{"streamcell.dev/cluster", "streamcell.dev/role", "streamcell.dev/required-pool", "streamcell.dev/tenant", "streamcell.dev/tier"} {
				if l[key] == "" {
					t.Errorf("%s template %d is missing label %s: %v", tier, i, key, l)
				}
			}
			if l["streamcell.dev/cluster"] != "kc-0123456789" {
				t.Errorf("%s template %d cluster label = %q", tier, i, l["streamcell.dev/cluster"])
			}
		}
	}
}

// Every operand pod gets the Restricted profile from the platform, because Strimzi sets none (spike S-4).
func TestRenderedPodsAreHardened(t *testing.T) {
	b := render(t, engine.TierShared, okSpec())
	k := find(b, "Kafka", "kc-0123456789")
	type spot struct {
		what string
		obj  *unstructured.Unstructured
		path []string // to the template map
		ctrs []string // container templates that must carry the context
	}
	for _, sp := range []spot{
		{"brokers", find(b, "KafkaNodePool", "brokers"), []string{"spec", "template"}, []string{"kafkaContainer", "initContainer"}},
		{"controllers", find(b, "KafkaNodePool", "controllers"), []string{"spec", "template"}, []string{"kafkaContainer", "initContainer"}},
		{"cruise-control", k, []string{"spec", "cruiseControl", "template"}, []string{"cruiseControlContainer"}},
		{"entity-operator", k, []string{"spec", "entityOperator", "template"}, []string{"userOperatorContainer"}},
	} {
		tmpl, _, _ := unstructured.NestedMap(sp.obj.Object, sp.path...)
		psc, _, _ := unstructured.NestedMap(tmpl, "pod", "securityContext")
		if psc["runAsNonRoot"] != true {
			t.Errorf("%s: pod runAsNonRoot = %v", sp.what, psc["runAsNonRoot"])
		}
		if sc, _ := psc["seccompProfile"].(map[string]any); sc["type"] != "RuntimeDefault" {
			t.Errorf("%s: seccompProfile = %v", sp.what, psc["seccompProfile"])
		}
		for _, c := range sp.ctrs {
			csc, _, _ := unstructured.NestedMap(tmpl, c, "securityContext")
			if csc["allowPrivilegeEscalation"] != false {
				t.Errorf("%s/%s: allowPrivilegeEscalation = %v", sp.what, c, csc["allowPrivilegeEscalation"])
			}
			drop, _, _ := unstructured.NestedStringSlice(csc, "capabilities", "drop")
			if len(drop) != 1 || drop[0] != "ALL" {
				t.Errorf("%s/%s: capabilities.drop = %v", sp.what, c, drop)
			}
		}
	}
}

func TestRenderControllersOnControlPool(t *testing.T) {
	b := render(t, engine.TierShared, okSpec())
	pod := podTemplate(t, b, "controllers")
	tol, _, _ := unstructured.NestedSlice(pod, "tolerations")
	if len(tol) != 1 || tol[0].(map[string]any)["key"] != TaintControl {
		t.Errorf("controllers must tolerate only the control taint: %v", tol)
	}
	sel, _, _ := unstructured.NestedStringMap(antiAffinityTerm(t, pod), "labelSelector", "matchLabels")
	if sel[LabelRole] != RoleController || sel[LabelCluster] != "kc-0123456789" {
		t.Errorf("controller anti-affinity wrong: %v", sel)
	}
	np := find(b, "KafkaNodePool", "controllers")
	roles, _, _ := unstructured.NestedStringSlice(np.Object, "spec", "roles")
	if len(roles) != 1 || roles[0] != "controller" {
		t.Errorf("controller pool roles = %v", roles)
	}
}

func TestPlanPlacement(t *testing.T) {
	req, err := New().PlanPlacement(spec(t, engine.TierShared, okSpec()))
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Groups) != 3 || req.Groups[2].Replicas != 3 || req.Groups[2].Pool != engine.PoolShared ||
		req.Groups[0].Pool != engine.PoolControl || req.Groups[1].Pool != engine.PoolControl {
		t.Fatalf("unexpected placement request: %+v", req)
	}
	if req.Groups[2].PerPod.MemoryMiB != 6144 || req.Groups[2].PerPod.CPURequestMilli != 1000 {
		t.Fatalf("broker demand wrong: %+v", req.Groups[2].PerPod)
	}
	if req.Groups[1].PerPod.MemoryMiB != 1536+384 {
		t.Fatalf("cluster-services demand wrong: %+v", req.Groups[1].PerPod)
	}
}

func TestObserveStatus(t *testing.T) {
	e := New()
	rs := spec(t, engine.TierShared, okSpec())
	ready := json.RawMessage(`{"observedGeneration":3,"conditions":[{"type":"Ready","status":"True"}],"listeners":[{"name":"external","bootstrapServers":"bootstrap.x:443"}]}`)
	obs := engine.Observed{Generation: 1, Objects: []engine.ObjectStatus{
		{Kind: "Namespace", Name: "kc-0123456789", Exists: true},
		{Kind: "Kafka", Name: "kc-0123456789", Exists: true, Generation: 3, Status: ready},
	}}
	st, _ := e.ObserveStatus(rs, obs)
	if !st.Ready || st.Values["endpoint.scram-sha-512"] != "bootstrap.x:443" {
		t.Fatalf("want ready with endpoint, got %+v", st)
	}
	obs.Objects[1].Generation = 4 // operator has not caught up
	if st, _ := e.ObserveStatus(rs, obs); st.Ready {
		t.Fatal("must not be ready when observedGeneration < generation")
	}
	obs.Objects[1].Generation = 3
	obs.Objects[0].Exists = false
	if st, _ := e.ObserveStatus(rs, obs); st.Ready {
		t.Fatal("must not be ready when an object is missing")
	}
	if st, _ := e.ObserveStatus(rs, engine.Observed{Deleted: true}); !st.Deleted {
		t.Fatal("deleted not reported")
	}
}

// Every listener port is reachable only by whom it is meant for. Strimzi adds its own NetworkPolicy per
// listener and opens the port to everyone unless networkPolicyPeers is set, so both listeners must set it.
func TestListenersRestrictNetworkPeers(t *testing.T) {
	b := render(t, engine.TierShared, okSpec())
	k := find(b, "Kafka", "kc-0123456789")
	if k == nil {
		t.Fatal("no Kafka")
	}
	ls, _, _ := unstructured.NestedSlice(k.Object, "spec", "kafka", "listeners")
	if len(ls) != 3 {
		t.Fatalf("listeners = %d", len(ls))
	}
	for _, l := range ls {
		m := l.(map[string]any)
		peers, _ := m["networkPolicyPeers"].([]any)
		if len(peers) != 1 {
			t.Fatalf("listener %v has no networkPolicyPeers: it would be open to every pod", m["name"])
		}
		p := peers[0].(map[string]any)
		switch m["name"] {
		case "external", "mtls":
			ns := p["namespaceSelector"].(map[string]any)["matchLabels"].(map[string]any)["kubernetes.io/metadata.name"]
			if ns != "envoy-gateway-system" {
				t.Errorf("external listener peer namespace = %v, want the gateway data-plane namespace", ns)
			}
			if _, ok := p["podSelector"].(map[string]any)["matchLabels"].(map[string]any)["gateway.envoyproxy.io/owning-gateway-name"]; !ok {
				t.Error("external listener peer does not select the gateway's proxy pods")
			}
		case "tls":
			if _, ok := p["namespaceSelector"]; ok {
				t.Error("internal listener must not accept other namespaces")
			}
		}
	}
	np := find(b, "NetworkPolicy", "allow-gateway-ingress")
	from, _, _ := unstructured.NestedSlice(np.Object, "spec", "ingress")
	if len(from) != 1 {
		t.Fatal("allow-gateway-ingress malformed")
	}
	if got := from[0].(map[string]any)["from"].([]any)[0].(map[string]any)["namespaceSelector"].(map[string]any)["matchLabels"].(map[string]any)["kubernetes.io/metadata.name"]; got != "envoy-gateway-system" {
		t.Errorf("allow-gateway-ingress namespace = %v", got)
	}
}

// The authorizer and the policies come from the derived image: authorization is `custom` with the
// platform class and the User Operator may manage ACLs; rendering without that image is refused.
func TestAuthorizationAndPluginImage(t *testing.T) {
	b := render(t, engine.TierShared, okSpec())
	k := find(b, "Kafka", "kc-0123456789")
	az, _, _ := unstructured.NestedMap(k.Object, "spec", "kafka", "authorization")
	if az["type"] != "custom" || az["authorizerClass"] != AuthorizerClass || az["supportsAdminApi"] != true {
		t.Errorf("authorization = %v", az)
	}
	supers, _ := az["superUsers"].([]any)
	if len(supers) != 2 {
		t.Errorf("superUsers = %v (the User Operator and Cruise Control only)", supers)
	}
	img, _, _ := unstructured.NestedString(k.Object, "spec", "kafka", "image")
	if img != "registry.example/kafka:4.3.1" {
		t.Errorf("image = %q", img)
	}
	cfg, _, _ := unstructured.NestedMap(k.Object, "spec", "kafka", "config")
	for _, key := range []string{"create.topic.policy.class.name", "alter.config.policy.class.name"} {
		if _, ok := cfg[key]; !ok {
			t.Errorf("platform config lacks %s", key)
		}
	}
	noImg := env
	noImg.EngineImages = nil
	_, err := New().Render(spec(t, engine.TierShared, okSpec()), engine.Assignment{CellID: "cell-1", Zones: env.Zones,
		GroupZones: map[string][]string{"controllers": env.Zones, "brokers": env.Zones}, OperatorShard: 2}, noImg)
	if err == nil || !strings.Contains(err.Error(), "no platform Kafka image") {
		t.Errorf("render without an image: %v", err)
	}
}

func TestObserveStatusLifecycleGates(t *testing.T) {
	e := New()
	rs := spec(t, engine.TierShared, okSpec()) // 3 brokers
	mk := func(autoRebalance string, replicas int) engine.Observed {
		kst := `{"observedGeneration":3,"conditions":[{"type":"Ready","status":"True"}]` + autoRebalance + `}`
		pst := fmt.Sprintf(`{"replicas":%d,"observedGeneration":2}`, replicas)
		return engine.Observed{Generation: 1, Objects: []engine.ObjectStatus{
			{Kind: "Kafka", Name: "kc-0123456789", Exists: true, Generation: 3, Status: json.RawMessage(kst)},
			{Kind: "KafkaNodePool", Name: "controllers", Exists: true, Generation: 1, Status: json.RawMessage(`{"replicas":3,"observedGeneration":1}`)},
			{Kind: "KafkaNodePool", Name: "brokers", Exists: true, Generation: 2, Status: json.RawMessage(pst)},
		}}
	}
	cases := []struct {
		name  string
		obs   engine.Observed
		ready bool
	}{
		{"steady", mk(``, 3), true},
		{"auto-rebalance idle", mk(`,"autoRebalance":{"state":"Idle"}`, 3), true},
		{"rebalancing after scale up", mk(`,"autoRebalance":{"state":"RebalanceOnScaleUp"}`, 3), false},
		{"rebalancing before scale down", mk(`,"autoRebalance":{"state":"RebalanceOnScaleDown"}`, 3), false},
		{"brokers not all there yet", mk(``, 2), false},
		{"brokers not removed yet", mk(``, 6), false},
		{"node pool change not yet seen by the operator (a resize)", func() engine.Observed {
			o := mk(``, 3)
			o.Objects[2].Generation = 3 // the pool was changed; its status still says generation 2
			return o
		}(), false},
	}
	for _, tc := range cases {
		st, err := e.ObserveStatus(rs, tc.obs)
		if err != nil || st.Ready != tc.ready {
			t.Errorf("%s: ready=%v (%s), want %v", tc.name, st.Ready, st.Reason, tc.ready)
		}
	}
}

func TestClassifyChange(t *testing.T) {
	e := New()
	base := okSpec()
	mk := func(f func(*Spec)) engine.ResourceSpec {
		s := base
		s.Config = map[string]string{}
		f(&s)
		return spec(t, engine.TierShared, s)
	}
	from := mk(func(*Spec) {})
	ok := []struct {
		name string
		f    func(*Spec)
		kind string
		cap  bool
	}{
		{"config", func(s *Spec) { s.Config = map[string]string{"log.retention.hours": "48"} }, "config", false},
		{"storage", func(s *Spec) { s.Storage.SizeGiB += 10 }, "storage", false},
		{"autoexpand bounds", func(s *Spec) { s.Storage.AutoExpand = !s.Storage.AutoExpand }, "storage", false},
		{"size", func(s *Spec) { s.BrokerType = "s.medium" }, "size", true},
		{"scale out", func(s *Spec) { s.BrokerCount = 6 }, "scale", true},
		{"version", func(s *Spec) { s.KafkaVersion = "4.3.1" }, "version", false},
	}
	for _, tc := range ok {
		f2 := from
		if tc.kind == "version" {
			f2 = mk(func(s *Spec) { s.KafkaVersion = "4.3.0" })
		}
		to := mk(func(s *Spec) {
			if tc.kind == "version" {
				s.KafkaVersion = "4.3.0"
			}
			tc.f(s)
		})
		c, err := e.ClassifyChange(f2, to)
		if err != nil || c.Kind != tc.kind || c.NeedsCapacity != tc.cap {
			t.Errorf("%s: %+v %v", tc.name, c, err)
		}
	}
	bad := []struct {
		name string
		f    func(*Spec)
	}{
		{"nothing", func(*Spec) {}},
		{"two kinds", func(s *Spec) { s.BrokerCount = 6; s.BrokerType = "s.medium" }},
		{"shrink", func(s *Spec) { s.Storage.SizeGiB-- }},
		{"downgrade", func(s *Spec) { s.KafkaVersion = "4.3.0" }},
	}
	for _, tc := range bad {
		if c, err := e.ClassifyChange(from, mk(tc.f)); err == nil {
			t.Errorf("%s accepted as %+v", tc.name, c)
		}
	}
	// users are not changed through a spec change
	to := mk(func(s *Spec) { s.Users = []User{{Name: "alice"}} })
	if _, err := e.ClassifyChange(from, to); err == nil {
		t.Error("a user change was accepted as a spec change")
	}
}
