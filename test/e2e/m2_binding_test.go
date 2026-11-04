//go:build e2e

package e2e

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/avilashs/kafka_as_service/internal/admission"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// guard is the real binding guard served over TLS and registered with the real
// apiserver for pods/binding in tenant namespaces, failurePolicy Fail.
type guard struct {
	srv  *httptest.Server
	name string
}

func startGuard(t *testing.T) *guard {
	t.Helper()
	certs, err := admission.Generate([]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certs.Cert, certs.Key)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer((&admission.Guard{Kube: kube, Log: logger}).Handler())
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	srv.StartTLS()

	g := &guard{srv: srv, name: "streamcell-binding-guard-" + randHex(3)}
	url := srv.URL + "/validate-binding"
	cfg := &admissionregv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: g.name},
		Webhooks: []admissionregv1.ValidatingWebhook{{
			Name:         "binding.streamcell.dev",
			ClientConfig: admissionregv1.WebhookClientConfig{URL: &url, CABundle: certs.CA},
			Rules: []admissionregv1.RuleWithOperations{{
				Operations: []admissionregv1.OperationType{admissionregv1.Create},
				Rule:       admissionregv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods/binding"}},
			}},
			NamespaceSelector:       &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "streamcell.dev/inventory", Operator: metav1.LabelSelectorOpExists}}},
			FailurePolicy:           ptr.To(admissionregv1.Fail),
			SideEffects:             ptr.To(admissionregv1.SideEffectClassNone),
			TimeoutSeconds:          ptr.To(int32(5)),
			AdmissionReviewVersions: []string{"v1"},
		}},
	}
	if _, err := kube.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(context.Background(), cfg, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = kube.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(context.Background(), g.name, metav1.DeleteOptions{})
		g.srv.Close()
	})
	return g
}

// killServer stops the webhook server but leaves its configuration registered: an outage.
func (g *guard) killServer() { g.srv.CloseClientConnections(); g.srv.Close() }

func newNode(t *testing.T, name, zone, pool, owner string) {
	t.Helper()
	l := map[string]string{"topology.kubernetes.io/zone": zone, "streamcell.dev/pool": pool}
	if owner != "" {
		l["streamcell.dev/dedicated-owner"] = owner
	}
	if _, err := kube.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: l}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func newPod(t *testing.T, tmpl *corev1.Pod, name string) *corev1.Pod {
	t.Helper()
	p := tmpl.DeepCopy()
	p.Name = name
	created, err := kube.CoreV1().Pods(p.Namespace).Create(context.Background(), p, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return created
}

func bind(p *corev1.Pod, node string) error {
	return kube.CoreV1().Pods(p.Namespace).Bind(context.Background(), &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace},
		Target:     corev1.ObjectReference{Kind: "Node", Name: node},
	}, metav1.CreateOptions{})
}

func mustRefuse(t *testing.T, what string, err error, rule string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: binding was allowed, want refusal by %s", what, rule)
	}
	if !strings.Contains(err.Error(), "streamcell placement guard") || !strings.Contains(err.Error(), "["+rule+"]") {
		t.Fatalf("%s: want refusal by %s, got: %v", what, rule, err)
	}
}

// waitGuardActive blocks until the apiserver is consulting the guard (it notices new
// webhook configurations asynchronously). It binds a throwaway pod to a node it must
// not be allowed on; the pod belongs to its own cluster so a premature success is harmless.
func waitGuardActive(t *testing.T, suffix string) {
	t.Helper()
	id := "kc-probe" + suffix
	tenantNamespace(t, id)
	node := "probe-ctrl-" + suffix
	newNode(t, node, "zone-a", "control", "")
	tmpl := renderPods(t, engine.TierShared, "s.small", id)["brokers"]
	deadline := time.Now().Add(30 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		p := newPod(t, tmpl, fmt.Sprintf("probe-%d", i))
		if err := bind(p, node); err != nil && strings.Contains(err.Error(), "streamcell placement guard") {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("binding guard did not become active")
}

// M2: "the admission policy rejects pods that would break these rules, even if the agent has a bug".
// The pods/binding guard covers what CEL cannot see: what already runs on the target node.
func TestBindingGuardRefusesViolatingBindings(t *testing.T) {
	startGuard(t)
	suffix := randHex(3)
	waitGuardActive(t, suffix)

	shared := []string{"bg-s-a-" + suffix, "bg-s-b-" + suffix, "bg-s-c-" + suffix}
	for i, n := range shared {
		newNode(t, n, "zone-"+string(rune('a'+i)), "shared", "")
	}
	idA, idB := "kc-bga"+suffix, "kc-bgb"+suffix
	idD1, idD2 := "kc-bgd"+suffix, "kc-bge"+suffix
	ctrl := "bg-c-a-" + suffix
	newNode(t, ctrl, "zone-a", "control", "")
	ded1, ded1b := "bg-d1-a-"+suffix, "bg-d1-b-"+suffix
	newNode(t, ded1, "zone-a", "dedicated", idD1)
	newNode(t, ded1b, "zone-b", "dedicated", idD1)
	for _, id := range []string{idA, idB, idD1, idD2} {
		tenantNamespace(t, id)
	}
	podsA := renderPods(t, engine.TierShared, "s.small", idA)
	podsB := renderPods(t, engine.TierShared, "s.small", idB)
	podsD1 := renderPods(t, engine.TierDedicated, "d.xlarge", idD1)
	podsD2 := renderPods(t, engine.TierDedicated, "d.xlarge", idD2)

	a0, a1, a2 := newPod(t, podsA["brokers"], "a0"), newPod(t, podsA["brokers"], "a1"), newPod(t, podsA["brokers"], "a2")
	b0 := newPod(t, podsB["brokers"], "b0")

	t.Run("rule 1: second broker of the same cluster on a node", func(t *testing.T) {
		if err := bind(a0, shared[0]); err != nil {
			t.Fatalf("first broker: %v", err)
		}
		mustRefuse(t, "second broker of the same cluster", bind(a1, shared[0]), "same_group_on_node")
		if err := bind(a1, shared[1]); err != nil {
			t.Fatalf("broker on a different node: %v", err)
		}
		if err := bind(a2, shared[2]); err != nil {
			t.Fatalf("third broker: %v", err)
		}
	})

	t.Run("shared tier: another tenant's broker may share the node", func(t *testing.T) {
		if err := bind(b0, shared[0]); err != nil {
			t.Fatalf("a different tenant's broker was refused on a shared node: %v", err)
		}
	})

	t.Run("rule 4: shared pods stay off dedicated and control nodes", func(t *testing.T) {
		b1 := newPod(t, podsB["brokers"], "b1")
		mustRefuse(t, "shared broker on a dedicated node", bind(b1, ded1), "pod_off_pool")
		mustRefuse(t, "shared broker on a control node", bind(b1, ctrl), "pod_off_pool")
	})

	t.Run("controllers belong on the control pool", func(t *testing.T) {
		c0 := newPod(t, podsA["controllers"], "c0")
		mustRefuse(t, "controller on a shared node", bind(c0, shared[1]), "pod_off_pool")
		if err := bind(c0, ctrl); err != nil {
			t.Fatalf("controller on the control node: %v", err)
		}
	})

	t.Run("rule 3: dedicated nodes hold one broker, only their owner's", func(t *testing.T) {
		d0, d1 := newPod(t, podsD1["brokers"], "d0"), newPod(t, podsD1["brokers"], "d1")
		if err := bind(d0, ded1); err != nil {
			t.Fatalf("dedicated broker on its own node: %v", err)
		}
		mustRefuse(t, "second broker on a dedicated node", bind(d1, ded1), "dedicated_node_multi_exclusive")
		mustRefuse(t, "another cluster's dedicated broker on this cluster's node", bind(newPod(t, podsD2["brokers"], "e0"), ded1), "dedicated_node_foreign_pod")
		if err := bind(d1, ded1b); err != nil {
			t.Fatalf("second dedicated broker on the other owned node: %v", err)
		}
		mustRefuse(t, "dedicated broker on a shared node", bind(newPod(t, podsD1["brokers"], "d2"), shared[2]), "pod_off_pool")
	})

	t.Run("concurrent binds to one node: exactly one wins", func(t *testing.T) {
		node := "bg-race-" + suffix
		newNode(t, node, "zone-b", "shared", "")
		id := "kc-bgr" + suffix
		tenantNamespace(t, id)
		tmpl := renderPods(t, engine.TierShared, "s.small", id)["brokers"]
		const n = 12
		pods := make([]*corev1.Pod, n)
		for i := range pods {
			pods[i] = newPod(t, tmpl, fmt.Sprintf("r%d", i))
		}
		var wg sync.WaitGroup
		errs := make([]error, n)
		for i := range pods {
			wg.Add(1)
			go func() { defer wg.Done(); errs[i] = bind(pods[i], node) }()
		}
		wg.Wait()
		ok := 0
		for _, err := range errs {
			if err == nil {
				ok++
			}
		}
		if ok != 1 {
			t.Fatalf("%d of %d concurrent binds of one cluster's brokers to one node succeeded, want exactly 1: %v", ok, n, errs)
		}
	})
}

// A webhook that cannot be reached must refuse binds, not allow them (failurePolicy Fail).
func TestBindingGuardFailsClosedWhenUnreachable(t *testing.T) {
	g := startGuard(t)
	suffix := randHex(3)
	waitGuardActive(t, suffix)
	node := "fc-node-" + suffix
	newNode(t, node, "zone-a", "shared", "")
	id := "kc-fc" + suffix
	tenantNamespace(t, id)
	p := newPod(t, renderPods(t, engine.TierShared, "s.small", id)["brokers"], "p0")

	g.killServer()
	err := bind(p, node)
	if err == nil {
		t.Fatal("bind succeeded with the guard down; failurePolicy Fail must refuse it")
	}
	if !strings.Contains(err.Error(), "failed calling webhook") {
		t.Fatalf("unexpected error: %v", err)
	}
	got, _ := kube.CoreV1().Pods(id).Get(context.Background(), "p0", metav1.GetOptions{})
	if got.Spec.NodeName != "" {
		t.Fatalf("pod was bound to %q despite the refusal", got.Spec.NodeName)
	}
}
