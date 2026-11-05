package admission

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/avilashs/kafka_as_service/internal/platform/labels"
)

func brokerPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kc-1", Labels: map[string]string{
		labels.Cluster: "kc-1", labels.Role: "kafka-broker", labels.RequiredPool: labels.PoolShared, labels.Exclusive: labels.ExclusivePerResource}}}
}

func bindReq(pod string) *admissionv1.AdmissionRequest {
	raw, _ := json.Marshal(corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: pod, Namespace: "kc-1"}, Target: corev1.ObjectReference{Kind: "Node", Name: "n1"}})
	return &admissionv1.AdmissionRequest{Operation: admissionv1.Create, Namespace: "kc-1", Name: pod, SubResource: "binding",
		Resource: metav1.GroupVersionResource{Version: "v1", Resource: "pods"}, Object: runtime.RawExtension{Raw: raw}}
}

func pods(kube *fake.Clientset) []corev1.Pod {
	l, _ := kube.Tracker().List(corev1.SchemeGroupVersion.WithResource("pods"), corev1.SchemeGroupVersion.WithKind("Pod"), "")
	return l.(*corev1.PodList).Items
}

// The first allowed binding is not yet in the API when the second request arrives (the apiserver persists it
// after the webhook has answered): the guard must still refuse the second broker of the same cluster.
func TestGuardRemembersWhatItJustAllowed(t *testing.T) {
	kube := fake.NewSimpleClientset(
		brokerPod("b0"), brokerPod("b1"),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{labels.NodePool: "shared", labels.Zone: "a"}}})
	// the fake ignores field selectors: answer the guard's "pods on node n1" list as the apiserver would
	kube.PrependReactor("list", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		la := a.(k8stesting.ListAction)
		sel := la.GetListRestrictions().Fields
		if node, ok := sel.RequiresExactMatch("spec.nodeName"); ok {
			var out corev1.PodList
			for _, p := range pods(kube) {
				if p.Spec.NodeName == node {
					out.Items = append(out.Items, p)
				}
			}
			return true, &out, nil
		}
		return false, nil, nil
	})
	now := time.Now()
	g := &Guard{Kube: kube, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), now: func() time.Time { return now }}
	ctx := context.Background()
	if r := g.decide(ctx, bindReq("b0")); !r.Allowed {
		t.Fatalf("the first broker on an empty node was refused: %v", r.Result)
	}
	// nothing was persisted in the fake: only the guard's memory knows
	if r := g.decide(ctx, bindReq("b1")); r.Allowed {
		t.Fatal("a second broker of one cluster was allowed onto the node before the first binding was visible")
	}
	// after the TTL the memory is dropped (and the API would show the truth by then)
	now = now.Add(recentTTL + time.Second)
	if r := g.decide(ctx, bindReq("b1")); !r.Allowed {
		t.Fatalf("a stale memory still refuses: %v", r.Result)
	}
}
