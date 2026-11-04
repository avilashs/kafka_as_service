//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var (
	kafkaGVR    = schema.GroupVersionResource{Group: "kafka.strimzi.io", Version: "v1", Resource: "kafkas"}
	nodePoolGVR = schema.GroupVersionResource{Group: "kafka.strimzi.io", Version: "v1", Resource: "kafkanodepools"}
)

// opSim is a TEST-ONLY stand-in for two controllers that envtest lacks:
//   - Strimzi: marks a Kafka CR Ready for its current generation (after readyDelay)
//     and publishes the external listener's bootstrap address, as Strimzi's status does;
//   - the namespace controller: finalizes Terminating namespaces once our objects are gone.
//
// It never creates or deletes platform objects itself.
type opSim struct {
	dyn        dynamic.Interface
	kube       kubernetes.Interface
	readyDelay atomic.Int64 // nanoseconds
	mu         sync.Mutex
	firstSeen  map[string]time.Time
}

func (s *opSim) setReadyDelay(d time.Duration) { s.readyDelay.Store(int64(d)) }

func (s *opSim) run(ctx context.Context) {
	s.firstSeen = map[string]time.Time{}
	t := time.NewTicker(150 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reconcileKafkas(ctx)
			s.confirmPodTermination(ctx)
			s.finalizeNamespaces(ctx)
		}
	}
}

func (s *opSim) reconcileKafkas(ctx context.Context) {
	list, err := s.dyn.Resource(kafkaGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for i := range list.Items {
		k := &list.Items[i]
		if k.GetDeletionTimestamp() != nil {
			continue
		}
		og, _, _ := unstructured.NestedInt64(k.Object, "status", "observedGeneration")
		if og == k.GetGeneration() {
			continue
		}
		key := fmt.Sprintf("%s/%s/%d", k.GetNamespace(), k.GetName(), k.GetGeneration())
		s.mu.Lock()
		first, ok := s.firstSeen[key]
		if !ok {
			first = time.Now()
			s.firstSeen[key] = first
		}
		s.mu.Unlock()
		if time.Since(first) < time.Duration(s.readyDelay.Load()) {
			continue
		}
		version, _, _ := unstructured.NestedString(k.Object, "spec", "kafka", "version")
		bootstrap := ""
		listeners, _, _ := unstructured.NestedSlice(k.Object, "spec", "kafka", "listeners")
		for _, l := range listeners {
			lm := l.(map[string]any)
			if lm["name"] == "external" {
				bootstrap, _, _ = unstructured.NestedString(lm, "configuration", "bootstrap", "host")
			}
		}
		k.Object["status"] = map[string]any{
			"observedGeneration": k.GetGeneration(),
			"conditions": []any{map[string]any{
				"type": "Ready", "status": "True", "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
			}},
			"kafkaVersion": version,
			"listeners":    []any{map[string]any{"name": "external", "bootstrapServers": bootstrap + ":443"}},
		}
		_, _ = s.dyn.Resource(kafkaGVR).Namespace(k.GetNamespace()).UpdateStatus(ctx, k, metav1.UpdateOptions{})
	}
}

func (s *opSim) confirmPodTermination(ctx context.Context) {
	pods, err := s.kube.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	zero := int64(0)
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil {
			_ = s.kube.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}
}

func (s *opSim) finalizeNamespaces(ctx context.Context) {
	nss, err := s.kube.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: "streamcell.dev/inventory"})
	if err != nil {
		return
	}
	for i := range nss.Items {
		ns := &nss.Items[i]
		if ns.DeletionTimestamp == nil || len(ns.Spec.Finalizers) == 0 {
			continue
		}
		// The real namespace controller deletes all content; here the agent must
		// already have deleted everything it created, so only finalize when empty.
		empty := true
		for _, gvr := range []schema.GroupVersionResource{kafkaGVR, nodePoolGVR} {
			l, err := s.dyn.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{})
			if err != nil || len(l.Items) > 0 {
				empty = false
			}
		}
		if np, err := s.kube.NetworkingV1().NetworkPolicies(ns.Name).List(ctx, metav1.ListOptions{}); err != nil || len(np.Items) > 0 {
			empty = false
		}
		if pods, err := s.kube.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{}); err != nil || len(pods.Items) > 0 {
			empty = false
		}
		if !empty {
			continue
		}
		ns.Spec.Finalizers = nil
		if _, err := s.kube.CoreV1().Namespaces().Finalize(ctx, ns, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
			continue
		}
	}
}
