package placement

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/pager"

	"github.com/avilashs/kafka_as_service/internal/platform/labels"
)

// NodeFrom converts a Kubernetes node.
func NodeFrom(n *corev1.Node) Node {
	return Node{Name: n.Name, Zone: n.Labels[labels.Zone], Pool: n.Labels[labels.NodePool], Owner: n.Labels[labels.DedicatedOwner]}
}

// PodFrom converts a Kubernetes pod.
func PodFrom(p *corev1.Pod) Pod {
	out := Pod{
		Namespace: p.Namespace, Name: p.Name, Node: p.Spec.NodeName,
		Cluster: p.Labels[labels.Cluster], Role: p.Labels[labels.Role],
		RequiredPool: p.Labels[labels.RequiredPool], Exclusive: p.Labels[labels.Exclusive],
	}
	for _, o := range p.OwnerReferences {
		if o.Kind == "DaemonSet" {
			out.System = true
		}
	}
	if _, mirror := p.Annotations[corev1.MirrorPodAnnotationKey]; mirror {
		out.System = true
	}
	return out
}

// CollectOptions restricts what is read.
type CollectOptions struct {
	// NodeSelector is a label selector for nodes. When set, only those nodes and
	// the pods running on them are considered (used by tests that share an API server).
	NodeSelector string
}

// Collect reads the actual state of the cell. Terminal and terminating pods are
// ignored: they no longer (or are about to stop) occupy the node.
func Collect(ctx context.Context, kube kubernetes.Interface, opt CollectOptions) (Snapshot, error) {
	s := Snapshot{Nodes: map[string]Node{}}
	nodes, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: opt.NodeSelector})
	if err != nil {
		return s, fmt.Errorf("list nodes: %w", err)
	}
	for i := range nodes.Items {
		n := NodeFrom(&nodes.Items[i])
		s.Nodes[n.Name] = n
	}
	pg := pager.New(func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
		return kube.CoreV1().Pods(metav1.NamespaceAll).List(ctx, o)
	})
	pg.PageSize = 500
	err = pg.EachListItem(ctx, metav1.ListOptions{}, func(obj runtime.Object) error {
		p := obj.(*corev1.Pod)
		if p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			return nil
		}
		pod := PodFrom(p)
		if opt.NodeSelector != "" {
			if _, ok := s.Nodes[pod.Node]; !ok {
				return nil
			}
		}
		s.Pods = append(s.Pods, pod)
		return nil
	})
	if err != nil {
		return s, fmt.Errorf("list pods: %w", err)
	}
	return s, nil
}
