// Package kind is the local NodePoolProvider: a "dedicated node pool" is a set of
// pre-created spare nodes that get the owner label and the dedicated taint. It stands in for
// Karpenter in kind, where nodes cannot be created on demand.
package kind

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"github.com/avilashs/kafka_as_service/internal/placement"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/labels"
)

// Provider claims spare nodes (label streamcell.dev/pool=spare).
type Provider struct {
	Kube kubernetes.Interface
	Log  *slog.Logger
	// SpareSelector optionally narrows which spare nodes may be claimed (extra label selector).
	// A real kind cell has one cell and leaves it empty; tests sharing an API server use it.
	SpareSelector string
}

func (p *Provider) log() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}

// Ensure claims enough spare nodes in each zone.
func (p *Provider) Ensure(ctx context.Context, spec engine.NodePoolSpec) error {
	claimed, err := p.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labels.NodePoolID + "=" + spec.ID})
	if err != nil {
		return err
	}
	have := map[string]int{}
	for _, n := range claimed.Items {
		have[n.Labels[labels.Zone]]++
	}
	zones := make([]string, 0, len(spec.NodesPerZone))
	for z := range spec.NodesPerZone {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	var errs []error
	for _, z := range zones {
		need := spec.NodesPerZone[z] - have[z]
		if need <= 0 {
			continue
		}
		sel := fmt.Sprintf("%s=%s,%s=%s", labels.NodePool, labels.PoolSpare, labels.Zone, z)
		if p.SpareSelector != "" {
			sel += "," + p.SpareSelector
		}
		spares, err := p.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: sel})
		if err != nil {
			return err
		}
		sort.Slice(spares.Items, func(i, j int) bool { return spares.Items[i].Name < spares.Items[j].Name })
		for i := range spares.Items {
			if need == 0 {
				break
			}
			n := &spares.Items[i]
			if _, taken := n.Labels[labels.NodePoolID]; taken {
				continue
			}
			busy, err := p.hasWorkloads(ctx, n.Name)
			if err != nil {
				return err
			}
			if busy {
				p.log().Warn("skipping spare node that is running workloads", "node", n.Name)
				continue
			}
			ok, err := p.claim(ctx, n.Name, spec)
			if err != nil {
				return err
			}
			if ok {
				need--
			}
		}
		if need > 0 {
			errs = append(errs, fmt.Errorf("zone %s: %d more spare node(s) needed for %s", z, need, spec.ID))
		}
	}
	return errors.Join(errs...)
}

func (p *Provider) claim(ctx context.Context, name string, spec engine.NodePoolSpec) (bool, error) {
	claimedNow := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		n, err := p.Kube.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if n.Labels[labels.NodePool] != labels.PoolSpare || n.Labels[labels.NodePoolID] != "" {
			return nil // someone else got it first
		}
		// Taint and labels go in one update, so there is no moment when the node is
		// labelled for the owner but open to everyone.
		for k, v := range spec.Labels {
			n.Labels[k] = v
		}
		n.Labels[labels.NodePoolID] = spec.ID
		for _, t := range spec.Taints {
			if !hasTaint(n, t) {
				n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: t.Key, Value: t.Value, Effect: corev1.TaintEffect(t.Effect)})
			}
		}
		n.Spec.Taints = withoutTaint(n.Spec.Taints, labels.TaintSpare) // claimed: no longer a spare
		if _, err := p.Kube.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{}); err != nil {
			return err
		}
		claimedNow = true
		return nil
	})
	if err == nil && claimedNow {
		p.log().Info("claimed node for dedicated pool", "node", name, "pool", spec.ID, "owner", spec.Owner)
	}
	return claimedNow, err
}

func withoutTaint(ts []corev1.Taint, key string) []corev1.Taint {
	out := make([]corev1.Taint, 0, len(ts))
	for _, t := range ts {
		if t.Key != key {
			out = append(out, t)
		}
	}
	return out
}

func hasTaint(n *corev1.Node, t engine.Taint) bool {
	for _, x := range n.Spec.Taints {
		if x.Key == t.Key && x.Value == t.Value && string(x.Effect) == t.Effect {
			return true
		}
	}
	return false
}

// hasWorkloads reports whether any non-system pod (including terminating ones) is on the node.
func (p *Provider) hasWorkloads(ctx context.Context, node string) (bool, error) {
	pods, err := p.Kube.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + node})
	if err != nil {
		return false, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if !placement.PodFrom(pod).System {
			return true, nil
		}
	}
	return false, nil
}

// Release turns the pool's nodes back into spares once their workloads are gone.
func (p *Provider) Release(ctx context.Context, spec engine.NodePoolSpec) (bool, error) {
	claimed, err := p.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labels.NodePoolID + "=" + spec.ID})
	if err != nil {
		return false, err
	}
	remaining := 0
	for _, n := range claimed.Items {
		busy, err := p.hasWorkloads(ctx, n.Name)
		if err != nil {
			return false, err
		}
		if busy {
			remaining++ // keep the taint until the owner's pods are gone
			continue
		}
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cur, err := p.Kube.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if cur.Labels[labels.NodePoolID] != spec.ID {
				return nil
			}
			for k := range spec.Labels {
				delete(cur.Labels, k)
			}
			delete(cur.Labels, labels.NodePoolID)
			delete(cur.Labels, labels.DedicatedOwner)
			cur.Labels[labels.NodePool] = labels.PoolSpare
			cur.Spec.Taints = withoutTaint(cur.Spec.Taints, labels.TaintDedicated)
			// Back to a spare: closed to general workloads again (the taint is added last-in-update,
			// together with removing the owner taint, so there is never an untainted moment).
			cur.Spec.Taints = append(withoutTaint(cur.Spec.Taints, labels.TaintSpare),
				corev1.Taint{Key: labels.TaintSpare, Value: "true", Effect: corev1.TaintEffectNoSchedule})
			_, err = p.Kube.CoreV1().Nodes().Update(ctx, cur, metav1.UpdateOptions{})
			return err
		})
		if err != nil {
			return false, err
		}
		p.log().Info("released node to the spare pool", "node", n.Name, "pool", spec.ID)
	}
	return remaining == 0, nil
}
