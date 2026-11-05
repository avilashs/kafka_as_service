// Package admission implements the pods/binding guard: a validating webhook that
// refuses to bind a pod to a node when doing so would break a node-local placement
// rule (design §10.2, rule 5). It complements the ValidatingAdmissionPolicy, which
// can only look at the pod itself, not at what already runs on the target node.
//
// The scheduler's own anti-affinity already prevents these placements; the guard
// exists so that a scheduler bug, a second scheduler, or a hand-made Binding cannot.
package admission

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/avilashs/kafka_as_service/internal/placement"
)

// Guard is the binding webhook handler.
type Guard struct {
	Kube kubernetes.Interface
	Log  *slog.Logger

	// locks serialise decisions per node within this replica, so two concurrent binds of
	// pods that conflict on the same node cannot both read an empty node.
	// Across replicas the guarantee is weaker (see docs/milestones/m2.md): the scheduler
	// binds one pod at a time, and the auditor catches anything that slips through.
	mu    sync.Mutex
	locks map[string]*sync.Mutex

	// recent remembers the bindings this replica allowed, per node, until they show up in the API (or
	// recentTTL passes). The per-node lock alone is released before the apiserver has persisted the
	// binding, so a second decision made a few milliseconds later could still read an empty node; found as
	// 2 winners in 12 in one full-suite run under CPU contention. A remembered pod that was never bound
	// only makes the guard stricter for recentTTL (fail closed).
	recent map[string][]recentBind
	now    func() time.Time
}

type recentBind struct {
	pod   placement.Pod
	until time.Time
}

const recentTTL = 30 * time.Second

func (g *Guard) clock() time.Time {
	if g.now != nil {
		return g.now()
	}
	return time.Now()
}

// recentOn returns the pods allowed onto node that the API list did not (yet) show, dropping expired ones.
func (g *Guard) recentOn(node string, listed []placement.Pod) []placement.Pod {
	g.mu.Lock()
	defer g.mu.Unlock()
	have := map[string]bool{}
	for _, p := range listed {
		have[p.Ref()] = true
	}
	var keep []recentBind
	var extra []placement.Pod
	now := g.clock()
	for _, r := range g.recent[node] {
		if now.After(r.until) || have[r.pod.Ref()] {
			continue // expired, or the API shows it now
		}
		keep = append(keep, r)
		extra = append(extra, r.pod)
	}
	if g.recent == nil {
		g.recent = map[string][]recentBind{}
	}
	g.recent[node] = keep
	return extra
}

func (g *Guard) remember(node string, p placement.Pod) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.recent == nil {
		g.recent = map[string][]recentBind{}
	}
	p.Node = node
	g.recent[node] = append(g.recent[node], recentBind{pod: p, until: g.clock().Add(recentTTL)})
}

func (g *Guard) nodeLock(node string) *sync.Mutex {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.locks == nil {
		g.locks = map[string]*sync.Mutex{}
	}
	l := g.locks[node]
	if l == nil {
		l = &sync.Mutex{}
		g.locks[node] = l
	}
	return l
}

// Handler serves POST /validate-binding and GET /healthz.
func (g *Guard) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /validate-binding", g.serve)
	return mux
}

func (g *Guard) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil || review.Request == nil {
		http.Error(w, "not an AdmissionReview", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	resp := g.decide(ctx, review.Request)
	resp.UID = review.Request.UID
	out := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Response: resp,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func deny(code int32, format string, args ...any) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{Allowed: false, Result: &metav1.Status{
		Status: metav1.StatusFailure, Code: code, Reason: metav1.StatusReasonForbidden,
		Message: "streamcell placement guard: " + fmt.Sprintf(format, args...)}}
}

func (g *Guard) decide(ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if req.Resource.Resource != "pods" || req.SubResource != "binding" || req.Operation != admissionv1.Create {
		return &admissionv1.AdmissionResponse{Allowed: true} // not ours; the webhook rules should not send these
	}
	var b corev1.Binding
	if err := json.Unmarshal(req.Object.Raw, &b); err != nil {
		return deny(http.StatusBadRequest, "cannot decode Binding: %v", err)
	}
	nodeName := b.Target.Name
	if nodeName == "" {
		return deny(http.StatusBadRequest, "binding has no target node")
	}
	lock := g.nodeLock(nodeName)
	lock.Lock()
	defer lock.Unlock()

	// Fail closed on any read error: a pod that cannot be checked is not bound (the scheduler retries).
	pod, err := g.Kube.CoreV1().Pods(req.Namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return deny(http.StatusServiceUnavailable, "cannot read pod %s/%s: %v", req.Namespace, req.Name, err)
	}
	node, err := g.Kube.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return deny(http.StatusServiceUnavailable, "cannot read node %s: %v", nodeName, err)
	}
	// Live (uncached) read of the pods already on the node, from all namespaces.
	onNodeList, err := g.Kube.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + nodeName})
	if err != nil {
		return deny(http.StatusServiceUnavailable, "cannot list pods on node %s: %v", nodeName, err)
	}
	var onNode []placement.Pod
	for i := range onNodeList.Items {
		p := &onNodeList.Items[i]
		if p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		onNode = append(onNode, placement.PodFrom(p))
	}
	onNode = append(onNode, g.recentOn(nodeName, onNode)...)
	vs := placement.CheckBinding(placement.PodFrom(pod), placement.NodeFrom(node), onNode)
	if len(vs) == 0 {
		g.remember(nodeName, placement.PodFrom(pod))
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
	msgs := make([]string, 0, len(vs))
	for _, v := range vs {
		msgs = append(msgs, fmt.Sprintf("[%s] %s", v.Rule, v.Detail))
	}
	g.Log.Warn("binding refused", "pod", req.Namespace+"/"+req.Name, "node", nodeName, "violations", msgs)
	return deny(http.StatusForbidden, "binding %s/%s to node %s refused: %s", req.Namespace, req.Name, nodeName, strings.Join(msgs, "; "))
}
