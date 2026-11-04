// Package agent is the cell agent: it pulls desired state from the control plane,
// applies it with server-side apply, and reports what it observes. It is
// engine-agnostic. Safety rules (design §7.2):
//  1. absence never means delete — only explicit tombstones delete;
//  2. a control-plane revision lower than the last applied one pauses the agent;
//  3. pruning is scoped to one cluster's own previously applied objects;
//  4. a poll that would delete many live clusters at once is held;
//  5. local autonomy only within bundle bounds (storage expansion, M4).
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// FieldManager is the server-side-apply field manager.
const FieldManager = "streamcell-agent"

// Config configures the agent.
type Config struct {
	StateNamespace string        // where the agent keeps its state ConfigMap
	PollWait       time.Duration // long-poll duration
	StatusEvery    time.Duration
	HeartbeatEvery time.Duration
	ResyncEvery    time.Duration // full re-apply for drift correction
	// MassDeleteLimit: a single poll tombstoning more than this many live clusters
	// (and more than 10% of them) is held for manual confirmation.
	MassDeleteLimit int
}

func (c *Config) defaults() {
	if c.StateNamespace == "" {
		c.StateNamespace = "streamcell-system"
	}
	if c.PollWait == 0 {
		c.PollWait = 30 * time.Second
	}
	if c.StatusEvery == 0 {
		c.StatusEvery = 15 * time.Second
	}
	if c.HeartbeatEvery == 0 {
		c.HeartbeatEvery = 15 * time.Second
	}
	if c.ResyncEvery == 0 {
		c.ResyncEvery = 10 * time.Minute
	}
	if c.MassDeleteLimit == 0 {
		c.MassDeleteLimit = 5
	}
}

// ObjectRef identifies an applied object.
type ObjectRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name"`
}

func refOf(u *unstructured.Unstructured) ObjectRef {
	return ObjectRef{APIVersion: u.GetAPIVersion(), Kind: u.GetKind(), Namespace: u.GetNamespace(), Name: u.GetName()}
}

// clusterState is what the agent knows about one resource.
type clusterState struct {
	Generation int64       `json:"generation"`
	Digest     string      `json:"digest"`
	Tombstone  bool        `json:"tombstone"`
	Refs       []ObjectRef `json:"refs"`    // objects applied for Generation (or to delete, for a tombstone)
	Deleted    bool        `json:"deleted"` // tombstone fully applied
	// ReportedDeleted: the control plane has acknowledged a report with Deleted=true,
	// so the agent may forget this cluster.
	ReportedDeleted bool `json:"reportedDeleted"`
	applyErr        string
	bundle          *engine.Bundle
}

// Agent is the cell agent.
type Agent struct {
	CP     *CPClient
	Dyn    dynamic.Interface
	Kube   kubernetes.Interface
	Mapper meta.ResettableRESTMapper
	Cfg    Config
	Log    *slog.Logger

	applyMu  sync.Mutex // serialises apply/delete of bundles (poll loop vs resync)
	mu       sync.Mutex // guards the fields below
	clusters map[string]*clusterState
	revision int64 // highest cell revision applied (persisted)
	seen     int64 // highest cell revision received this process
	paused   string
}

// New builds an agent from a REST config.
func New(rc *rest.Config, cp *CPClient, cfg Config, log *slog.Logger) (*Agent, error) {
	cfg.defaults()
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, err
	}
	kube, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(rc)
	if err != nil {
		return nil, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))
	return &Agent{CP: cp, Dyn: dyn, Kube: kube, Mapper: mapper, Cfg: cfg, Log: log, clusters: map[string]*clusterState{}}, nil
}

// Paused reports why the agent is paused ("" if running).
func (a *Agent) Paused() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.paused
}

// Run runs until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.loadState(ctx); err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	go a.every(ctx, a.Cfg.StatusEvery, a.reportStatus)
	go a.every(ctx, a.Cfg.HeartbeatEvery, a.heartbeat)
	go a.every(ctx, a.Cfg.ResyncEvery, a.resync)
	backoff := time.Second
	for ctx.Err() == nil {
		if err := a.pollOnce(ctx); err != nil {
			if ctx.Err() != nil {
				break
			}
			a.Log.Warn("poll failed; keeping last applied state", "err", err, "retryIn", backoff)
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, time.Minute)
			continue
		}
		backoff = time.Second
	}
	return nil
}

func (a *Agent) every(ctx context.Context, d time.Duration, fn func(context.Context)) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

// pollOnce fetches and applies one batch of desired-state entries.
func (a *Agent) pollOnce(ctx context.Context) error {
	a.mu.Lock()
	since, persisted := a.seen, a.revision
	a.mu.Unlock()
	resp, err := a.CP.Desired(ctx, since, a.Cfg.PollWait)
	if err != nil {
		return err
	}
	if resp.CurrentRevision < persisted {
		msg := fmt.Sprintf("control plane revision %d is lower than last applied %d; refusing to apply (possible database restore)", resp.CurrentRevision, persisted)
		a.setPaused(msg)
		a.Log.Error("CellRevisionRegression", "detail", msg)
		select {
		case <-ctx.Done():
		case <-time.After(a.Cfg.PollWait):
		}
		return nil
	}
	if held := a.massDeleteHeld(ctx, resp.Entries); held != "" {
		a.setPaused(held)
		a.Log.Error("MassDeleteHeld", "detail", held)
		select {
		case <-ctx.Done():
		case <-time.After(a.Cfg.PollWait):
		}
		return nil
	}
	a.setPaused("")
	for _, e := range resp.Entries {
		a.handle(ctx, e)
		a.mu.Lock()
		if e.CellRevision > a.seen {
			a.seen = e.CellRevision
		}
		if e.CellRevision > a.revision {
			a.revision = e.CellRevision
		}
		a.mu.Unlock()
		if err := a.saveState(ctx); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
	}
	if len(resp.Entries) > 0 {
		a.reportStatus(ctx)
	}
	return nil
}

func (a *Agent) setPaused(msg string) {
	a.mu.Lock()
	a.paused = msg
	a.mu.Unlock()
}

// massDeleteHeld implements rule 4. An operator releases the hold by annotating the
// state ConfigMap with streamcell.dev/allow-mass-delete=<revision of the last entry>.
func (a *Agent) massDeleteHeld(ctx context.Context, entries []store.DesiredEntry) string {
	a.mu.Lock()
	live, deleting := 0, 0
	for _, cs := range a.clusters {
		if !cs.Tombstone {
			live++
		}
	}
	var lastRev int64
	for _, e := range entries {
		if cs, ok := a.clusters[e.ClusterID]; ok && e.Tombstone && !cs.Tombstone {
			deleting++
		}
		lastRev = max(lastRev, e.CellRevision)
	}
	a.mu.Unlock()
	if deleting <= a.Cfg.MassDeleteLimit || deleting*10 <= live {
		return ""
	}
	cm, err := a.Kube.CoreV1().ConfigMaps(a.Cfg.StateNamespace).Get(ctx, stateConfigMap, metav1.GetOptions{})
	if err == nil && cm.Annotations["streamcell.dev/allow-mass-delete"] == strconv.FormatInt(lastRev, 10) {
		return ""
	}
	return fmt.Sprintf("poll would delete %d of %d live clusters; annotate configmap %s/%s with streamcell.dev/allow-mass-delete=%d to proceed",
		deleting, live, a.Cfg.StateNamespace, stateConfigMap, lastRev)
}

func (a *Agent) handle(ctx context.Context, e store.DesiredEntry) {
	a.applyMu.Lock()
	defer a.applyMu.Unlock()
	a.mu.Lock()
	cs := a.clusters[e.ClusterID]
	if cs == nil {
		cs = &clusterState{}
		a.clusters[e.ClusterID] = cs
	}
	curGen, curRefs := cs.Generation, append([]ObjectRef{}, cs.Refs...)
	a.mu.Unlock()
	if curGen > e.Generation {
		return // never go backwards
	}
	if e.Tombstone {
		refs := curRefs
		if e.Bundle != nil && len(e.Bundle.Objects) > 0 {
			refs = nil
			for _, o := range e.Bundle.Objects {
				refs = append(refs, refOf(o))
			}
		}
		done, err := a.deleteRefs(ctx, refs)
		a.mu.Lock()
		cs.Generation, cs.Digest, cs.Tombstone, cs.Refs, cs.Deleted, cs.bundle = e.Generation, e.BundleDigest, true, refs, done, nil
		cs.applyErr = errString(err)
		a.mu.Unlock()
		return
	}
	if e.Bundle == nil {
		a.mu.Lock()
		cs.applyErr = "entry has no bundle"
		a.mu.Unlock()
		return
	}
	newRefs, err := a.apply(ctx, e.Bundle)
	if err == nil {
		err = a.prune(ctx, curRefs, newRefs)
	}
	a.mu.Lock()
	cs.Generation, cs.Digest, cs.Tombstone, cs.Deleted, cs.bundle = e.Generation, e.BundleDigest, false, false, e.Bundle
	if err == nil {
		cs.Refs = newRefs
	} else {
		cs.Refs = unionRefs(curRefs, newRefs)
	}
	cs.applyErr = errString(err)
	a.mu.Unlock()
	if err != nil {
		a.Log.Warn("apply failed", "cluster", e.ClusterID, "generation", e.Generation, "err", err)
	} else {
		a.Log.Info("applied", "cluster", e.ClusterID, "generation", e.Generation, "objects", len(newRefs))
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func unionRefs(a, b []ObjectRef) []ObjectRef {
	seen := map[ObjectRef]bool{}
	var out []ObjectRef
	for _, r := range append(append([]ObjectRef{}, a...), b...) {
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	return out
}

func (a *Agent) resource(ref ObjectRef) (dynamic.ResourceInterface, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, err
	}
	gvk := gv.WithKind(ref.Kind)
	m, err := a.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if meta.IsNoMatchError(err) {
		a.Mapper.Reset()
		m, err = a.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	}
	if err != nil {
		return nil, err
	}
	if m.Scope.Name() == meta.RESTScopeNameNamespace {
		return a.Dyn.Resource(m.Resource).Namespace(ref.Namespace), nil
	}
	return a.Dyn.Resource(m.Resource), nil
}

// apply server-side-applies objects in bundle order.
func (a *Agent) apply(ctx context.Context, b *engine.Bundle) ([]ObjectRef, error) {
	var refs []ObjectRef
	for _, o := range b.Objects {
		ref := refOf(o)
		ri, err := a.resource(ref)
		if err != nil {
			return refs, fmt.Errorf("%s %s: %w", ref.Kind, ref.Name, err)
		}
		data, err := json.Marshal(o)
		if err != nil {
			return refs, err
		}
		force := true
		if _, err := ri.Patch(ctx, ref.Name, types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: FieldManager, Force: &force}); err != nil {
			return refs, fmt.Errorf("apply %s %s/%s: %w", ref.Kind, ref.Namespace, ref.Name, err)
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// prune deletes objects this cluster previously applied that are absent from the new bundle.
func (a *Agent) prune(ctx context.Context, old, cur []ObjectRef) error {
	keep := map[ObjectRef]bool{}
	for _, r := range cur {
		keep[r] = true
	}
	var gone []ObjectRef
	for _, r := range old {
		if !keep[r] {
			gone = append(gone, r)
		}
	}
	_, err := a.deleteRefs(ctx, gone)
	return err
}

// deleteRefs deletes in reverse order and reports whether all are gone.
func (a *Agent) deleteRefs(ctx context.Context, refs []ObjectRef) (bool, error) {
	allGone := true
	policy := metav1.DeletePropagationBackground // children (e.g. Strimzi-created) are garbage-collected via owner references
	for i := len(refs) - 1; i >= 0; i-- {
		ref := refs[i]
		ri, err := a.resource(ref)
		if err != nil {
			if meta.IsNoMatchError(err) {
				continue // the type no longer exists, so neither does the object
			}
			return false, err
		}
		err = ri.Delete(ctx, ref.Name, metav1.DeleteOptions{PropagationPolicy: &policy})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete %s %s/%s: %w", ref.Kind, ref.Namespace, ref.Name, err)
		}
		if _, err := ri.Get(ctx, ref.Name, metav1.GetOptions{}); err == nil {
			allGone = false
		} else if !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return allGone, nil
}

// resync re-applies every live bundle (drift correction) and retries unfinished deletes.
func (a *Agent) resync(ctx context.Context) {
	if a.Paused() != "" {
		return
	}
	a.mu.Lock()
	ids := make([]string, 0, len(a.clusters))
	for id := range a.clusters {
		ids = append(ids, id)
	}
	a.mu.Unlock()
	sort.Strings(ids)
	for _, id := range ids {
		a.mu.Lock()
		cs := a.clusters[id]
		if cs == nil { // forgotten since we listed it (deletion completed and acknowledged)
			a.mu.Unlock()
			continue
		}
		gen, digest, tomb, b, refs := cs.Generation, cs.Digest, cs.Tombstone, cs.bundle, cs.Refs
		a.mu.Unlock()
		if tomb {
			a.handle(ctx, store.DesiredEntry{ClusterID: id, Generation: gen, BundleDigest: digest, Tombstone: true, Bundle: refsBundle(refs)})
			continue
		}
		if b != nil {
			a.handle(ctx, store.DesiredEntry{ClusterID: id, Generation: gen, BundleDigest: digest, Bundle: b})
		}
	}
	_ = a.saveState(ctx)
}

func refsBundle(refs []ObjectRef) *engine.Bundle {
	b := &engine.Bundle{}
	for _, r := range refs {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion(r.APIVersion)
		u.SetKind(r.Kind)
		u.SetNamespace(r.Namespace)
		u.SetName(r.Name)
		b.Objects = append(b.Objects, u)
	}
	return b
}

// reportStatus observes every known cluster and pushes the results.
func (a *Agent) reportStatus(ctx context.Context) {
	a.mu.Lock()
	type snap struct {
		id   string
		cs   clusterState
		refs []ObjectRef
	}
	var snaps []snap
	for id, cs := range a.clusters {
		snaps = append(snaps, snap{id: id, cs: *cs, refs: append([]ObjectRef{}, cs.Refs...)})
	}
	a.mu.Unlock()
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].id < snaps[j].id })
	var reports []store.StatusReport
	for _, s := range snaps {
		obs := engine.Observed{Generation: s.cs.Generation, ApplyError: s.cs.applyErr}
		if s.cs.Tombstone {
			if !s.cs.Deleted {
				done, err := a.deleteRefs(ctx, s.refs) // keep driving deletion
				if err == nil && done {
					a.mu.Lock()
					if c := a.clusters[s.id]; c != nil && c.Generation == s.cs.Generation {
						c.Deleted = true
					}
					a.mu.Unlock()
					s.cs.Deleted = true
				}
			}
			obs.Deleted = s.cs.Deleted
		} else {
			for _, r := range s.refs {
				obs.Objects = append(obs.Objects, a.observe(ctx, r))
			}
		}
		reports = append(reports, store.StatusReport{ClusterID: s.id, Observed: obs})
	}
	if len(reports) == 0 {
		return
	}
	if err := a.CP.Status(ctx, reports); err != nil {
		if ctx.Err() == nil {
			a.Log.Warn("status report failed", "err", err)
		}
		return
	}
	a.mu.Lock()
	for _, r := range reports {
		if c := a.clusters[r.ClusterID]; c != nil && r.Observed.Deleted && c.Generation == r.Observed.Generation {
			c.ReportedDeleted = true
		}
	}
	a.mu.Unlock()
	_ = a.saveState(ctx)
}

func (a *Agent) observe(ctx context.Context, r ObjectRef) engine.ObjectStatus {
	os := engine.ObjectStatus{APIVersion: r.APIVersion, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}
	ri, err := a.resource(r)
	if err != nil {
		return os
	}
	u, err := ri.Get(ctx, r.Name, metav1.GetOptions{})
	if err != nil {
		return os
	}
	os.Exists = true
	os.Generation = u.GetGeneration()
	if st, ok := u.Object["status"]; ok {
		os.Status, _ = json.Marshal(st)
	}
	return os
}

// Inventory is the heartbeat payload.
type Inventory struct {
	Nodes []NodeInfo `json:"nodes"`
	Agent string     `json:"agent"`
}

// NodeInfo summarises a node for capacity reconciliation.
type NodeInfo struct {
	Name          string            `json:"name"`
	Labels        map[string]string `json:"labels"`
	CPUMilli      int64             `json:"cpuMilli"`
	MemoryMiB     int64             `json:"memoryMiB"`
	Unschedulable bool              `json:"unschedulable"`
}

func (a *Agent) heartbeat(ctx context.Context) {
	inv := Inventory{Agent: "streamcell-agent/m1"}
	nodes, err := a.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, n := range nodes.Items {
			inv.Nodes = append(inv.Nodes, NodeInfo{
				Name: n.Name, Labels: n.Labels, Unschedulable: n.Spec.Unschedulable,
				CPUMilli:  n.Status.Allocatable.Cpu().MilliValue(),
				MemoryMiB: n.Status.Allocatable.Memory().Value() / (1 << 20),
			})
		}
	}
	if err := a.CP.Heartbeat(ctx, inv); err != nil && ctx.Err() == nil {
		a.Log.Warn("heartbeat failed", "err", err)
	}
}

// ---------- persisted state ----------

const stateConfigMap = "streamcell-agent-state"

func (a *Agent) loadState(ctx context.Context) error {
	cm, err := a.Kube.CoreV1().ConfigMaps(a.Cfg.StateNamespace).Get(ctx, stateConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if v, ok := cm.Data["revision"]; ok {
		a.revision, _ = strconv.ParseInt(v, 10, 64)
	}
	for k, v := range cm.Data {
		id, ok := cutPrefix(k, "c.")
		if !ok {
			continue
		}
		var cs clusterState
		if err := json.Unmarshal([]byte(v), &cs); err != nil {
			return fmt.Errorf("state for %s: %w", id, err)
		}
		a.clusters[id] = &cs
	}
	// seen stays 0: the first poll fetches the full current state so bundles are
	// back in memory; the persisted revision is used only for the regression check.
	return nil
}

func cutPrefix(s, p string) (string, bool) {
	if len(s) >= len(p) && s[:len(p)] == p {
		return s[len(p):], true
	}
	return "", false
}

func (a *Agent) saveState(ctx context.Context) error {
	a.mu.Lock()
	data := map[string]string{"revision": strconv.FormatInt(a.revision, 10)}
	for id, cs := range a.clusters {
		if cs.Tombstone && cs.Deleted && cs.ReportedDeleted {
			delete(a.clusters, id) // deletion done and acknowledged: forget it
			continue
		}
		b, _ := json.Marshal(cs)
		data["c."+id] = string(b)
	}
	a.mu.Unlock()
	cms := a.Kube.CoreV1().ConfigMaps(a.Cfg.StateNamespace)
	cm, err := cms.Get(ctx, stateConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: stateConfigMap, Namespace: a.Cfg.StateNamespace}, Data: data}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	cm.Data = data
	_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return errors.New("state configmap update conflict (is a second agent running?)")
	}
	return err
}
