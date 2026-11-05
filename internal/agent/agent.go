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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
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

	"github.com/avilashs/kafka_as_service/internal/cloud"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
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
	// NodePools are the dedicated pools this resource holds; they are released, after every
	// object and pod is gone, when the resource is deleted.
	NodePools []engine.NodePoolSpec `json:"nodePools,omitempty"`
	// DeleteGrace, if > 0, is the termination grace used for the resource's pods on delete.
	DeleteGrace int64 `json:"deleteGrace,omitempty"`
	applyErr    string
	poolErr     string
	bundle      *engine.Bundle
}

// Agent is the cell agent.
type Agent struct {
	CP     *CPClient
	Dyn    dynamic.Interface
	Kube   kubernetes.Interface
	Mapper meta.ResettableRESTMapper
	Cfg    Config
	Log    *slog.Logger
	// Pools provisions dedicated node pools. Nil means the cell has none: bundles that
	// ask for one then report an apply error instead of silently running unplaced.
	Pools cloud.NodePoolProvider
	// Secrets resolves the references in bundle.Secrets. Nil means the cell has no secret manager:
	// a bundle that needs a secret then reports an apply error.
	Secrets secrets.Store
	// Metering, if set, makes the agent sample what its resources run and deliver billing records.
	Metering *MeteringConfig
	mrt      *meterRuntime

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
	go a.meterLoop(ctx)
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
		a.mu.Lock()
		pools, grace := cs.NodePools, cs.DeleteGrace
		a.mu.Unlock()
		if e.Bundle != nil {
			if len(e.Bundle.Objects) > 0 {
				refs = nil
				for _, o := range e.Bundle.Objects {
					refs = append(refs, refOf(o))
				}
			}
			if len(e.Bundle.NodePools) > 0 {
				pools = e.Bundle.NodePools
			}
			if e.Bundle.DeleteGraceSeconds > 0 {
				grace = e.Bundle.DeleteGraceSeconds
			}
		}
		a.mu.Lock()
		cs.Generation, cs.Digest, cs.Tombstone, cs.Refs, cs.bundle = e.Generation, e.BundleDigest, true, refs, nil
		cs.NodePools, cs.DeleteGrace, cs.Deleted = pools, grace, false
		a.mu.Unlock()
		done, err := a.driveDelete(ctx, refs, pools, grace)
		a.mu.Lock()
		cs.Deleted, cs.applyErr = done, errString(err)
		a.mu.Unlock()
		return
	}
	if e.Bundle == nil {
		a.mu.Lock()
		cs.applyErr = "entry has no bundle"
		a.mu.Unlock()
		return
	}
	// Dedicated nodes first, so they are labelled and tainted before any pod can be scheduled.
	poolErr := a.ensurePools(ctx, e.Bundle.NodePools)
	newRefs, err := a.apply(ctx, e.Bundle)
	if err == nil {
		err = a.prune(ctx, curRefs, newRefs)
	}
	a.mu.Lock()
	cs.Generation, cs.Digest, cs.Tombstone, cs.Deleted, cs.bundle = e.Generation, e.BundleDigest, false, false, e.Bundle
	cs.NodePools, cs.DeleteGrace, cs.poolErr = e.Bundle.NodePools, e.Bundle.DeleteGraceSeconds, errString(poolErr)
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

// ensurePools asks the provider for the bundle's dedicated node pools.
func (a *Agent) ensurePools(ctx context.Context, pools []engine.NodePoolSpec) error {
	if len(pools) == 0 {
		return nil
	}
	if a.Pools == nil {
		return errors.New("bundle requests a dedicated node pool but this cell has no node pool provider")
	}
	var errs []error
	for _, p := range pools {
		if err := a.Pools.Ensure(ctx, p); err != nil {
			errs = append(errs, fmt.Errorf("node pool %s: %w", p.ID, err))
		}
	}
	return errors.Join(errs...)
}

// driveDelete performs one pass of deleting a resource and reports whether it is finished:
// objects deleted, pods terminated quickly (the data is being destroyed anyway), and only
// then the dedicated nodes returned. Called on every status tick until it reports done.
//
// Order matters. The grace period a kubelet works to is the one on the *first* delete it sees:
// shortening it afterwards updates the API object but does not interrupt a container stop that
// is already running (CI showed brokers lingering for the full 300 s with gracePeriod=20 stamped
// on them). Deleting an owner makes garbage collection delete its pods with their full grace, so
// the pods must be deleted with the short grace *before* the owners go, and once more after, for
// pods an operator recreated in between.
func (a *Agent) driveDelete(ctx context.Context, refs []ObjectRef, pools []engine.NodePoolSpec, graceSeconds int64) (bool, error) {
	if graceSeconds > 0 {
		a.terminatePods(ctx, refs, graceSeconds)
	}
	gone, err := a.deleteRefs(ctx, refs)
	if graceSeconds > 0 {
		a.terminatePods(ctx, refs, graceSeconds)
	}
	if err != nil || !gone {
		return false, err
	}
	if len(pools) == 0 {
		return true, nil
	}
	if a.Pools == nil {
		return false, errors.New("resource holds a dedicated node pool but this cell has no node pool provider")
	}
	all := true
	for _, p := range pools {
		done, err := a.Pools.Release(ctx, p)
		if err != nil {
			return false, fmt.Errorf("release node pool %s: %w", p.ID, err)
		}
		all = all && done
	}
	return all, nil
}

// terminatePods shortens the grace period of every pod left in the resource's namespaces.
func (a *Agent) terminatePods(ctx context.Context, refs []ObjectRef, graceSeconds int64) {
	for _, r := range refs {
		if r.Kind != "Namespace" || r.APIVersion != "v1" {
			continue
		}
		pods, err := a.Kube.CoreV1().Pods(r.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue // namespace already gone
		}
		for i := range pods.Items {
			err := a.Kube.CoreV1().Pods(r.Name).Delete(ctx, pods.Items[i].Name, metav1.DeleteOptions{GracePeriodSeconds: &graceSeconds})
			if err != nil && !apierrors.IsNotFound(err) {
				a.Log.Warn("terminate pod", "pod", r.Name+"/"+pods.Items[i].Name, "err", err)
			}
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func joinErr(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + "; " + b
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
	// Secrets are created as soon as their namespace exists, before the other objects of that
	// namespace, so that objects which reference them (KafkaUser passwords) find them at once.
	secretsDone := map[string]bool{}
	applySecrets := func(ns string) error {
		if secretsDone[ns] {
			return nil
		}
		secretsDone[ns] = true
		for _, m := range b.Secrets {
			if m.Namespace != ns {
				continue
			}
			ref, err := a.applySecret(ctx, m)
			if err != nil {
				return err
			}
			refs = append(refs, ref)
		}
		return nil
	}
	for _, o := range b.Objects {
		if o.GetKind() != "Namespace" {
			if err := applySecrets(o.GetNamespace()); err != nil {
				return refs, err
			}
		}
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
	for _, m := range b.Secrets { // namespaces that had no other object
		if err := applySecrets(m.Namespace); err != nil {
			return refs, err
		}
	}
	return refs, nil
}

// applySecret resolves the references of m from the secret manager and writes the Kubernetes
// Secret. Values are never logged and never leave this function except into the Secret.
func (a *Agent) applySecret(ctx context.Context, m engine.SecretMaterial) (ObjectRef, error) {
	ref := ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: m.Namespace, Name: m.Name}
	if a.Secrets == nil {
		return ref, fmt.Errorf("secret %s/%s needs a secret manager, but this cell has none configured", m.Namespace, m.Name)
	}
	data := map[string]any{}
	for key, r := range m.Data {
		vals, _, err := a.Secrets.Get(ctx, r.Path)
		if err != nil {
			return ref, fmt.Errorf("secret %s/%s: resolving %s: %w", m.Namespace, m.Name, r.Path, err)
		}
		v, ok := vals[r.Key]
		if !ok {
			return ref, fmt.Errorf("secret %s/%s: %s has no field %q", m.Namespace, m.Name, r.Path, r.Key)
		}
		data[key] = base64.StdEncoding.EncodeToString([]byte(v))
	}
	lbls := map[string]any{}
	for k, v := range m.Labels {
		lbls[k] = v
	}
	obj := map[string]any{"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
		"metadata": map[string]any{"name": m.Name, "namespace": m.Namespace, "labels": lbls}, "data": data}
	raw, _ := json.Marshal(obj)
	force := true
	if _, err := a.Kube.CoreV1().Secrets(m.Namespace).Patch(ctx, m.Name, types.ApplyPatchType, raw, metav1.PatchOptions{FieldManager: FieldManager, Force: &force}); err != nil {
		return ref, fmt.Errorf("apply Secret %s/%s: %w", m.Namespace, m.Name, err)
	}
	return ref, nil
}

// readOutputs reads the public values the bundle asked to publish. A missing object is not an
// error: the value simply is not there yet (for example the CA before the operator created it).
func (a *Agent) readOutputs(ctx context.Context, b *engine.Bundle) map[string]string {
	if b == nil || len(b.Outputs) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, o := range b.Outputs {
		if o.Kind != "Secret" || o.APIVersion != "v1" {
			continue // only Secrets (of public material) are supported
		}
		sec, err := a.Kube.CoreV1().Secrets(o.Namespace).Get(ctx, o.ObjectName, metav1.GetOptions{})
		if err != nil {
			continue
		}
		if v, ok := sec.Data[o.Key]; ok {
			out[o.Name] = string(v)
		}
	}
	return out
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
				done, err := a.driveDelete(ctx, s.refs, s.cs.NodePools, s.cs.DeleteGrace) // keep driving deletion
				a.mu.Lock()
				if c := a.clusters[s.id]; c != nil && c.Generation == s.cs.Generation {
					c.applyErr = errString(err)
					if err == nil && done {
						c.Deleted = true
					}
				}
				a.mu.Unlock()
				s.cs.Deleted = err == nil && done
				obs.ApplyError = errString(err)
			}
			obs.Deleted = s.cs.Deleted
		} else {
			if len(s.cs.NodePools) > 0 && s.cs.poolErr != "" { // retry pools that could not be provisioned yet
				err := a.ensurePools(ctx, s.cs.NodePools)
				a.mu.Lock()
				if c := a.clusters[s.id]; c != nil && c.Generation == s.cs.Generation {
					c.poolErr = errString(err)
				}
				a.mu.Unlock()
				s.cs.poolErr = errString(err)
			}
			if s.cs.poolErr != "" {
				obs.ApplyError = joinErr(obs.ApplyError, s.cs.poolErr)
			}
			for _, r := range s.refs {
				obs.Objects = append(obs.Objects, a.observe(ctx, r))
			}
			obs.Pods = a.observePods(ctx, s.refs)
			obs.Outputs = a.readOutputs(ctx, s.cs.bundle)
			for k, v := range a.certOutputs(ctx, s.cs.bundle) {
				if obs.Outputs == nil {
					obs.Outputs = map[string]string{}
				}
				obs.Outputs[k] = v
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

// observePods lists the pods of every namespace the cluster owns, in the compact form the engine checks.
func (a *Agent) observePods(ctx context.Context, refs []ObjectRef) []engine.PodStatus {
	seen := map[string]bool{}
	var out []engine.PodStatus
	for _, r := range refs {
		if r.Kind != "Namespace" || seen[r.Name] {
			continue
		}
		seen[r.Name] = true
		pods, err := a.Kube.CoreV1().Pods(r.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}
		for i := range pods.Items {
			p := &pods.Items[i]
			ps := engine.PodStatus{Name: p.Name, Terminating: p.DeletionTimestamp != nil, Labels: map[string]string{}}
			for k, v := range p.Labels {
				if strings.HasPrefix(k, "strimzi.io/") || strings.HasPrefix(k, "streamcell.dev/") {
					ps.Labels[k] = v
				}
			}
			for _, c := range p.Status.Conditions {
				if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
					ps.Ready = true
				}
			}
			for _, c := range p.Spec.Containers {
				ps.Containers = append(ps.Containers, engine.ContainerStatus{Name: c.Name, Image: c.Image,
					MemoryLimitMiB: c.Resources.Limits.Memory().Value() / (1 << 20)})
			}
			out = append(out, ps)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
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

// HasBundle reports whether the agent holds the rendered state of a live resource (after a restart this
// is true again once the first poll has delivered it).
func (a *Agent) HasBundle(clusterID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	cs := a.clusters[clusterID]
	return cs != nil && cs.bundle != nil && !cs.Tombstone
}
