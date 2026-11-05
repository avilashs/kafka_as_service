package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/avilashs/kafka_as_service/internal/metering"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// MeteringConfig enables billing metering in the agent (design §17, ADR-0017).
type MeteringConfig struct {
	// Dir holds the meter state and the spool of undelivered records. In a cell this is a PVC: it must survive agent restarts.
	Dir string
	// Cell is the cell id stamped on the records.
	Cell string
	// SampleEvery is how often resources are sampled (a minute in production).
	SampleEvery time.Duration
	// Interval is the billing interval (an hour in production).
	Interval time.Duration
	// MaxGap bounds how long the last state is carried over missed samples.
	MaxGap time.Duration
	// Now is the clock (tests set their own).
	Now func() time.Time
}

type meterRuntime struct {
	cfg   MeteringConfig
	meter *metering.Meter
	spool *metering.Spool
}

func (a *Agent) meterRT() (*meterRuntime, error) {
	if a.Metering == nil {
		return nil, nil
	}
	if a.mrt != nil {
		return a.mrt, nil
	}
	cfg := *a.Metering
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SampleEvery <= 0 {
		cfg.SampleEvery = time.Minute
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.MaxGap <= 0 {
		cfg.MaxGap = 30 * time.Minute
	}
	m := metering.New(metering.Config{Cell: cfg.Cell, Interval: cfg.Interval, MaxGap: cfg.MaxGap})
	if b, err := os.ReadFile(filepath.Join(cfg.Dir, "meter-state.json")); err == nil {
		if err := m.UnmarshalState(b); err != nil {
			return nil, fmt.Errorf("meter state: %w", err)
		}
	}
	sp, err := metering.OpenSpool(filepath.Join(cfg.Dir, "spool.jsonl"))
	if err != nil {
		return nil, err
	}
	a.mrt = &meterRuntime{cfg: cfg, meter: m, spool: sp}
	return a.mrt, nil
}

// MeterTick samples every resource the agent holds and does the accounting for time `now`. It is the
// body of the sampling loop, exported so that tests can drive it with a clock of their own.
func (a *Agent) MeterTick(ctx context.Context, now time.Time) error {
	rt, err := a.meterRT()
	if err != nil || rt == nil {
		return err
	}
	type target struct {
		id      string
		meters  []engine.MeterSource
		deleted bool
	}
	a.mu.Lock()
	live := map[string]bool{}  // resources whose meters we can read now
	known := map[string]bool{} // resources the agent still holds (a restart has not reloaded every bundle yet)
	var targets []target
	for id, cs := range a.clusters {
		t := target{id: id, deleted: cs.Deleted || cs.ReportedDeleted}
		if cs.bundle != nil {
			t.meters = cs.bundle.Meters
		}
		if !t.deleted {
			known[id] = true
			if len(t.meters) > 0 {
				live[id] = true
			}
		}
		targets = append(targets, t)
	}
	a.mu.Unlock()
	sort.Slice(targets, func(i, j int) bool { return targets[i].id < targets[j].id })

	var closed []metering.Record
	for _, t := range targets {
		if !live[t.id] {
			continue
		}
		readings, err := a.sample(ctx, t.meters)
		if err != nil {
			a.Log.Warn("metering: sampling failed; this tick is skipped for the resource", "cluster", t.id, "err", err)
			continue
		}
		closed = append(closed, rt.meter.Observe(t.id, now, readings)...)
	}
	// Resources the meter still holds but the agent no longer has were deleted: close their intervals. One
	// that is merely not readable yet (bundle not reloaded after a restart) is left alone; the gap rule applies.
	for _, id := range rt.meter.Clusters() {
		if !known[id] {
			closed = append(closed, rt.meter.Close(id, now)...)
		}
	}
	// Records reach the disk before the state that produced them: a crash in between re-derives the same
	// records (same IDs, same quantities), which the control plane takes once.
	if err := rt.spool.Append(closed); err != nil {
		return err
	}
	b, err := rt.meter.MarshalState()
	if err != nil {
		return err
	}
	return metering.SaveFile(filepath.Join(rt.cfg.Dir, "meter-state.json"), b)
}

// MeterDeliver sends the spooled records to the control plane and forgets those it acknowledged.
func (a *Agent) MeterDeliver(ctx context.Context) error {
	rt, err := a.meterRT()
	if err != nil || rt == nil {
		return err
	}
	for {
		batch := rt.spool.Pending(500)
		if len(batch) == 0 {
			return nil
		}
		resp, err := a.CP.Metering(ctx, batch)
		if err != nil {
			return err // stays in the spool; retried on the next tick
		}
		if len(resp.Rejected) > 0 {
			a.Log.Error("metering: the control plane rejected records (kept in the spool for inspection)", "count", len(resp.Rejected))
		}
		if len(resp.Acked) == 0 {
			return fmt.Errorf("control plane acknowledged none of %d records", len(batch))
		}
		if err := rt.spool.Ack(resp.Acked); err != nil {
			return err
		}
	}
}

func (a *Agent) meterLoop(ctx context.Context) {
	if a.Metering == nil {
		return
	}
	rt, err := a.meterRT()
	if err != nil {
		a.Log.Error("metering disabled", "err", err)
		return
	}
	a.every(ctx, rt.cfg.SampleEvery, func(ctx context.Context) {
		if err := a.MeterTick(ctx, rt.cfg.Now()); err != nil {
			a.Log.Warn("metering tick failed", "err", err)
		}
		if err := a.MeterDeliver(ctx); err != nil {
			a.Log.Warn("metering delivery failed; records stay in the spool", "err", err)
		}
	})
}

// sample reads the engine-described meters from the cell.
func (a *Agent) sample(ctx context.Context, srcs []engine.MeterSource) ([]metering.Reading, error) {
	var out []metering.Reading
	for _, s := range srcs {
		kind := metering.Gauge
		if s.Kind == "counter" {
			kind = metering.Counter
		}
		switch s.Source {
		case "pods":
			pods, err := a.Kube.CoreV1().Pods(s.Namespace).List(ctx, metav1.ListOptions{LabelSelector: s.Selector})
			if err != nil {
				return nil, err
			}
			counts := map[string]int64{}
			for i := range pods.Items {
				p := &pods.Items[i]
				if p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil {
					continue
				}
				key := ""
				if s.GroupByLabel != "" {
					key = p.Labels[s.GroupByLabel]
				}
				counts[key]++
			}
			keys := make([]string, 0, len(counts))
			for k := range counts {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				r := metering.Reading{Meter: s.Meter, Kind: kind, Unit: s.Unit, Value: counts[k]}
				if s.LabelName != "" {
					r.Labels = map[string]string{s.LabelName: k}
				}
				out = append(out, r)
			}
		case "pvc-gib":
			pvcs, err := a.Kube.CoreV1().PersistentVolumeClaims(s.Namespace).List(ctx, metav1.ListOptions{LabelSelector: s.Selector})
			if err != nil {
				return nil, err
			}
			var gib int64
			for i := range pvcs.Items {
				p := &pvcs.Items[i]
				if s.NamePrefix != "" && !strings.HasPrefix(p.Name, s.NamePrefix) {
					continue
				}
				if p.Status.Phase != corev1.ClaimBound {
					continue
				}
				q := p.Status.Capacity.Storage()
				if q.IsZero() {
					q = p.Spec.Resources.Requests.Storage()
				}
				gib += (q.Value() + (1<<30 - 1)) >> 30
			}
			if gib > 0 {
				out = append(out, metering.Reading{Meter: s.Meter, Kind: kind, Unit: s.Unit, Value: gib})
			}
		default:
			return nil, fmt.Errorf("unknown meter source %q", s.Source)
		}
	}
	return out, nil
}
