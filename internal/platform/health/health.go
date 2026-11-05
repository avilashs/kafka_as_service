// Package health decides when a cluster is DEGRADED and when it is well again (design §17). Inputs are the
// canary's round trips through the public endpoint and the engine's reading of what the agent observed.
// A signal degrades a cluster only after it has lasted a configured time, so a restarting broker does not
// flap the state; a cluster being changed by an operation is left alone.
package health

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// Config holds the thresholds; the zero value takes the design's defaults.
type Config struct {
	Every time.Duration // how often the evaluator runs (15 s)
	// Window and MinAvailability: DEGRADED when canary availability over Window is below MinAvailability
	// (5 minutes, 99 %). MinAttempts guards against judging a handful of probes.
	Window          time.Duration
	MinAvailability float64
	MinAttempts     int
	// MinFailedAttempts: a window without a single success is judged from this many attempts (default 6, two rounds of a 3-broker cluster).
	MinFailedAttempts int
	// For maps an engine signal kind to how long it must last: brokerDown 5 minutes, underMinIsr 2 minutes.
	For map[string]time.Duration
	// StaleAfter: an observation older than this is not judged (the agent is not reporting; that is a different alarm).
	StaleAfter time.Duration
	// Now is the clock (tests set their own).
	Now func() time.Time
}

func (c *Config) defaults() {
	if c.Every == 0 {
		c.Every = 15 * time.Second
	}
	if c.Window == 0 {
		c.Window = 5 * time.Minute
	}
	if c.MinAvailability == 0 {
		c.MinAvailability = 0.99
	}
	if c.MinFailedAttempts == 0 {
		c.MinFailedAttempts = 6
	}
	if c.MinAttempts == 0 {
		c.MinAttempts = 20
	}
	if c.For == nil {
		c.For = map[string]time.Duration{"brokerDown": 5 * time.Minute, "underMinIsr": 2 * time.Minute}
	}
	if c.StaleAfter == 0 {
		c.StaleAfter = 5 * time.Minute
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Evaluator runs the rules.
type Evaluator struct {
	Store   *store.Store
	Engines *engine.Registry
	Cfg     Config
	Log     *slog.Logger
}

// Run evaluates until ctx is done.
func (e *Evaluator) Run(ctx context.Context) {
	e.Cfg.defaults()
	t := time.NewTicker(e.Cfg.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := e.EvaluateOnce(ctx); err != nil && ctx.Err() == nil {
				e.Log.Warn("health evaluation failed", "err", err)
			}
		}
	}
}

// EvaluateOnce judges every serving cluster once.
func (e *Evaluator) EvaluateOnce(ctx context.Context) error {
	e.Cfg.defaults()
	now := e.Cfg.Now()
	cands, err := e.Store.HealthCandidates(ctx)
	if err != nil {
		return err
	}
	for _, hc := range cands {
		c := hc.Cluster
		// No fresh observation: do not judge (and keep whatever state the cluster has).
		if hc.Observed == nil || now.Sub(hc.ReportedAt) > e.Cfg.StaleAfter {
			continue
		}
		active := map[string]string{}
		if eng, err := e.Engines.Get(c.Engine); err == nil {
			if h, ok := eng.(engine.Healther); ok {
				for _, s := range h.Health(c.ResourceSpec(), *hc.Observed) {
					active[s.Kind] = s.Detail
				}
			}
		}
		attempts, successes, _, err := e.Store.CanaryWindow(ctx, c.ID, now.Add(-e.Cfg.Window), now)
		if err != nil {
			return err
		}
		// Judged on enough probes, or on a few probes that all failed: when the path is cut a round takes as long
		// as its timeout, so the window may never hold MinAttempts of them (the kind run showed 15 attempts in 90 s).
		if attempts >= e.Cfg.MinAttempts || (successes == 0 && attempts >= e.Cfg.MinFailedAttempts) {
			if avail := float64(successes) / float64(attempts); avail < e.Cfg.MinAvailability {
				active["canary"] = fmt.Sprintf("canary availability %.1f%% over %s (%d of %d round trips)", avail*100, e.Cfg.Window, successes, attempts)
			}
		}
		signals, err := e.Store.SyncHealth(ctx, c.ID, now, active)
		if err != nil {
			return err
		}
		var reasons []string
		for _, s := range signals {
			if now.Sub(s.Since) >= e.Cfg.For[s.Kind] { // the canary has no entry: its window is its duration
				reasons = append(reasons, s.Detail)
			}
		}
		sort.Strings(reasons)
		switch {
		case len(reasons) > 0 && c.State == "ACTIVE":
			if ok, err := e.Store.SetHealthState(ctx, c.ID, "ACTIVE", "DEGRADED", strings.Join(reasons, "; ")); err != nil {
				return err
			} else if ok {
				e.Log.Warn("cluster degraded", "cluster", c.ID, "reasons", reasons)
			}
		case len(reasons) == 0 && c.State == "DEGRADED":
			if ok, err := e.Store.SetHealthState(ctx, c.ID, "DEGRADED", "ACTIVE", ""); err != nil {
				return err
			} else if ok {
				e.Log.Info("cluster healthy again", "cluster", c.ID)
			}
		}
	}
	return nil
}
