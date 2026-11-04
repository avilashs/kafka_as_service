package placement

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/kubernetes"
)

// Metrics exposes audit results.
type Metrics struct {
	Registry    *prometheus.Registry
	violations  *prometheus.GaugeVec
	lastRun     prometheus.Gauge
	lastSuccess prometheus.Gauge
	errors      prometheus.Counter
}

// NewMetrics creates the audit metrics in a fresh registry.
func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		violations: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamcell_placement_violations",
			Help: "Number of placement violations currently found in the cell, by rule, resource and node.",
		}, []string{"rule", "cluster", "node"}),
		lastRun: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streamcell_placement_audit_last_run_timestamp_seconds", Help: "Unix time of the last audit attempt."}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streamcell_placement_audit_last_success_timestamp_seconds", Help: "Unix time of the last audit that completed."}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamcell_placement_audit_errors_total", Help: "Audits that failed to read the cell."}),
	}
	m.Registry.MustRegister(m.violations, m.lastRun, m.lastSuccess, m.errors)
	return m
}

// Record replaces the exported violation series with vs.
func (m *Metrics) Record(vs []Violation) {
	m.violations.Reset()
	for _, v := range vs {
		m.violations.WithLabelValues(v.Rule, v.Cluster, v.Node).Inc()
	}
	now := float64(time.Now().Unix())
	m.lastRun.Set(now)
	m.lastSuccess.Set(now)
}

// Failed records an audit that could not read the cell. The previous violation
// series are left in place (stale-but-visible beats silently cleared); the
// last-success timestamp stops advancing, which the StaleAudit alert watches.
func (m *Metrics) Failed() {
	m.lastRun.Set(float64(time.Now().Unix()))
	m.errors.Inc()
}

// Run audits every interval until ctx is cancelled.
func Run(ctx context.Context, kube kubernetes.Interface, opt CollectOptions, check Options, interval time.Duration, m *Metrics, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		snap, err := Collect(ctx, kube, opt)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("placement audit failed", "err", err)
			m.Failed()
		} else {
			vs := Check(snap, check)
			m.Record(vs)
			for _, v := range vs {
				log.Warn("placement violation", "rule", v.Rule, "cluster", v.Cluster, "node", v.Node, "pods", v.Pods, "detail", v.Detail)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
