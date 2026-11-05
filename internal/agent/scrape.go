package agent

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// series is one sample of a Prometheus text exposition.
type series struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// parseProm reads the Prometheus text format (counters and gauges; comments, HELP and TYPE lines are skipped).
func parseProm(text string) []series {
	var out []series
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		name, rest := line, ""
		labels := map[string]string{}
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name, rest = line[:i], line[i:]
		}
		if strings.HasPrefix(rest, "{") {
			j, in, esc := 1, false, false
			for ; j < len(rest); j++ {
				c := rest[j]
				if esc {
					esc = false
				} else if c == '\\' {
					esc = true
				} else if c == '"' {
					in = !in
				} else if c == '}' && !in {
					break
				}
			}
			if j >= len(rest) {
				continue
			}
			labels = parseLabels(rest[1:j])
			rest = rest[j+1:]
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(f[0], 64)
		if err != nil || math.IsNaN(v) {
			continue
		}
		out = append(out, series{Name: name, Labels: labels, Value: v})
	}
	return out
}

func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for len(s) > 0 {
		eq := strings.Index(s, "=")
		if eq < 0 {
			break
		}
		k := strings.TrimSpace(strings.TrimPrefix(s[:eq], ","))
		s = s[eq+1:]
		if !strings.HasPrefix(s, `"`) {
			break
		}
		var v strings.Builder
		i, esc := 1, false
		for ; i < len(s); i++ {
			c := s[i]
			if esc {
				v.WriteByte(c)
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				break
			} else {
				v.WriteByte(c)
			}
		}
		out[k] = v.String()
		if i+1 >= len(s) {
			break
		}
		s = s[i+1:]
	}
	return out
}

// scrapePod fetches a pod's metrics through the API server's pod proxy: the agent needs no network path to
// the pod, and the tenant namespace's NetworkPolicy has no hole for it.
func (a *Agent) scrapePod(ctx context.Context, ns, pod string, port int, path string) ([]series, error) {
	cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	raw, err := a.Kube.CoreV1().Pods(ns).ProxyGet("http", pod, strconv.Itoa(port), strings.TrimPrefix(path, "/"), nil).DoRaw(cctx)
	if err != nil {
		return nil, err
	}
	return parseProm(string(raw)), nil
}

func (a *Agent) runningPods(ctx context.Context, ns, selector string) ([]corev1.Pod, error) {
	pods, err := a.Kube.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
			out = append(out, p)
		}
	}
	return out, nil
}

// metricValue is the value of a metric in one scrape: the series without a topic label (the aggregate)
// minus the series of excluded topics. When the exposition has no aggregate series the plain sum is used.
func metricValue(ss []series, metric string, excludeTopics []string) (float64, bool) {
	var agg, all, excluded float64
	haveAgg, have := false, false
	skip := map[string]bool{}
	for _, t := range excludeTopics {
		skip[t] = true
	}
	for _, s := range ss {
		if s.Name != metric {
			continue
		}
		have = true
		topic, hasTopic := s.Labels["topic"]
		switch {
		case !hasTopic:
			agg += s.Value
			haveAgg = true
		case skip[topic]:
			excluded += s.Value
			all += s.Value
		default:
			all += s.Value
		}
	}
	if !have {
		return 0, false
	}
	if haveAgg {
		return agg - excluded, true
	}
	return all - excluded, true
}

// probeMetrics evaluates the bundle's probes (gauges such as under-min-ISR partitions) for health.
func (a *Agent) probeMetrics(ctx context.Context, probes []engine.ProbeSource) map[string]any {
	if len(probes) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, pr := range probes {
		pods, err := a.runningPods(ctx, pr.Namespace, pr.Selector)
		if err != nil {
			continue
		}
		var vals []float64
		for _, p := range pods {
			ss, err := a.scrapePod(ctx, pr.Namespace, p.Name, pr.Port, pr.Path)
			if err != nil {
				continue
			}
			if v, ok := metricValue(ss, pr.Metric, nil); ok {
				vals = append(vals, v)
			}
		}
		if len(vals) == 0 {
			continue // no answer is not a zero
		}
		sort.Float64s(vals)
		switch pr.Agg {
		case "max":
			out[pr.Name] = vals[len(vals)-1]
		default:
			var sum float64
			for _, v := range vals {
				sum += v
			}
			out[pr.Name] = sum
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
