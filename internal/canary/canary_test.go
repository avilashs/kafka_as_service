//go:build e2e

package canary

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// The canary against a real Kafka broker (docker): it must succeed while the broker is healthy, report
// failures while the broker is frozen (an injected outage on the path, not a clean shutdown), and succeed
// again after recovery.
func TestCanaryDetectsAnInjectedOutage(t *testing.T) {
	image := os.Getenv("STREAMCELL_E2E_KAFKA_IMAGE")
	if image == "" {
		image = "mirror.gcr.io/apache/kafka:4.1.0"
	}
	name := "streamcell-canary-test"
	_ = exec.Command("docker", "rm", "-f", name).Run()
	if out, err := exec.Command("docker", "run", "-d", "--name", name, "--network", "host", image).CombinedOutput(); err != nil {
		t.Skipf("docker not available: %v %s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "unpause", name).Run()
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})

	var mu sync.Mutex
	var reports []store.CanaryResult
	newProber := func() *Prober {
		return &Prober{
			Targets: func(context.Context) ([]Target, error) {
				return []Target{{ClusterID: "kc-test", Bootstrap: "localhost:9092", Partitions: 2}}, nil
			},
			Password: func(context.Context, string) (string, error) { return "", nil },
			Report: func(_ context.Context, rs []store.CanaryResult) error {
				mu.Lock()
				reports = append(reports, rs...)
				mu.Unlock()
				return nil
			},
			Log:               slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
			ReconnectEvery: 3 * time.Second, ReplicationFactor: 1, MinISR: 1, RoundEvery: 500 * time.Millisecond, RoundTimeout: 2 * time.Second, ReportEvery: 2 * time.Second, Warmup: time.Second,
		}
	}
	p := newProber()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	waitFor := func(what string, d time.Duration, f func(rs []store.CanaryResult) bool) {
		t.Helper()
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			mu.Lock()
			cp := append([]store.CanaryResult(nil), reports...)
			mu.Unlock()
			if f(cp) {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("timed out waiting for %s; reports: %+v", what, reports)
	}
	mark := func() int { mu.Lock(); defer mu.Unlock(); return len(reports) }
	after := func(from int, pred func(r store.CanaryResult) bool) func([]store.CanaryResult) bool {
		return func(rs []store.CanaryResult) bool {
			for _, r := range rs[min(from, len(rs)):] {
				if pred(r) {
					return true
				}
			}
			return false
		}
	}

	waitFor("healthy round trips", 120*time.Second, after(0, func(r store.CanaryResult) bool { return r.Attempts >= 4 && r.Successes == r.Attempts }))

	// outage: freeze the broker (its sockets stay open, nothing answers)
	m := mark()
	if out, err := exec.Command("docker", "pause", name).CombinedOutput(); err != nil {
		t.Fatalf("pause: %v %s", err, out)
	}
	waitFor("failed round trips while the broker is frozen", 60*time.Second, after(m, func(r store.CanaryResult) bool { return r.Attempts >= 2 && r.Successes == 0 }))

	// recovery
	m = mark()
	if out, err := exec.Command("docker", "unpause", name).CombinedOutput(); err != nil {
		t.Fatalf("unpause: %v %s", err, out)
	}
	waitFor("healthy round trips after recovery", 120*time.Second, after(m, func(r store.CanaryResult) bool { return r.Attempts >= 4 && r.Successes == r.Attempts }))

	// A canary that starts (or reconnects) against a cluster whose canary topic already exists must not take
	// that for a failure. Found on the kind cell: kadm returns TOPIC_ALREADY_EXISTS as the call's error, so
	// every round after the first reconnect failed, for ever.
	cancel()
	time.Sleep(time.Second)
	mu.Lock()
	reports = nil
	mu.Unlock()
	p2 := newProber()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go p2.Run(ctx2)
	waitFor("healthy round trips from a second canary on the existing topic", 60*time.Second, after(0, func(r store.CanaryResult) bool { return r.Attempts >= 4 && r.Successes == r.Attempts }))
}
