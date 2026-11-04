//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/avilashs/kafka_as_service/internal/agent"
	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/cellapi"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// fakeCP is a scriptable control plane for agent safety tests.
type fakeCP struct {
	mu      sync.Mutex
	down    bool
	rev     int64
	entries []store.DesiredEntry
	srv     *httptest.Server
	reports atomic.Int64
}

func newFakeCP(t *testing.T) *fakeCP {
	f := &fakeCP{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.down {
			http.Error(w, "control plane down", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/cell/v1/desired":
			var since int64
			fmt.Sscan(r.URL.Query().Get("since"), &since)
			out := []store.DesiredEntry{}
			for _, e := range f.entries {
				if e.CellRevision > since {
					out = append(out, e)
				}
			}
			_ = json.NewEncoder(w).Encode(cellapi.DesiredResponse{CellID: "fake", CurrentRevision: f.rev, Entries: out})
		case "/cell/v1/status":
			f.reports.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCP) set(fn func(f *fakeCP)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func bundleFor(t *testing.T, clusterID string) *engine.Bundle {
	t.Helper()
	raw, _ := json.Marshal(kafka.Spec{KafkaVersion: "4.3.1", BrokerType: "s.small", BrokerCount: 3, Storage: kafka.StorageSpec{SizeGiB: 10}})
	b, err := kafka.New().Render(engine.ResourceSpec{ID: clusterID, Name: "x", Tenant: "t", Engine: "kafka", Tier: engine.TierShared, Spec: raw},
		engine.Assignment{CellID: "fake", Zones: []string{"a", "b", "c"}}, engine.CellEnv{CellID: "fake", Domain: "streams.example.net", GatewayName: "gw", GatewayNS: "gw-ns", EngineImages: map[string]string{"4.3.1": "registry.example/kafka:4.3.1"}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func startTestAgent(t *testing.T, f *fakeCP, stateNS string) (*agent.Agent, context.CancelFunc) {
	t.Helper()
	ag, err := agent.New(restCfg, &agent.CPClient{BaseURL: f.srv.URL, Token: "x"}, agent.Config{
		StateNamespace: stateNS, PollWait: 300 * time.Millisecond, StatusEvery: 200 * time.Millisecond,
		HeartbeatEvery: time.Second, ResyncEvery: time.Second, MassDeleteLimit: 5}, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = ag.Run(ctx) }()
	return ag, cancel
}

func newStateNS(t *testing.T) string {
	ns := "agent-test-" + randHex(3)
	if _, err := kube.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	return ns
}

func kafkaExists(id string) bool {
	_, err := dyn.Resource(kafkaGVR).Namespace(id).Get(context.Background(), id, metav1.GetOptions{})
	return err == nil
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func consistently(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("condition violated: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Data plane keeps running while the control plane is down, and after an agent
// restart during the outage; an empty desired list never deletes anything.
func TestAgentControlPlaneDownKeepsDataPlane(t *testing.T) {
	f := newFakeCP(t)
	id := "kc-" + randHex(5)
	f.set(func(f *fakeCP) {
		f.rev = 1
		f.entries = []store.DesiredEntry{{ClusterID: id, Generation: 1, CellRevision: 1, BundleDigest: "d1", Bundle: bundleFor(t, id)}}
	})
	stateNS := newStateNS(t)
	_, stop := startTestAgent(t, f, stateNS)
	eventually(t, 10*time.Second, "cluster applied", func() bool { return kafkaExists(id) })

	f.set(func(f *fakeCP) { f.down = true })
	consistently(t, 3*time.Second, "objects survive control-plane outage", func() bool { return kafkaExists(id) })

	stop() // agent restarts while the control plane is still down
	_, stop2 := startTestAgent(t, f, stateNS)
	defer stop2()
	consistently(t, 3*time.Second, "objects survive agent restart during outage", func() bool { return kafkaExists(id) })

	// Control plane returns but (e.g. buggy) serves no entries: absence is not deletion.
	f.set(func(f *fakeCP) { f.down = false; f.entries = nil })
	consistently(t, 3*time.Second, "empty desired list deletes nothing", func() bool { return kafkaExists(id) })
}

// A control plane whose revision went backwards (e.g. database restore) pauses the agent.
func TestAgentRevisionRegressionPauses(t *testing.T) {
	f := newFakeCP(t)
	id := "kc-" + randHex(5)
	f.set(func(f *fakeCP) {
		f.rev = 5
		f.entries = []store.DesiredEntry{{ClusterID: id, Generation: 1, CellRevision: 5, BundleDigest: "d1", Bundle: bundleFor(t, id)}}
	})
	stateNS := newStateNS(t)
	_, stop := startTestAgent(t, f, stateNS)
	eventually(t, 10*time.Second, "cluster applied", func() bool { return kafkaExists(id) })
	stop()

	// "Restored" control plane: revision 2, and it serves a tombstone for the cluster.
	f.set(func(f *fakeCP) {
		f.rev = 2
		f.entries = []store.DesiredEntry{{ClusterID: id, Generation: 2, CellRevision: 2, Tombstone: true, BundleDigest: "t"}}
	})
	ag, stop2 := startTestAgent(t, f, stateNS)
	defer stop2()
	eventually(t, 5*time.Second, "agent paused", func() bool { return strings.Contains(ag.Paused(), "lower than last applied") })
	consistently(t, 2*time.Second, "nothing deleted while paused", func() bool { return kafkaExists(id) })
}

// A poll that would delete many live clusters at once is held until an operator confirms.
func TestAgentMassDeleteBrake(t *testing.T) {
	f := newFakeCP(t)
	var ids []string
	var entries []store.DesiredEntry
	for i := 0; i < 7; i++ {
		id := "kc-" + randHex(5)
		ids = append(ids, id)
		entries = append(entries, store.DesiredEntry{ClusterID: id, Generation: 1, CellRevision: int64(i + 1), BundleDigest: "d", Bundle: bundleFor(t, id)})
	}
	f.set(func(f *fakeCP) { f.rev = 7; f.entries = entries })
	stateNS := newStateNS(t)
	ag, stop := startTestAgent(t, f, stateNS)
	defer stop()
	eventually(t, 20*time.Second, "7 clusters applied", func() bool {
		for _, id := range ids {
			if !kafkaExists(id) {
				return false
			}
		}
		return true
	})
	var tombs []store.DesiredEntry
	for i, id := range ids {
		b := bundleFor(t, id) // tombstones carry refs, as PublishTombstone does
		tombs = append(tombs, store.DesiredEntry{ClusterID: id, Generation: 2, CellRevision: int64(8 + i), Tombstone: true, BundleDigest: "t", Bundle: b})
	}
	f.set(func(f *fakeCP) { f.rev = 14; f.entries = append(entries, tombs...) })
	eventually(t, 5*time.Second, "mass delete held", func() bool { return strings.Contains(ag.Paused(), "would delete 7 of 7") })
	consistently(t, 2*time.Second, "nothing deleted while held", func() bool { return kafkaExists(ids[0]) && kafkaExists(ids[6]) })

	// Operator confirms the exact revision.
	cm, err := kube.CoreV1().ConfigMaps(stateNS).Get(context.Background(), "streamcell-agent-state", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Annotations == nil {
		cm.Annotations = map[string]string{}
	}
	cm.Annotations["streamcell.dev/allow-mass-delete"] = "14"
	if _, err := kube.CoreV1().ConfigMaps(stateNS).Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "all namespaces deleted after confirmation", func() bool {
		for _, id := range ids {
			if _, err := kube.CoreV1().Namespaces().Get(context.Background(), id, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				return false
			}
		}
		return true
	})
}
