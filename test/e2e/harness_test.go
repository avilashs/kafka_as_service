//go:build e2e

// Package e2e runs the control plane, a real Temporal server, real PostgreSQL and
// a real kube-apiserver (envtest) with the Strimzi 1.3.0 CRDs installed.
//
// What is NOT real: there is no kubelet, scheduler or controller-manager in
// envtest, and the Strimzi operator is not running (its images are on quay.io,
// which is unreachable from the CI sandbox used for M1). opSim (sim_test.go)
// stands in for exactly two behaviours: it sets Kafka status Ready like Strimzi
// would, and it finalizes Terminating namespaces like the namespace controller
// would. Everything else — API, outbox, workflows, capacity, the agent's
// server-side apply against real CRD schemas, deletion — is the real code.
//
// Run: eval "$(hack/e2e-deps.sh up)"; go test -tags e2e ./test/e2e/ -v
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/client"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/avilashs/kafka_as_service/internal/agent"
	kindprov "github.com/avilashs/kafka_as_service/internal/cloud/kind"
	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/api"
	"github.com/avilashs/kafka_as_service/internal/platform/cellapi"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/outbox"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

var (
	repoRoot   string
	dsn        string
	temporalHP string
	restCfg    *rest.Config
	kube       kubernetes.Interface
	dyn        dynamic.Interface
	workerBin  string
	auditBin   string
	st         *store.Store
	secretsSt  secrets.Store // the secret manager every component of the test uses
	sim        *opSim
	logger     = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	base := os.Getenv("STREAMCELL_E2E_DATABASE_URL")
	temporalHP = os.Getenv("STREAMCELL_E2E_TEMPORAL")
	if base == "" || temporalHP == "" || os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("e2e: dependencies not configured; run: eval \"$(hack/e2e-deps.sh up)\"")
		return 1
	}
	ctx := context.Background()
	wd, _ := os.Getwd()
	repoRoot = filepath.Clean(filepath.Join(wd, "..", ".."))

	// Fresh database per run.
	dbName := "streamcell_e2e_" + randHex(4)
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		fmt.Println("e2e: postgres:", err)
		return 1
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		fmt.Println("e2e: create db:", err)
		return 1
	}
	defer func() {
		_ = admin.Close(ctx)
		if a2, err := pgx.Connect(ctx, base); err == nil {
			_, _ = a2.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
			_ = a2.Close(ctx)
		}
	}()
	u, _ := url.Parse(base)
	u.Path = "/" + dbName
	dsn = u.String()
	if st, err = store.Open(ctx, dsn); err != nil {
		fmt.Println("e2e: store:", err)
		return 1
	}
	defer st.Close()

	// Real kube-apiserver + etcd with the vendored Strimzi 1.3.0 CRDs.
	crds, _ := filepath.Glob(filepath.Join(repoRoot, "third_party/strimzi/1.3.0/04*-Crd-*.yaml"))
	testEnv := &envtest.Environment{
		CRDInstallOptions:     envtest.CRDInstallOptions{Paths: crds, ErrorIfPathMissing: true},
		BinaryAssetsDirectory: os.Getenv("KUBEBUILDER_ASSETS"),
	}
	if restCfg, err = testEnv.Start(); err != nil {
		fmt.Println("e2e: envtest:", err)
		return 1
	}
	defer func() { _ = testEnv.Stop() }()
	kube = kubernetes.NewForConfigOrDie(restCfg)
	dyn = dynamic.NewForConfigOrDie(restCfg)

	tmp, _ := os.MkdirTemp("", "streamcell-e2e")
	defer os.RemoveAll(tmp)

	// Secret manager: real OpenBao when configured (CI), a 0600-file tree otherwise. The worker
	// subprocess inherits these variables.
	if addr := os.Getenv("STREAMCELL_E2E_OPENBAO_ADDR"); addr != "" {
		os.Setenv("SECRETS_BACKEND", "openbao")
		os.Setenv("OPENBAO_ADDR", addr)
		os.Setenv("OPENBAO_TOKEN", os.Getenv("STREAMCELL_E2E_OPENBAO_TOKEN"))
	} else {
		os.Setenv("SECRETS_BACKEND", "file")
		os.Setenv("SECRETS_DIR", filepath.Join(tmp, "secrets"))
	}
	if secretsSt, err = secrets.FromEnv(); err != nil {
		fmt.Println("e2e: secrets:", err)
		return 1
	}

	// Worker binary with fault injection compiled in.
	workerBin = filepath.Join(tmp, "streamcell-worker")
	build := exec.Command("go", "build", "-tags", "faultinject", "-o", workerBin, "./cmd/streamcell-worker")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Println("e2e: build worker:", err, string(out))
		return 1
	}

	if err := installPolicies(); err != nil {
		fmt.Println("e2e: admission policies:", err)
		return 1
	}

	auditBin = filepath.Join(tmp, "streamcell-audit")
	buildAudit := exec.Command("go", "build", "-o", auditBin, "./cmd/streamcell-audit")
	buildAudit.Dir = repoRoot
	if out, err := buildAudit.CombinedOutput(); err != nil {
		fmt.Println("e2e: build audit:", err, string(out))
		return 1
	}

	simCtx, cancelSim := context.WithCancel(ctx)
	defer cancelSim()
	sim = &opSim{dyn: dyn, kube: kube}
	go sim.run(simCtx)

	return m.Run()
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// env is one test's isolated cell + tenant + servers + agent.
type env struct {
	t        *testing.T
	cellID   string
	tenant   string
	token    string
	apiURL   string
	cellURL  string
	agent    *agent.Agent
	stopAgnt context.CancelFunc
	temporal client.Client
	workers  []*worker
	tokens   *tokenMap
}

// tokenMap is an api.Authenticator whose tokens tests can add while the server runs.
type tokenMap struct {
	mu sync.RWMutex
	m  map[string]string
}

func (tm *tokenMap) set(tok, tenant string) { tm.mu.Lock(); tm.m[tok] = tenant; tm.mu.Unlock() }

// Tenant implements api.Authenticator.
func (tm *tokenMap) Tenant(r *http.Request) (string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return api.StaticTokens(tm.m).Tenant(r)
}

// otherTenant registers a second tenant in the same cell and returns its token.
func (e *env) otherTenant() string {
	e.t.Helper()
	id, tok := "tenant-"+randHex(3), "tok-"+randHex(8)
	if err := st.UpsertTenant(context.Background(), id, id, &e.cellID); err != nil {
		e.t.Fatal(err)
	}
	e.tokens.set(tok, id)
	return tok
}

// killWorkers stops every worker this env started.
func (e *env) killWorkers() {
	for _, w := range e.workers {
		w.kill()
	}
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	e := &env{t: t, cellID: "cell-" + randHex(3), tenant: "tenant-" + randHex(3), token: "tok-" + randHex(8), tokens: &tokenMap{m: map[string]string{}}}
	e.tokens.set(e.token, e.tenant)
	cellTok := "celltok-" + randHex(8)
	zones := []string{"zone-a", "zone-b", "zone-c"}
	zc := func(cpu, mem int64, nodes int) []store.ZoneCapacity {
		var out []store.ZoneCapacity
		for _, z := range zones {
			out = append(out, store.ZoneCapacity{Zone: z, CPUMilli: cpu, MemoryMiB: mem, EBSMBps: 4000, Attach: 64, Nodes: nodes})
		}
		return out
	}
	err := st.UpsertCell(ctx, store.CellSpec{
		ID: e.cellID, Region: "local", Cloud: "kind", State: "ACTIVE", OperatorShards: 2, AgentToken: cellTok,
		Env: engine.CellEnv{Domain: "streams.example.net", Zones: zones, StorageClass: "standard",
			GatewayName: "kafka-gateway", GatewayNS: "streamcell-gateway", APIServerCIDRs: []string{"10.96.0.1/32"},
			EngineImages: map[string]string{"4.3.0": "registry.example/kafka:4.3.0", "4.3.1": "registry.example/kafka:4.3.1"}},
		Pools: []store.PoolSpec{
			{ID: e.cellID + "-control", Kind: "control", InstanceType: "kind", MaxNodes: 3, Zones: zc(64000, 262144, 1)},
			{ID: e.cellID + "-shared", Kind: "shared", InstanceType: "kind", MaxNodes: 3, Zones: zc(64000, 262144, 1)},
			// Dedicated budget: one whole node per zone of instance type "kind", each 4 CPU / 8 GiB.
			{ID: e.cellID + "-dedicated", Kind: "dedicated", InstanceType: "kind", MaxNodes: 3, NodeCPUMilli: 4000, NodeMemoryMiB: 8192, Zones: zc(0, 0, 1)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertTenant(ctx, e.tenant, e.tenant, &e.cellID); err != nil {
		t.Fatal(err)
	}

	tc, err := client.Dial(client.Options{HostPort: temporalHP, Logger: quietTemporal{}})
	if err != nil {
		t.Fatal(err)
	}
	e.temporal = tc
	reg := engine.NewRegistry(kafka.New())
	apiSrv := httptest.NewServer((&api.Server{Store: st, Engines: reg, Secrets: secretsSt, Auth: e.tokens, Log: logger}).Handler())
	cellSrv := httptest.NewServer((&cellapi.Server{Store: st, Log: logger, MaxWait: 2 * time.Second, PollEvery: 100 * time.Millisecond}).Handler())
	e.apiURL, e.cellURL = apiSrv.URL, cellSrv.URL
	dctx, stopDispatch := context.WithCancel(ctx)
	go (&outbox.Dispatcher{Store: st, Temporal: tc, Interval: 100 * time.Millisecond, Log: logger}).Run(dctx)

	stateNS := "agent-" + e.cellID
	if _, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: stateNS}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	e.startAgent(cellTok, stateNS)
	t.Cleanup(func() {
		e.stopAgnt()
		stopDispatch()
		apiSrv.Close()
		cellSrv.Close()
		tc.Close()
	})
	return e
}

func (e *env) startAgent(cellTok, stateNS string) {
	ag, err := agent.New(restCfg, &agent.CPClient{BaseURL: e.cellURL, Token: cellTok},
		agent.Config{StateNamespace: stateNS, PollWait: 2 * time.Second, StatusEvery: 300 * time.Millisecond,
			HeartbeatEvery: time.Second, ResyncEvery: 5 * time.Second}, logger)
	if err != nil {
		e.t.Fatal(err)
	}
	ag.Secrets = secretsSt
	ag.Pools = &kindprov.Provider{Kube: kube, SpareSelector: "test-cell=" + e.cellID}
	ctx, cancel := context.WithCancel(context.Background())
	e.agent, e.stopAgnt = ag, cancel
	go func() { _ = ag.Run(ctx) }()
}

// worker is a streamcell-worker subprocess.
type worker struct {
	cmd  *exec.Cmd
	log  *bytes.Buffer
	done chan struct{}
}

func (e *env) startWorker(extraEnv ...string) *worker {
	e.t.Helper()
	cmd := exec.Command(workerBin)
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dsn, "TEMPORAL_ADDRESS="+temporalHP,
		"STREAMCELL_POLL_INTERVAL=200ms", "STREAMCELL_ACTIVITY_TIMEOUT=4s", "STREAMCELL_HEARTBEAT_TIMEOUT=4s",
		"STREAMCELL_READY_TIMEOUT=2m", "STREAMCELL_DELETED_TIMEOUT=2m")
	cmd.Env = append(cmd.Env, extraEnv...)
	buf := &bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = buf, buf
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	w := &worker{cmd: cmd, log: buf, done: make(chan struct{})}
	e.workers = append(e.workers, w)
	go func() { _ = cmd.Wait(); close(w.done) }()
	e.t.Cleanup(func() {
		w.kill()
		if e.t.Failed() {
			e.t.Logf("worker log:\n%s", w.log.String())
		}
	})
	return w
}

func (w *worker) kill() {
	select {
	case <-w.done:
	default:
		_ = w.cmd.Process.Signal(syscall.SIGKILL)
		<-w.done
	}
}

func (w *worker) exitCode() int {
	<-w.done
	return w.cmd.ProcessState.ExitCode()
}

func (w *worker) waitExit(t *testing.T, d time.Duration) int {
	t.Helper()
	select {
	case <-w.done:
		return w.cmd.ProcessState.ExitCode()
	case <-time.After(d):
		t.Fatalf("worker did not exit within %s; log:\n%s", d, w.log.String())
		return -1
	}
}

// ---------- API helpers ----------

type apiResp struct {
	Code   int
	Header http.Header
	Body   map[string]any
	Raw    string
}

func (e *env) call(method, path, idemKey string, body any) apiResp {
	e.t.Helper()
	return e.callAs(e.token, method, path, idemKey, body)
}

// callAs is call with another tenant's token.
func (e *env) callAs(token, method, path, idemKey string, body any) apiResp {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.apiURL+path, rd)
	req.Header.Set("Authorization", "Bearer "+token)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	out := apiResp{Code: resp.StatusCode, Header: resp.Header}
	raw, _ := io.ReadAll(resp.Body)
	out.Raw = string(raw)
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

func createBody(name string) map[string]any {
	return map[string]any{"name": name, "tier": "shared", "spec": map[string]any{
		"kafkaVersion": "4.3.1", "brokerType": "s.small", "brokerCount": 3,
		"storage": map[string]any{"sizeGiB": 100, "maxSizeGiB": 400, "autoExpand": true},
	}}
}

func (e *env) createCluster(name, key string) (opID, clusterID string) {
	e.t.Helper()
	r := e.call("POST", "/v1/clusters", key, createBody(name))
	if r.Code != http.StatusAccepted {
		e.t.Fatalf("create: %d %v", r.Code, r.Body)
	}
	return r.Body["id"].(string), r.Body["clusterId"].(string)
}

func (e *env) waitOp(opID, want string, d time.Duration) map[string]any {
	e.t.Helper()
	deadline := time.Now().Add(d)
	var last map[string]any
	for time.Now().Before(deadline) {
		r := e.call("GET", "/v1/operations/"+opID, "", nil)
		last = r.Body
		if s, _ := r.Body["state"].(string); s == want {
			return r.Body
		} else if s == "FAILED" || s == "SUCCEEDED" {
			e.t.Fatalf("operation %s ended %s, want %s: %v", opID, s, want, r.Body)
		}
		time.Sleep(200 * time.Millisecond)
	}
	e.t.Fatalf("operation %s not %s within %s; last: %v", opID, want, d, last)
	return nil
}

func (e *env) waitStep(opID, step string, d time.Duration) {
	e.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		r := e.call("GET", "/v1/operations/"+opID, "", nil)
		if s, _ := r.Body["step"].(string); s == step {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("operation %s never reached step %s", opID, step)
}

func countRows(t *testing.T, q string, args ...any) int {
	t.Helper()
	var n int
	if err := st.Pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

type quietTemporal struct{}

func (quietTemporal) Debug(string, ...any) {}
func (quietTemporal) Info(string, ...any)  {}
func (quietTemporal) Warn(string, ...any)  {}
func (quietTemporal) Error(string, ...any) {}
