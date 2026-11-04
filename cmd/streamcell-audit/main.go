// Command streamcell-audit checks the placement invariants against the actual pod
// and node state of a cell.
//
//	streamcell-audit                      one audit; prints violations; exit 0 clean, 1 violations, 2 error
//	streamcell-audit --serve :9102        continuous auditor: exposes /metrics and /healthz
//
// It is used as the placement-auditor Deployment in every cell and by the test suites.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/avilashs/kafka_as_service/internal/placement"
)

func main() { os.Exit(run()) }

func run() int {
	var (
		serve    = flag.String("serve", "", "listen address for the continuous auditor, e.g. :9102 (empty: audit once and exit)")
		interval = flag.Duration("interval", 30*time.Second, "audit interval in --serve mode")
		surge    = flag.Int("surge", 1, "dedicated nodes a resource may hold beyond its pod count (rotation surge)")
		nodeSel  = flag.String("node-selector", "", "only audit nodes matching this label selector (tests)")
		output   = flag.String("output", "text", "text or json")
		kubecfg  = flag.String("kubeconfig", "", "path to a kubeconfig (default: in-cluster, then $KUBECONFIG, then ~/.kube/config)")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	var rc *rest.Config
	var err error
	if *kubecfg != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", *kubecfg)
	} else if rc, err = rest.InClusterConfig(); err != nil {
		rc, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kubeconfig:", err)
		return 2
	}
	kube, err := kubernetes.NewForConfig(rc)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	opt := placement.CollectOptions{NodeSelector: *nodeSel}
	check := placement.Options{Surge: *surge}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *serve != "" {
		m := placement.NewMetrics()
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
		srv := &http.Server{Addr: *serve, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("http server", "err", err)
				stop()
			}
		}()
		placement.Run(ctx, kube, opt, check, *interval, m, log)
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
		return 0
	}

	snap, err := placement.Collect(ctx, kube, opt)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	vs := placement.Check(snap, check)
	if *output == "json" {
		if vs == nil {
			vs = []placement.Violation{}
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"violations": vs, "nodes": len(snap.Nodes), "pods": len(snap.Pods)})
	} else {
		for _, v := range vs {
			fmt.Println(v)
		}
		fmt.Fprintf(os.Stderr, "audited %d nodes, %d pods: %d violation(s)\n", len(snap.Nodes), len(snap.Pods), len(vs))
	}
	if len(vs) > 0 {
		return 1
	}
	return 0
}
