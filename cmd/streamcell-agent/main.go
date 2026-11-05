// Command streamcell-agent runs in each cell. It only makes outbound connections.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/avilashs/kafka_as_service/internal/agent"
	awsprov "github.com/avilashs/kafka_as_service/internal/cloud/aws"
	kindprov "github.com/avilashs/kafka_as_service/internal/cloud/kind"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rc, err := rest.InClusterConfig()
	if err != nil {
		rc, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	}
	if err != nil {
		log.Error("kubeconfig", "err", err)
		os.Exit(1)
	}
	cfg := agent.Config{StateNamespace: os.Getenv("STATE_NAMESPACE")}
	if d, err := time.ParseDuration(os.Getenv("STATUS_EVERY")); err == nil {
		cfg.StatusEvery = d
	}
	if d, err := time.ParseDuration(os.Getenv("POLL_WAIT")); err == nil {
		cfg.PollWait = d
	}
	a, err := agent.New(rc, &agent.CPClient{BaseURL: os.Getenv("CONTROL_PLANE_URL"), Token: os.Getenv("CELL_TOKEN")}, cfg, log)
	if err != nil {
		log.Error("agent", "err", err)
		os.Exit(1)
	}
	if a.Secrets, err = secrets.FromEnv(); err != nil {
		log.Error("secrets", "err", err)
		os.Exit(1)
	}
	if dir := os.Getenv("METERING_DIR"); dir != "" {
		mc := &agent.MeteringConfig{Dir: dir, Cell: os.Getenv("CELL_ID")}
		if mc.Cell == "" {
			log.Error("METERING_DIR needs CELL_ID")
			os.Exit(1)
		}
		if d, err := time.ParseDuration(os.Getenv("METERING_SAMPLE_EVERY")); err == nil {
			mc.SampleEvery = d
		}
		if d, err := time.ParseDuration(os.Getenv("METERING_INTERVAL")); err == nil {
			mc.Interval = d
		}
		a.Metering = mc
	}
	switch prov := os.Getenv("NODEPOOL_PROVIDER"); prov {
	case "kind":
		a.Pools = &kindprov.Provider{Kube: a.Kube, Log: log}
	case "karpenter":
		a.Pools = &awsprov.Karpenter{Dyn: a.Dyn, NodeClassName: os.Getenv("KARPENTER_NODECLASS")}
	case "", "none":
	default:
		log.Error("unknown NODEPOOL_PROVIDER", "value", prov)
		os.Exit(1)
	}
	if err := a.Run(ctx); err != nil {
		log.Error("agent", "err", err)
		os.Exit(1)
	}
}
