// Command streamcell-worker runs the Temporal workflow worker.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/capacity"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
	"github.com/avilashs/kafka_as_service/internal/platform/workflows"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	st, err := store.Open(ctx, env("DATABASE_URL", "postgres://localhost/streamcell"))
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	tc, err := client.Dial(client.Options{HostPort: env("TEMPORAL_ADDRESS", "localhost:7233"), Namespace: env("TEMPORAL_NAMESPACE", "default")})
	if err != nil {
		log.Error("temporal", "err", err)
		os.Exit(1)
	}
	defer tc.Close()
	if d, err := time.ParseDuration(os.Getenv("STREAMCELL_READY_TIMEOUT")); err == nil {
		workflows.ReadyTimeout = d
	}
	if d, err := time.ParseDuration(os.Getenv("STREAMCELL_DELETED_TIMEOUT")); err == nil {
		workflows.DeletedTimeout = d
	}
	if d, err := time.ParseDuration(os.Getenv("STREAMCELL_ACTIVITY_TIMEOUT")); err == nil {
		workflows.ShortActivityTimeout = d
	}
	if d, err := time.ParseDuration(os.Getenv("STREAMCELL_HEARTBEAT_TIMEOUT")); err == nil {
		workflows.WaitHeartbeatTimeout = d
	}
	poll := 2 * time.Second
	if d, err := time.ParseDuration(os.Getenv("STREAMCELL_POLL_INTERVAL")); err == nil {
		poll = d
	}
	w := worker.New(tc, workflows.TaskQueue, worker.Options{})
	workflows.Register(w, &workflows.Activities{
		Store: st, Allocator: &capacity.Allocator{Store: st}, Engines: engine.NewRegistry(kafka.New()), PollInterval: poll,
	})
	log.Info("worker starting", "taskQueue", workflows.TaskQueue)
	if err := w.Run(interruptCh(ctx)); err != nil {
		log.Error("worker", "err", err)
		os.Exit(1)
	}
}

func interruptCh(ctx context.Context) <-chan interface{} {
	ch := make(chan interface{})
	go func() { <-ctx.Done(); close(ch) }()
	return ch
}
