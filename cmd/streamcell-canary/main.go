// Command streamcell-canary runs in each cell: it probes every serving cluster through its public endpoint
// and reports round trips to the control plane (design §17). It makes outbound connections only.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/avilashs/kafka_as_service/internal/agent"
	"github.com/avilashs/kafka_as_service/internal/canary"
	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
)

func dur(env string) time.Duration {
	d, _ := time.ParseDuration(os.Getenv(env))
	return d
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	sec, err := secrets.FromEnv()
	if err != nil {
		log.Error("secrets", "err", err)
		os.Exit(1)
	}
	cp := &agent.CPClient{BaseURL: os.Getenv("CONTROL_PLANE_URL"), Token: os.Getenv("CELL_TOKEN")}
	cell := os.Getenv("CELL_ID")
	if cell == "" {
		log.Error("CELL_ID is required")
		os.Exit(1)
	}
	p := &canary.Prober{
		Log:  log,
		User: kafka.PlatformCanaryUser,
		Targets: func(ctx context.Context) ([]canary.Target, error) {
			ts, err := cp.CanaryTargets(ctx)
			out := make([]canary.Target, 0, len(ts))
			for _, t := range ts {
				out = append(out, canary.Target{ClusterID: t.ClusterID, Bootstrap: t.Bootstrap, CAPEM: t.CAPEM, Partitions: t.Partitions})
			}
			return out, err
		},
		Password: func(ctx context.Context, clusterID string) (string, error) {
			data, _, err := sec.Get(ctx, secrets.UserPath(cell, clusterID, kafka.PlatformCanaryUser))
			if err != nil {
				return "", err
			}
			return data["password"], nil
		},
		Report:       cp.CanaryReport,
		DialAddr:     os.Getenv("CANARY_DIAL"), // set where the endpoint names resolve to a local relay (kind)
		RoundEvery:   dur("CANARY_ROUND_EVERY"),
		RoundTimeout: dur("CANARY_ROUND_TIMEOUT"),
		ReportEvery:  dur("CANARY_REPORT_EVERY"),
		Warmup:       dur("CANARY_WARMUP"),
	}
	log.Info("canary starting", "cell", cell)
	p.Run(ctx)
}
