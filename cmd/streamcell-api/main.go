// Command streamcell-api serves the tenant REST API and the cell-agent API on
// separate listeners, and runs the outbox dispatcher and reservation sweeper.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/avilashs/kafka_as_service/internal/engines/kafka"
	"github.com/avilashs/kafka_as_service/internal/platform/api"
	"github.com/avilashs/kafka_as_service/internal/platform/capacity"
	"github.com/avilashs/kafka_as_service/internal/platform/cellapi"
	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/outbox"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
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
	tc, err := client.Dial(client.Options{HostPort: env("TEMPORAL_ADDRESS", "localhost:7233"), Namespace: env("TEMPORAL_NAMESPACE", "default"), Logger: nopTemporalLogger{}})
	if err != nil {
		log.Error("temporal", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	tokens := api.StaticTokens{}
	for _, pair := range strings.Split(os.Getenv("STREAMCELL_DEV_TOKENS"), ",") { // token:tenant,...
		if tok, tenant, ok := strings.Cut(strings.TrimSpace(pair), ":"); ok {
			tokens[tok] = tenant
		}
	}
	sec, err := secrets.FromEnv()
	if err != nil {
		log.Error("secrets", "err", err)
		os.Exit(1)
	}
	reg := engine.NewRegistry(kafka.New())
	tenantSrv := &http.Server{Addr: env("LISTEN", ":8080"), Handler: (&api.Server{Store: st, Engines: reg, Secrets: sec, Auth: tokens, Log: log}).Handler(), ReadHeaderTimeout: 10 * time.Second}
	cellSrv := &http.Server{Addr: env("CELL_LISTEN", ":8081"), Handler: (&cellapi.Server{Store: st, Log: log}).Handler(), ReadHeaderTimeout: 10 * time.Second}

	go (&outbox.Dispatcher{Store: st, Temporal: tc, Log: log}).Run(ctx)
	go func() {
		alloc := &capacity.Allocator{Store: st}
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n, err := alloc.Sweep(ctx); err != nil {
					log.Warn("reservation sweep", "err", err)
				} else if n > 0 {
					log.Info("released expired reservations", "count", n)
				}
			}
		}
	}()
	for _, s := range []*http.Server{tenantSrv, cellSrv} {
		go func(s *http.Server) {
			log.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("http server", "addr", s.Addr, "err", err)
				stop()
			}
		}(s)
	}
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = tenantSrv.Shutdown(shutdown)
	_ = cellSrv.Shutdown(shutdown)
}

type nopTemporalLogger struct{}

func (nopTemporalLogger) Debug(string, ...any) {}
func (nopTemporalLogger) Info(string, ...any)  {}
func (nopTemporalLogger) Warn(string, ...any)  {}
func (nopTemporalLogger) Error(string, ...any) {}
