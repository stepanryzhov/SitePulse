package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"sitepulse/internal/config"
	"sitepulse/internal/contracts"
	"sitepulse/internal/notifier"
	"sitepulse/internal/platform"
	"sitepulse/migrations"
)

func main() {
	log := platform.Logger("notifier")
	cfg, err := config.Load("notifier")
	if err != nil {
		log.Error("configuration error", "error", err)
		os.Exit(1)
	}
	ctx, cancel := platform.Context()
	defer cancel()
	if err = migrations.Up("notifier", cfg.DatabaseURL); err != nil {
		log.Error("migration failed", "error", err)
		os.Exit(1)
	}
	db, err := platform.Database(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database connection failed")
		os.Exit(1)
	}
	defer db.Close()
	client, err := platform.Kafka(cfg, kgo.ConsumerGroup("sitepulse-notifier-v1"), kgo.ConsumeTopics(contracts.AccountsTopic, contracts.IncidentsTopic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.RebalanceTimeout(5*time.Minute), kgo.FetchMaxBytes(2<<20))
	if err != nil {
		log.Error("Kafka configuration failed")
		os.Exit(1)
	}
	defer client.Close()
	mailer, err := notifier.NewMailer(cfg)
	if err != nil {
		log.Error("SMTP configuration failed", "error", err)
		os.Exit(1)
	}
	service := &notifier.Service{DB: db, PublicURL: cfg.PublicURL, Mailer: mailer, Log: log}
	var wg sync.WaitGroup
	wg.Go(func() { service.Consume(ctx, client) })
	wg.Go(func() { service.RunDelivery(ctx, cfg.DeliveryWorkers) })
	wg.Go(func() { service.RunCleanup(ctx) })
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "application/json")
		if db.Ping(ctx) != nil {
			w.WriteHeader(503)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "database unavailable"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})
	err = platform.Serve(ctx, cfg.Addr, mux, log)
	cancel()
	wg.Wait()
	if err != nil {
		log.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
	log.Info("stopped")
}
