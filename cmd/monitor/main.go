package main

import (
	"os"
	"sync"

	"sitepulse/internal/config"
	"sitepulse/internal/monitor"
	"sitepulse/internal/platform"
	"sitepulse/migrations"
)

func main() {
	log := platform.Logger("monitor")
	cfg, err := config.Load("monitor")
	if err != nil {
		log.Error("configuration error", "error", err)
		os.Exit(1)
	}
	ctx, cancel := platform.Context()
	defer cancel()
	if err = migrations.Up("monitor", cfg.DatabaseURL); err != nil {
		log.Error("migration failed", "error", err)
		os.Exit(1)
	}
	db, err := platform.Database(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database connection failed")
		os.Exit(1)
	}
	defer db.Close()
	kafka, err := platform.Kafka(cfg)
	if err != nil {
		log.Error("Kafka configuration failed")
		os.Exit(1)
	}
	defer kafka.Close()
	store := &monitor.Store{DB: db, PublicURL: cfg.PublicURL}
	server, err := monitor.NewServer(store, cfg, log)
	if err != nil {
		log.Error("template loading failed", "error", err)
		os.Exit(1)
	}
	var wg sync.WaitGroup
	wg.Go(func() { store.RunScheduler(ctx, monitor.NewChecker(), cfg.CheckWorkers, cfg.SchedulerInterval, log) })
	wg.Go(func() { store.RunOutbox(ctx, kafka, log) })
	wg.Go(func() { store.RunCleanup(ctx, cfg.RetentionDays, log) })
	err = platform.Serve(ctx, cfg.Addr, server.Handler(), log)
	cancel()
	wg.Wait()
	if err != nil {
		log.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
	log.Info("stopped")
}
