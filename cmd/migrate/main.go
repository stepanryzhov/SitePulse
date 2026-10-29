package main

import (
	"fmt"
	"os"
	"sitepulse/internal/config"
	"sitepulse/migrations"
)

func main() {
	for _, service := range []string{"monitor", "notifier"} {
		cfg, err := config.Load(service)
		if err == nil {
			err = migrations.Up(service, cfg.DatabaseURL)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "migration failed:", err)
			os.Exit(1)
		}
		fmt.Println(service + ": migrations applied")
	}
}
