package migrations

import (
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed monitor/*.sql notifier/*.sql
var files embed.FS

func Up(service, databaseURL string) error {
	source, err := iofs.New(files, service)
	if err != nil {
		return err
	}
	databaseURL = strings.Replace(databaseURL, "postgres://", "pgx5://", 1)
	databaseURL = strings.Replace(databaseURL, "postgresql://", "pgx5://", 1)
	m, err := migrate.NewWithSourceInstance("iofs", source, databaseURL)
	if err != nil {
		return fmt.Errorf("initialize migrations: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}
