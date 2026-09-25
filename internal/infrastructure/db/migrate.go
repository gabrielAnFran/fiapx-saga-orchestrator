package db

import (
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	pgmigrate "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Migrate runs pending migrations from migrationsPath against dsn. It is
// idempotent: calling it with nothing pending is a no-op.
func Migrate(dsn, migrationsPath string) error {
	sqlDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	genericDB, err := sqlDB.DB()
	if err != nil {
		return err
	}
	driver, err := pgmigrate.WithInstance(genericDB, &pgmigrate.Config{})
	if err != nil {
		return err
	}
	m, err := migrate.NewWithDatabaseInstance(fmt.Sprintf("file://%s", migrationsPath), "postgres", driver)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}
