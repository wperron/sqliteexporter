// Copyright 2024 William Perron. All rights reserved. MIT License.
package sqliteexporter

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"

	"go.wperron.io/sqliteexporter/internal/metadata"
)

//go:embed migrations/*.sql
var migrations embed.FS

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(nil, metadata.MetricsStability),
		exporter.WithLogs(nil, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Traces, error) {
	conf := cfg.(*Config)

	se, err := newSqliteExporter(set.Logger, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create sqlite exporter: %w", err)
	}

	return exporterhelper.NewTracesExporter(
		ctx, set, cfg,
		se.ConsumeTraces,
		exporterhelper.WithStart(se.Start),
		exporterhelper.WithShutdown(se.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

func newSqliteExporter(logger *zap.Logger, cfg *Config) (*sqliteExporter, error) {
	db, err := sql.Open("sqlite3", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("couldn't open sqlite3 database: %w", err)
	}

	// IMPORTANT: database/sql opens a connection pool by default, but sqlite
	// only allows a single connection to be open at the same time.
	db.SetMaxOpenConns(1)

	if err := doMigrate(db); err != nil {
		return nil, err
	}

	return &sqliteExporter{
		db:     db,
		logger: logger,
	}, nil
}

func NewSqliteSDKTraceExporter(cfg *Config) (sdktrace.SpanExporter, error) {
	return newSqliteExporter(zap.NewNop(), cfg)
}

func NewSqliteSDKTraceExporterWithDB(db *sql.DB) (sdktrace.SpanExporter, error) {
	if err := doMigrate(db); err != nil {
		return nil, err
	}

	return &sqliteExporter{
		db:     db,
		logger: zap.NewNop(),
	}, nil
}

func doMigrate(db *sql.DB) error {
	d, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("failed to open iofs migration source: %w", err)
	}

	dr, err := sqlite3.WithInstance(db, &sqlite3.Config{
		MigrationsTable: "schema_migrations_sqliteexporter",
	})
	if err != nil {
		return fmt.Errorf("failed to initialize sqlite3 migrate driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", d, "sqliteexporter", dr)
	if err != nil {
		return fmt.Errorf("failed to initialize db migrate: %w", err)
	}

	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to roll up migrations: %w", err)
	}

	return nil
}
