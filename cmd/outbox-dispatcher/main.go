package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/config"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/messaging"
)

const batchSize = 100

func main() {
	cfg := config.Load()

	if err := db.Migrate(cfg.DBDSN, "migrations"); err != nil {
		slog.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}

	gormDB, err := db.Connect(cfg.DBDSN)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}

	amqpConn, err := messaging.Dial(cfg.AMQPURL)
	if err != nil {
		slog.Error("failed to connect to amqp", "error", err)
		os.Exit(1)
	}
	defer amqpConn.Close()

	outboxRepo := db.NewOutboxRepository(gormDB)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	interval := time.Duration(cfg.DispatchIntervalMS) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("saga-orchestrator outbox-dispatcher started", "interval_ms", cfg.DispatchIntervalMS)
	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox-dispatcher shutting down")
			return
		case <-ticker.C:
			dispatchBatch(ctx, outboxRepo, amqpConn)
		}
	}
}

func dispatchBatch(ctx context.Context, outboxRepo *db.OutboxRepository, amqpConn *messaging.Conn) {
	rows, err := outboxRepo.FetchUnpublished(ctx, batchSize)
	if err != nil {
		slog.Error("failed to fetch unpublished outbox rows", "error", err)
		return
	}
	for _, row := range rows {
		var ev messaging.Event
		if err := json.Unmarshal(row.Payload, &ev); err != nil {
			slog.Error("failed to unmarshal outbox payload, skipping", "outbox_id", row.ID, "error", err)
			continue
		}
		if err := amqpConn.Publish(ctx, ev); err != nil {
			slog.Warn("failed to publish outbox event, will retry next tick", "outbox_id", row.ID, "error", err)
			continue
		}
		if err := outboxRepo.MarkPublished(ctx, row.ID); err != nil {
			slog.Error("failed to mark outbox row published", "outbox_id", row.ID, "error", err)
		}
	}
}
