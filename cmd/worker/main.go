package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/application/usecases"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/config"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/messaging"
)

const serviceName = "saga-orchestrator"

// routingKeys lists every "fact" event produced by upload-service and
// processing-service that this orchestrator needs to react to. It is the
// only service allowed to turn these into the 4 "command" events that
// drive the pipeline forward.
var routingKeys = []string{
	saga.EventVideoUploaded,
	saga.EventVideoProcessingCompleted,
	saga.EventVideoProcessingFailed,
}

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

	if _, err := amqpConn.DeclareServiceQueue(serviceName, routingKeys); err != nil {
		slog.Error("failed to declare service queue", "error", err)
		os.Exit(1)
	}

	sagaRepo := db.NewSagaRepository(gormDB)
	processedRepo := db.NewProcessedEventRepository(gormDB)
	handler := usecases.NewHandleEvent(sagaRepo, processedRepo)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("saga-orchestrator worker consuming events")
	if err := amqpConn.Consume(ctx, serviceName, handler.Handle); err != nil && ctx.Err() == nil {
		slog.Error("consumer stopped unexpectedly", "error", err)
		os.Exit(1)
	}
	slog.Info("saga-orchestrator worker shutting down")
}
