package main

import (
	"log/slog"
	"os"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/config"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/presentation/handlers"
	"github.com/gin-gonic/gin"
)

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

	sagaRepo := db.NewSagaRepository(gormDB)

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	h := handlers.New(sagaRepo, gormDB)
	h.Register(router)

	slog.Info("saga-orchestrator server listening", "port", cfg.Port)
	if err := router.Run(":" + cfg.Port); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
