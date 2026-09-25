// Package usecases wires the pure saga.Apply state machine to persistence
// and messaging: the "handle one incoming event" workflow that the worker
// drives. Ports here are hand-rolled interfaces so tests can supply
// in-memory fakes without a real DB or broker.
package usecases

import (
	"context"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/google/uuid"
)

// SagaRepository is the persistence port for saga instances.
type SagaRepository interface {
	Create(ctx context.Context, s saga.SagaInstance, outboxRows []db.OutboxEvent) error
	ApplyTransition(ctx context.Context, sagaID uuid.UUID, fromState string, next saga.SagaInstance, eventName string, eventID uuid.UUID, outboxRows []db.OutboxEvent) error
	FindByVideoID(ctx context.Context, videoID uuid.UUID) (*saga.SagaInstance, error)
}

// ProcessedEventRepository provides idempotency for event handling.
type ProcessedEventRepository interface {
	IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error)
	MarkProcessed(ctx context.Context, eventID uuid.UUID) error
}
