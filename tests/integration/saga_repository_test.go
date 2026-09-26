//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	infradb "github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSagaRepository_CreateAndTransition_EndToEnd exercises SagaRepository's
// Create and ApplyTransition against real Postgres, including history rows,
// outbox rows and idempotency (processed_events), across the full
// UPLOADED -> PROCESSING -> COMPLETED pipeline.
func TestSagaRepository_CreateAndTransition_EndToEnd(t *testing.T) {
	gormDB := newTestDB(t)
	truncateAll(t, gormDB)
	ctx := context.Background()

	repo := infradb.NewSagaRepository(gormDB)
	outboxRepo := infradb.NewOutboxRepository(gormDB)
	processedRepo := infradb.NewProcessedEventRepository(gormDB)

	videoID := uuid.New()
	sagaID := uuid.New()
	createEventID := uuid.New()

	instance := saga.SagaInstance{
		ID:          sagaID,
		SagaType:    saga.SagaTypeVideoProcessing,
		VideoID:     videoID,
		State:       saga.StateProcessing,
		Context:     []byte(`{"video_id":"` + videoID.String() + `"}`),
		LastEventID: createEventID,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	outboxEvent := infradb.OutboxEvent{
		EventID:     uuid.New(),
		AggregateID: videoID,
		EventName:   saga.CommandProcessVideo,
		Payload:     []byte(`{"video_id":"` + videoID.String() + `"}`),
		Headers:     []byte(`{}`),
	}

	require.NoError(t, repo.Create(ctx, instance, []infradb.OutboxEvent{outboxEvent}))

	found, err := repo.FindByVideoID(ctx, videoID)
	require.NoError(t, err)
	require.Equal(t, saga.StateProcessing, found.State)
	require.Equal(t, saga.SagaTypeVideoProcessing, found.SagaType)

	unpublished, err := outboxRepo.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Len(t, unpublished, 1)
	require.Equal(t, saga.CommandProcessVideo, unpublished[0].EventName)

	isProcessed, err := processedRepo.IsProcessed(ctx, createEventID)
	require.NoError(t, err)
	require.True(t, isProcessed)

	require.NoError(t, outboxRepo.MarkPublished(ctx, unpublished[0].ID))
	remaining, err := outboxRepo.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, remaining)

	extraEventID := uuid.New()
	require.NoError(t, processedRepo.MarkProcessed(ctx, extraEventID))
	isProcessed, err = processedRepo.IsProcessed(ctx, extraEventID)
	require.NoError(t, err)
	require.True(t, isProcessed)

	// Happy-path transition: PROCESSING -> COMPLETED, emitting two commands
	// (status + notify) via a single ApplyTransition call.
	completedEventID := uuid.New()
	next := *found
	next.State = saga.StateCompleted
	completedOutbox := []infradb.OutboxEvent{
		{EventID: uuid.New(), AggregateID: videoID, EventName: saga.CommandStatusCompleted, Payload: []byte(`{}`), Headers: []byte(`{}`)},
		{EventID: uuid.New(), AggregateID: videoID, EventName: saga.CommandNotifyRequested, Payload: []byte(`{}`), Headers: []byte(`{}`)},
	}
	require.NoError(t, repo.ApplyTransition(ctx, sagaID, saga.StateProcessing, next, saga.EventVideoProcessingCompleted, completedEventID, completedOutbox))

	found, err = repo.FindByVideoID(ctx, videoID)
	require.NoError(t, err)
	require.Equal(t, saga.StateCompleted, found.State)

	history, err := repo.History(ctx, sagaID)
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, saga.StateProcessing, history[0].ToState)
	require.Equal(t, saga.StateCompleted, history[1].ToState)
	require.Equal(t, saga.EventVideoProcessingCompleted, history[1].EventName)

	isProcessed, err = processedRepo.IsProcessed(ctx, completedEventID)
	require.NoError(t, err)
	require.True(t, isProcessed)

	afterCompletion, err := outboxRepo.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Len(t, afterCompletion, 2)
	names := []string{afterCompletion[0].EventName, afterCompletion[1].EventName}
	assert.Contains(t, names, saga.CommandStatusCompleted)
	assert.Contains(t, names, saga.CommandNotifyRequested)
}

// TestSagaRepository_ApplyTransition_ToFailed exercises the alternate
// terminal transition (PROCESSING -> FAILED) and confirms it is reported as
// no longer stuck once terminal, while a saga stuck in PROCESSING is.
func TestSagaRepository_ApplyTransition_ToFailed(t *testing.T) {
	gormDB := newTestDB(t)
	truncateAll(t, gormDB)
	ctx := context.Background()

	repo := infradb.NewSagaRepository(gormDB)
	videoID := uuid.New()
	sagaID := uuid.New()

	instance := saga.SagaInstance{
		ID:        sagaID,
		SagaType:  saga.SagaTypeVideoProcessing,
		VideoID:   videoID,
		State:     saga.StateProcessing,
		Context:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	require.NoError(t, repo.Create(ctx, instance, nil))

	stuckBefore, err := repo.StuckSagas(ctx, time.Now().UTC().Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, stuckBefore, 1, "a non-terminal saga updated before the cutoff must be reported stuck")

	failedEventID := uuid.New()
	next := instance
	next.State = saga.StateFailed
	require.NoError(t, repo.ApplyTransition(ctx, sagaID, saga.StateProcessing, next, saga.EventVideoProcessingFailed, failedEventID, nil))

	found, err := repo.FindByVideoID(ctx, videoID)
	require.NoError(t, err)
	require.Equal(t, saga.StateFailed, found.State)

	stuckAfter, err := repo.StuckSagas(ctx, time.Now().UTC().Add(time.Hour))
	require.NoError(t, err)
	require.Empty(t, stuckAfter, "a terminal saga must never be reported stuck")
}

// TestSagaRepository_ApplyTransition_UnknownSaga_ReturnsNotFound confirms
// ApplyTransition surfaces ErrNotFound rather than silently no-op'ing when
// the saga id doesn't exist (RowsAffected == 0 path).
func TestSagaRepository_ApplyTransition_UnknownSaga_ReturnsNotFound(t *testing.T) {
	gormDB := newTestDB(t)
	truncateAll(t, gormDB)
	ctx := context.Background()

	repo := infradb.NewSagaRepository(gormDB)
	ghost := saga.SagaInstance{State: saga.StateCompleted, Context: []byte(`{}`)}
	err := repo.ApplyTransition(ctx, uuid.New(), saga.StateProcessing, ghost, saga.EventVideoProcessingCompleted, uuid.New(), nil)
	require.ErrorIs(t, err, infradb.ErrNotFound)
}

// TestSagaRepository_FindByVideoID_NotFound confirms the not-found sentinel
// for a video with no saga at all.
func TestSagaRepository_FindByVideoID_NotFound(t *testing.T) {
	gormDB := newTestDB(t)
	truncateAll(t, gormDB)

	repo := infradb.NewSagaRepository(gormDB)
	_, err := repo.FindByVideoID(context.Background(), uuid.New())
	require.ErrorIs(t, err, infradb.ErrNotFound)
}

// TestProcessedEventRepository_IsProcessed_Missing confirms the zero-value
// (not processed) path for an event id that was never marked.
func TestProcessedEventRepository_IsProcessed_Missing(t *testing.T) {
	gormDB := newTestDB(t)
	truncateAll(t, gormDB)

	repo := infradb.NewProcessedEventRepository(gormDB)
	processed, err := repo.IsProcessed(context.Background(), uuid.New())
	require.NoError(t, err)
	assert.False(t, processed)
}
