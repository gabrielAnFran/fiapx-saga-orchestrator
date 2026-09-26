//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/application/usecases"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	infradb "github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/messaging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flowRoutingKeys mirrors cmd/worker/main.go's routingKeys: every "fact"
// event the orchestrator reacts to.
var flowRoutingKeys = []string{
	saga.EventVideoUploaded,
	saga.EventVideoProcessingCompleted,
	saga.EventVideoProcessingFailed,
}

// startWorker wires a real messaging.Conn to the real HandleEvent use case
// against the shared Postgres, exactly as cmd/worker/main.go does, and
// starts consuming in the background. It returns the repos so the test can
// assert on Postgres state, plus a cleanup-free conn the test owns.
func startWorker(t *testing.T, ctx context.Context, svc string) (*messaging.Conn, *infradb.SagaRepository, *infradb.OutboxRepository, *infradb.ProcessedEventRepository) {
	t.Helper()

	gormDB := newTestDB(t)
	sagaRepo := infradb.NewSagaRepository(gormDB)
	processedRepo := infradb.NewProcessedEventRepository(gormDB)
	outboxRepo := infradb.NewOutboxRepository(gormDB)
	handler := usecases.NewHandleEvent(sagaRepo, processedRepo)

	conn, err := messaging.Dial(testAMQPURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.DeclareServiceQueue(svc, flowRoutingKeys)
	require.NoError(t, err)

	go func() { _ = conn.Consume(ctx, svc, handler.Handle) }()

	return conn, sagaRepo, outboxRepo, processedRepo
}

func publishFact(t *testing.T, ctx context.Context, conn *messaging.Conn, eventName, correlationID string, payload map[string]any) messaging.Event {
	t.Helper()
	ev, err := messaging.NewEvent(eventName, correlationID, "", payload)
	require.NoError(t, err)
	require.NoError(t, conn.Publish(ctx, ev))
	return ev
}

// waitForSagaState polls FindByVideoID until it reports the wanted state
// (or the state-machine has moved past it into a different terminal state),
// since the worker is consuming asynchronously in the background.
func waitForSagaState(t *testing.T, repo *infradb.SagaRepository, videoID uuid.UUID, want string) saga.SagaInstance {
	t.Helper()
	var found *saga.SagaInstance
	assert.Eventually(t, func() bool {
		s, err := repo.FindByVideoID(context.Background(), videoID)
		if err != nil {
			return false
		}
		found = s
		return s.State == want
	}, 15*time.Second, 100*time.Millisecond, "saga for video %s never reached state %s", videoID, want)
	require.NotNil(t, found, "saga for video %s was never created", videoID)
	return *found
}

func waitForOutboxCount(t *testing.T, repo *infradb.OutboxRepository, n int) []infradb.OutboxRow {
	t.Helper()
	var rows []infradb.OutboxRow
	assert.Eventually(t, func() bool {
		r, err := repo.FetchUnpublished(context.Background(), 10)
		if err != nil {
			return false
		}
		rows = r
		return len(rows) == n
	}, 15*time.Second, 100*time.Millisecond, "expected %d unpublished outbox rows, last saw %d", n, len(rows))
	return rows
}

func outboxEventNames(rows []infradb.OutboxRow) []string {
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.EventName
	}
	return names
}

// TestSagaFlow_UploadToCompleted_EndToEnd is the primary end-to-end test:
// a real video.uploaded event is published to real RabbitMQ, the worker (a
// real messaging.Conn + real HandleEvent use case) consumes it and creates
// the saga in real Postgres, then a real video.processing.completed event
// drives it to COMPLETED, with the outbox holding the exact commands the
// state machine says it should for each step. Finally, one outbox row is
// dispatched onto the real broker and consumed back, closing the loop from
// "real event in" to "real outbox event out".
func TestSagaFlow_UploadToCompleted_EndToEnd(t *testing.T) {
	gormDB := newTestDB(t)
	truncateAll(t, gormDB)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := uniqueSvc("saga-flow-completed")
	conn, sagaRepo, outboxRepo, processedRepo := startWorker(t, ctx, svc)

	videoID := uuid.New()
	uploadedEv := publishFact(t, ctx, conn, saga.EventVideoUploaded, "corr-flow-1", map[string]any{
		"video_id":          videoID.String(),
		"user_id":           "user-1",
		"user_email":        "user1@example.com",
		"original_filename": "movie.mp4",
		"content_type":      "video/mp4",
		"size_bytes":        12345,
		"source_bucket":     "uploads",
		"source_object_key": "raw/" + videoID.String() + ".mp4",
	})

	uploaded := waitForSagaState(t, sagaRepo, videoID, saga.StateProcessing)
	require.Equal(t, saga.SagaTypeVideoProcessing, uploaded.SagaType)

	processRows := waitForOutboxCount(t, outboxRepo, 1)
	require.Equal(t, saga.CommandProcessVideo, processRows[0].EventName)

	var processCmdEvent messaging.Event
	require.NoError(t, json.Unmarshal(processRows[0].Payload, &processCmdEvent))
	var processPayload map[string]any
	require.NoError(t, json.Unmarshal(processCmdEvent.Payload, &processPayload))
	assert.Equal(t, "user-1", processPayload["user_id"])
	assert.Equal(t, "uploads", processPayload["source_bucket"])
	assert.EqualValues(t, 1, processPayload["frame_interval_seconds"])

	uploadedEventID, err := uuid.Parse(uploadedEv.EventID)
	require.NoError(t, err)
	isProcessed, err := processedRepo.IsProcessed(ctx, uploadedEventID)
	require.NoError(t, err)
	require.True(t, isProcessed)

	// Dispatch the CommandProcessVideo outbox row onto the real broker (the
	// job outbox-dispatcher performs on a tick) and verify a consumer bound
	// to its routing key actually receives it: the "real outbox event out"
	// half of the flow.
	scratchQ, err := conn.Channel().QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, conn.Channel().QueueBind(scratchQ.Name, saga.CommandProcessVideo, messaging.EventsExchange, false, nil))
	require.NoError(t, conn.Publish(ctx, processCmdEvent))
	require.NoError(t, outboxRepo.MarkPublished(ctx, processRows[0].ID))

	scratchMsgs, err := conn.Channel().Consume(scratchQ.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-scratchMsgs:
		var received messaging.Event
		require.NoError(t, json.Unmarshal(d.Body, &received))
		require.Equal(t, saga.CommandProcessVideo, received.EventName)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the dispatched video.process.requested command on the broker")
	}

	// Duplicate delivery of the already-processed video.uploaded event must
	// not create a second saga or a second outbox row.
	require.NoError(t, conn.Publish(ctx, uploadedEv))
	time.Sleep(500 * time.Millisecond)
	afterDuplicate, err := outboxRepo.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, afterDuplicate, "duplicate video.uploaded delivery must not enqueue another command")

	// Drive the saga to completion.
	publishFact(t, ctx, conn, saga.EventVideoProcessingCompleted, "corr-flow-1", map[string]any{
		"video_id":       videoID.String(),
		"job_id":         "job-1",
		"zip_bucket":     "results",
		"zip_object_key": "zips/" + videoID.String() + ".zip",
		"frame_count":    42,
		"completed_at":   time.Now().UTC().Format(time.RFC3339),
	})

	completed := waitForSagaState(t, sagaRepo, videoID, saga.StateCompleted)
	require.Equal(t, videoID, completed.VideoID)

	finalRows := waitForOutboxCount(t, outboxRepo, 2)
	names := outboxEventNames(finalRows)
	assert.Contains(t, names, saga.CommandStatusCompleted)
	assert.Contains(t, names, saga.CommandNotifyRequested)

	for _, row := range finalRows {
		if row.EventName != saga.CommandNotifyRequested {
			continue
		}
		var notifyEvent messaging.Event
		require.NoError(t, json.Unmarshal(row.Payload, &notifyEvent))
		var notifyPayload map[string]any
		require.NoError(t, json.Unmarshal(notifyEvent.Payload, &notifyPayload))
		assert.Equal(t, "COMPLETED", notifyPayload["notification_type"])
		assert.Equal(t, "user1@example.com", notifyPayload["recipient_email"])
	}

	history, err := sagaRepo.History(ctx, completed.ID)
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, saga.StateProcessing, history[0].ToState)
	require.Equal(t, saga.StateCompleted, history[1].ToState)
}

// TestSagaFlow_UploadToFailed_EndToEnd exercises the other terminal branch:
// video.uploaded followed by video.processing.failed must land the saga in
// FAILED and enqueue a FAILED-flavored notification command.
func TestSagaFlow_UploadToFailed_EndToEnd(t *testing.T) {
	gormDB := newTestDB(t)
	truncateAll(t, gormDB)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := uniqueSvc("saga-flow-failed")
	conn, sagaRepo, outboxRepo, _ := startWorker(t, ctx, svc)

	videoID := uuid.New()
	publishFact(t, ctx, conn, saga.EventVideoUploaded, "corr-flow-2", map[string]any{
		"video_id":          videoID.String(),
		"user_id":           "user-2",
		"user_email":        "user2@example.com",
		"original_filename": "clip.mov",
		"source_bucket":     "uploads",
		"source_object_key": "raw/" + videoID.String() + ".mov",
	})
	waitForSagaState(t, sagaRepo, videoID, saga.StateProcessing)
	waitForOutboxCount(t, outboxRepo, 1)

	publishFact(t, ctx, conn, saga.EventVideoProcessingFailed, "corr-flow-2", map[string]any{
		"video_id":      videoID.String(),
		"job_id":        "job-2",
		"error_code":    "DECODE_ERROR",
		"error_message": "could not decode video stream",
		"failed_at":     time.Now().UTC().Format(time.RFC3339),
	})

	failed := waitForSagaState(t, sagaRepo, videoID, saga.StateFailed)
	require.Equal(t, videoID, failed.VideoID)

	finalRows := waitForOutboxCount(t, outboxRepo, 3)
	var notifyPayload map[string]any
	for _, row := range finalRows {
		if row.EventName != saga.CommandNotifyRequested {
			continue
		}
		var notifyEvent messaging.Event
		require.NoError(t, json.Unmarshal(row.Payload, &notifyEvent))
		require.NoError(t, json.Unmarshal(notifyEvent.Payload, &notifyPayload))
	}
	require.NotNil(t, notifyPayload, "expected a video.notify.requested outbox row")
	assert.Equal(t, "FAILED", notifyPayload["notification_type"])
	assert.Equal(t, "could not decode video stream", notifyPayload["error_message"])

	stuck, err := sagaRepo.StuckSagas(ctx, time.Now().UTC().Add(time.Hour))
	require.NoError(t, err)
	for _, s := range stuck {
		require.NotEqual(t, videoID, s.VideoID, "a FAILED saga is terminal and must never be reported stuck")
	}
}
