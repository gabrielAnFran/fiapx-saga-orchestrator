package usecases

import (
	"context"
	"testing"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/messaging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSagaRepo is a hand-rolled in-memory double: no real DB, keyed by
// video_id like the real repository's unique lookup path.
type fakeSagaRepo struct {
	byVideoID map[uuid.UUID]*saga.SagaInstance
	outbox    []db.OutboxEvent
}

func newFakeSagaRepo() *fakeSagaRepo {
	return &fakeSagaRepo{byVideoID: map[uuid.UUID]*saga.SagaInstance{}}
}

func (f *fakeSagaRepo) Create(ctx context.Context, s saga.SagaInstance, outboxRows []db.OutboxEvent) error {
	cp := s
	f.byVideoID[s.VideoID] = &cp
	f.outbox = append(f.outbox, outboxRows...)
	return nil
}

func (f *fakeSagaRepo) ApplyTransition(ctx context.Context, sagaID uuid.UUID, fromState string, next saga.SagaInstance, eventName string, eventID uuid.UUID, outboxRows []db.OutboxEvent) error {
	cp := next
	f.byVideoID[next.VideoID] = &cp
	f.outbox = append(f.outbox, outboxRows...)
	return nil
}

func (f *fakeSagaRepo) FindByVideoID(ctx context.Context, videoID uuid.UUID) (*saga.SagaInstance, error) {
	s, ok := f.byVideoID[videoID]
	if !ok {
		return nil, db.ErrNotFound
	}
	cp := *s
	return &cp, nil
}

type fakeProcessedRepo struct {
	processed map[uuid.UUID]bool
}

func newFakeProcessedRepo() *fakeProcessedRepo {
	return &fakeProcessedRepo{processed: map[uuid.UUID]bool{}}
}

func (f *fakeProcessedRepo) IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	return f.processed[eventID], nil
}

func (f *fakeProcessedRepo) MarkProcessed(ctx context.Context, eventID uuid.UUID) error {
	f.processed[eventID] = true
	return nil
}

func newHandler() (*HandleEvent, *fakeSagaRepo, *fakeProcessedRepo) {
	sagas := newFakeSagaRepo()
	processed := newFakeProcessedRepo()
	return NewHandleEvent(sagas, processed), sagas, processed
}

func mustEvent(t *testing.T, name, correlationID string, payload map[string]any) messaging.Event {
	t.Helper()
	ev, err := messaging.NewEvent(name, correlationID, "", payload)
	require.NoError(t, err)
	return ev
}

func TestHandle_VideoUploaded_CreatesNewSagaInProcessing(t *testing.T) {
	h, sagas, _ := newHandler()
	videoID := uuid.New()
	ev := mustEvent(t, saga.EventVideoUploaded, "corr-1", map[string]any{
		"video_id": videoID.String(), "user_id": "u1", "user_email": "u1@example.com",
		"source_bucket": "videos", "source_object_key": "v1.mp4",
	})

	err := h.Handle(context.Background(), ev)
	require.NoError(t, err)

	s, err := sagas.FindByVideoID(context.Background(), videoID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateProcessing, s.State)
	require.Len(t, sagas.outbox, 1)
	assert.Equal(t, saga.CommandProcessVideo, sagas.outbox[0].EventName)
}

func TestHandle_ProcessingCompleted_TransitionsToCompletedWithTwoCommands(t *testing.T) {
	h, sagas, _ := newHandler()
	videoID := uuid.New()

	uploaded := mustEvent(t, saga.EventVideoUploaded, "c", map[string]any{
		"video_id": videoID.String(), "user_id": "u1", "user_email": "u1@example.com",
	})
	require.NoError(t, h.Handle(context.Background(), uploaded))

	completed := mustEvent(t, saga.EventVideoProcessingCompleted, "c", map[string]any{
		"video_id": videoID.String(), "job_id": uuid.New().String(),
		"zip_bucket": "zips", "zip_object_key": videoID.String() + ".zip", "frame_count": 12,
	})
	require.NoError(t, h.Handle(context.Background(), completed))

	s, err := sagas.FindByVideoID(context.Background(), videoID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateCompleted, s.State)

	// 1 command from creation + 2 from this transition (status + notify).
	require.Len(t, sagas.outbox, 3)
	names := []string{sagas.outbox[1].EventName, sagas.outbox[2].EventName}
	assert.ElementsMatch(t, []string{saga.CommandStatusCompleted, saga.CommandNotifyRequested}, names)
}

func TestHandle_ProcessingFailed_TransitionsToFailedWithTwoCommands(t *testing.T) {
	h, sagas, _ := newHandler()
	videoID := uuid.New()

	uploaded := mustEvent(t, saga.EventVideoUploaded, "c", map[string]any{
		"video_id": videoID.String(), "user_id": "u1", "user_email": "u1@example.com",
	})
	require.NoError(t, h.Handle(context.Background(), uploaded))

	failed := mustEvent(t, saga.EventVideoProcessingFailed, "c", map[string]any{
		"video_id": videoID.String(), "job_id": uuid.New().String(),
		"error_code": "DECODE_ERROR", "error_message": "boom",
	})
	require.NoError(t, h.Handle(context.Background(), failed))

	s, err := sagas.FindByVideoID(context.Background(), videoID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateFailed, s.State)

	require.Len(t, sagas.outbox, 3)
	names := []string{sagas.outbox[1].EventName, sagas.outbox[2].EventName}
	assert.ElementsMatch(t, []string{saga.CommandStatusFailed, saga.CommandNotifyRequested}, names)
}

func TestHandle_DuplicateEvent_IsNoOp(t *testing.T) {
	h, sagas, processed := newHandler()
	videoID := uuid.New()
	ev := mustEvent(t, saga.EventVideoUploaded, "c", map[string]any{"video_id": videoID.String()})

	require.NoError(t, h.Handle(context.Background(), ev))
	initialOutboxLen := len(sagas.outbox)

	require.NoError(t, h.Handle(context.Background(), ev))
	assert.Len(t, sagas.outbox, initialOutboxLen, "no new commands should be emitted for a duplicate event")

	eventID, err := uuid.Parse(ev.EventID)
	require.NoError(t, err)
	isProcessed, err := processed.IsProcessed(context.Background(), eventID)
	require.NoError(t, err)
	assert.True(t, isProcessed)
}

func TestHandle_DuplicateVideoUploaded_ForExistingSaga_IsIgnored(t *testing.T) {
	h, sagas, _ := newHandler()
	videoID := uuid.New()
	first := mustEvent(t, saga.EventVideoUploaded, "c", map[string]any{"video_id": videoID.String()})
	require.NoError(t, h.Handle(context.Background(), first))

	second := mustEvent(t, saga.EventVideoUploaded, "c", map[string]any{"video_id": videoID.String()})
	require.NoError(t, h.Handle(context.Background(), second))

	s, err := sagas.FindByVideoID(context.Background(), videoID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateProcessing, s.State)
	assert.Len(t, sagas.outbox, 1, "no new commands should be emitted for a duplicate VideoUploaded")
}

func TestHandle_EventForUnknownVideoID_IsIgnoredNotError(t *testing.T) {
	h, sagas, processed := newHandler()
	videoID := uuid.New()
	ev := mustEvent(t, saga.EventVideoProcessingCompleted, "c", map[string]any{
		"video_id": videoID.String(), "job_id": uuid.New().String(),
	})

	require.NoError(t, h.Handle(context.Background(), ev))

	_, err := sagas.FindByVideoID(context.Background(), videoID)
	assert.ErrorIs(t, err, db.ErrNotFound)

	eventID, err := uuid.Parse(ev.EventID)
	require.NoError(t, err)
	isProcessed, err := processed.IsProcessed(context.Background(), eventID)
	require.NoError(t, err)
	assert.True(t, isProcessed)
}

func TestHandle_InvalidEventID_ReturnsError(t *testing.T) {
	h, _, _ := newHandler()
	ev := messaging.Event{EventID: "not-a-uuid", EventName: saga.EventVideoUploaded, Payload: []byte(`{}`)}
	err := h.Handle(context.Background(), ev)
	require.Error(t, err)
}

func TestHandle_MissingVideoID_ReturnsError(t *testing.T) {
	h, _, _ := newHandler()
	ev := mustEvent(t, saga.EventVideoUploaded, "c", map[string]any{})
	err := h.Handle(context.Background(), ev)
	require.Error(t, err)
}
