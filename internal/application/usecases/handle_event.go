package usecases

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/messaging"
	"github.com/google/uuid"
)

// HandleEvent connects the pure saga.Apply state machine to persistence
// and messaging. Unlike a saga with real compensating actions, there is no
// synchronous best-effort callback to a sibling service here: status
// propagation to upload-service happens ONLY via the
// video.status.completed/video.status.failed command events already in
// the transition table, published through the outbox — never a direct
// call. See docs/adr/0001-orchestrated-saga.md.
type HandleEvent struct {
	Sagas     SagaRepository
	Processed ProcessedEventRepository
}

func NewHandleEvent(sagas SagaRepository, processed ProcessedEventRepository) *HandleEvent {
	return &HandleEvent{Sagas: sagas, Processed: processed}
}

// Handle processes one incoming domain event. It returns an error only for
// genuine infrastructure failures the caller should retry; duplicate
// events and out-of-order/invalid transitions are logged and swallowed.
func (h *HandleEvent) Handle(ctx context.Context, ev messaging.Event) error {
	eventID, err := uuid.Parse(ev.EventID)
	if err != nil {
		return fmt.Errorf("invalid event_id %q: %w", ev.EventID, err)
	}

	processed, err := h.Processed.IsProcessed(ctx, eventID)
	if err != nil {
		return fmt.Errorf("checking idempotency: %w", err)
	}
	if processed {
		slog.Info("event already processed, skipping", "event_id", ev.EventID, "event_name", ev.EventName)
		return nil
	}

	videoID, err := extractVideoID(ev.Payload)
	if err != nil {
		return fmt.Errorf("extracting video_id: %w", err)
	}

	if ev.EventName == saga.EventVideoUploaded {
		return h.handleVideoUploaded(ctx, videoID, ev, eventID)
	}

	current, err := h.Sagas.FindByVideoID(ctx, videoID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			slog.Warn("no saga found for video_id, ignoring event", "video_id", videoID, "event_name", ev.EventName)
			return h.Processed.MarkProcessed(ctx, eventID)
		}
		return fmt.Errorf("finding saga by video_id: %w", err)
	}

	return h.applyAndPersist(ctx, *current, ev, eventID)
}

func (h *HandleEvent) handleVideoUploaded(ctx context.Context, videoID uuid.UUID, ev messaging.Event, eventID uuid.UUID) error {
	if existing, err := h.Sagas.FindByVideoID(ctx, videoID); err == nil && existing != nil {
		slog.Warn("duplicate VideoUploaded for existing saga, ignoring", "video_id", videoID)
		return h.Processed.MarkProcessed(ctx, eventID)
	} else if err != nil && !errors.Is(err, db.ErrNotFound) {
		return fmt.Errorf("checking for existing saga: %w", err)
	}

	// The new saga ID is generated here (rather than after Apply, as a
	// non-creation transition would) because CommandProcessVideo's
	// payload needs to carry saga_id, and saga.Apply is kept pure/
	// deterministic (no uuid.New() calls inside it).
	next, commands, err := saga.Apply(saga.SagaInstance{ID: uuid.New()}, ev.EventName, ev.Payload)
	if err != nil {
		if errors.Is(err, saga.ErrInvalidTransition) {
			slog.Warn("invalid transition on VideoUploaded, ignoring", "video_id", videoID)
			return h.Processed.MarkProcessed(ctx, eventID)
		}
		return fmt.Errorf("applying transition: %w", err)
	}

	next.LastEventID = eventID

	outboxRows, err := buildOutboxRows(commands, ev.CorrelationID, next.ID, videoID)
	if err != nil {
		return fmt.Errorf("building outbox rows: %w", err)
	}

	if err := h.Sagas.Create(ctx, next, outboxRows); err != nil {
		return fmt.Errorf("persisting new saga: %w", err)
	}
	slog.Info("saga created", "saga_id", next.ID, "video_id", videoID, "state", next.State)
	return nil
}

func (h *HandleEvent) applyAndPersist(ctx context.Context, current saga.SagaInstance, ev messaging.Event, eventID uuid.UUID) error {
	next, commands, err := saga.Apply(current, ev.EventName, ev.Payload)
	if err != nil {
		if errors.Is(err, saga.ErrInvalidTransition) {
			slog.Warn("invalid/duplicate transition, ignoring", "saga_id", current.ID, "state", current.State, "event_name", ev.EventName)
			return h.Processed.MarkProcessed(ctx, eventID)
		}
		return fmt.Errorf("applying transition: %w", err)
	}

	outboxRows, err := buildOutboxRows(commands, ev.CorrelationID, current.ID, current.VideoID)
	if err != nil {
		return fmt.Errorf("building outbox rows: %w", err)
	}

	if err := h.Sagas.ApplyTransition(ctx, current.ID, current.State, next, ev.EventName, eventID, outboxRows); err != nil {
		return fmt.Errorf("persisting transition: %w", err)
	}
	slog.Info("saga transitioned", "saga_id", current.ID, "from", current.State, "to", next.State, "event_name", ev.EventName)

	return nil
}

func buildOutboxRows(commands []saga.CommandToEmit, correlationID string, sagaID, videoID uuid.UUID) ([]db.OutboxEvent, error) {
	rows := make([]db.OutboxEvent, 0, len(commands))
	for _, cmd := range commands {
		event, err := messaging.NewEvent(cmd.Name, correlationID, sagaID.String(), cmd.Payload)
		if err != nil {
			return nil, err
		}
		payloadBytes, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		headersBytes, err := json.Marshal(map[string]any{"x-correlation-id": correlationID})
		if err != nil {
			return nil, err
		}
		eventID, err := uuid.Parse(event.EventID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, db.OutboxEvent{
			EventID:     eventID,
			AggregateID: videoID,
			EventName:   cmd.Name,
			Payload:     payloadBytes,
			Headers:     headersBytes,
		})
	}
	return rows, nil
}

func extractVideoID(payload json.RawMessage) (uuid.UUID, error) {
	var m struct {
		VideoID string `json:"video_id"`
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		return uuid.Nil, err
	}
	if m.VideoID == "" {
		return uuid.Nil, fmt.Errorf("payload missing video_id")
	}
	return uuid.Parse(m.VideoID)
}
