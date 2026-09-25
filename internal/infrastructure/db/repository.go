package db

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var ErrNotFound = errors.New("db: not found")

type SagaRepository struct {
	db *gorm.DB
}

func NewSagaRepository(d *gorm.DB) *SagaRepository { return &SagaRepository{db: d} }

// OutboxEvent is the minimal shape needed to persist a command into the
// outbox table alongside a saga state change, in the same transaction.
type OutboxEvent struct {
	EventID     uuid.UUID
	AggregateID uuid.UUID
	EventName   string
	Payload     json.RawMessage
	Headers     json.RawMessage
}

func toInstanceRow(s saga.SagaInstance) SagaInstanceRow {
	return SagaInstanceRow{
		ID:          s.ID,
		SagaType:    s.SagaType,
		VideoID:     s.VideoID,
		State:       s.State,
		Context:     s.Context,
		LastEventID: s.LastEventID,
		RetryCount:  s.RetryCount,
		CreatedAt:   s.CreatedAt,
		UpdatedAt:   s.UpdatedAt,
	}
}

func fromInstanceRow(r SagaInstanceRow) saga.SagaInstance {
	return saga.SagaInstance{
		ID:          r.ID,
		SagaType:    r.SagaType,
		VideoID:     r.VideoID,
		State:       r.State,
		Context:     r.Context,
		LastEventID: r.LastEventID,
		RetryCount:  r.RetryCount,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
	}
}

func toOutboxRow(o OutboxEvent) OutboxRow {
	return OutboxRow{
		EventID:     o.EventID,
		AggregateID: o.AggregateID,
		EventName:   o.EventName,
		Payload:     o.Payload,
		Headers:     o.Headers,
		CreatedAt:   time.Now().UTC(),
	}
}

// Create persists a brand-new saga (from the VideoUploaded transition)
// plus its initial history row and any outbox rows for commands emitted,
// atomically.
func (r *SagaRepository) Create(ctx context.Context, s saga.SagaInstance, outboxRows []OutboxEvent) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := toInstanceRow(s)
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		hist := SagaHistoryRow{
			SagaID:    s.ID,
			FromState: "",
			ToState:   s.State,
			EventName: saga.EventVideoUploaded,
			EventID:   s.LastEventID,
			At:        time.Now().UTC(),
		}
		if err := tx.Create(&hist).Error; err != nil {
			return err
		}
		for _, o := range outboxRows {
			row := toOutboxRow(o)
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
		if s.LastEventID != uuid.Nil {
			processed := ProcessedEventRow{EventID: s.LastEventID, ProcessedAt: time.Now().UTC()}
			if err := tx.Create(&processed).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// ApplyTransition atomically updates the saga's state, appends a history
// row, and enqueues any outbox rows for emitted commands. This is the core
// "atomic state change + command publish" operation of the outbox pattern
// applied to command emission.
func (r *SagaRepository) ApplyTransition(ctx context.Context, sagaID uuid.UUID, fromState string, next saga.SagaInstance, eventName string, eventID uuid.UUID, outboxRows []OutboxEvent) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&SagaInstanceRow{}).Where("id = ?", sagaID).Updates(map[string]any{
			"state":         next.State,
			"context":       next.Context,
			"last_event_id": eventID,
			"updated_at":    time.Now().UTC(),
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return ErrNotFound
		}
		hist := SagaHistoryRow{
			SagaID:    sagaID,
			FromState: fromState,
			ToState:   next.State,
			EventName: eventName,
			EventID:   eventID,
			At:        time.Now().UTC(),
		}
		if err := tx.Create(&hist).Error; err != nil {
			return err
		}
		for _, o := range outboxRows {
			row := toOutboxRow(o)
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
		if eventID != uuid.Nil {
			processed := ProcessedEventRow{EventID: eventID, ProcessedAt: time.Now().UTC()}
			if err := tx.Create(&processed).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *SagaRepository) FindByVideoID(ctx context.Context, videoID uuid.UUID) (*saga.SagaInstance, error) {
	var row SagaInstanceRow
	err := r.db.WithContext(ctx).Where("video_id = ?", videoID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	s := fromInstanceRow(row)
	return &s, nil
}

func (r *SagaRepository) History(ctx context.Context, sagaID uuid.UUID) ([]SagaHistoryRow, error) {
	var rows []SagaHistoryRow
	err := r.db.WithContext(ctx).Where("saga_id = ?", sagaID).Order("at asc").Find(&rows).Error
	return rows, err
}

// StuckSagas returns non-terminal sagas that haven't progressed since
// before the given cutoff. Not driven by any loop in this hackathon build
// (kept for parity with the sibling orchestrator and as a seam for a
// future saga-tick reconciliation loop); cheap to keep even unused.
func (r *SagaRepository) StuckSagas(ctx context.Context, cutoff time.Time) ([]saga.SagaInstance, error) {
	var rows []SagaInstanceRow
	err := r.db.WithContext(ctx).
		Where("updated_at < ?", cutoff).
		Where("state NOT IN ?", []string{saga.StateCompleted, saga.StateFailed}).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]saga.SagaInstance, 0, len(rows))
	for _, row := range rows {
		out = append(out, fromInstanceRow(row))
	}
	return out, nil
}

// OutboxRepository handles polling and marking rows for the dispatcher.
type OutboxRepository struct {
	db *gorm.DB
}

func NewOutboxRepository(d *gorm.DB) *OutboxRepository { return &OutboxRepository{db: d} }

func (r *OutboxRepository) FetchUnpublished(ctx context.Context, limit int) ([]OutboxRow, error) {
	var rows []OutboxRow
	err := r.db.WithContext(ctx).
		Where("published_at IS NULL").
		Order("created_at asc").
		Limit(limit).
		Find(&rows).Error
	return rows, err
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id int64) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&OutboxRow{}).Where("id = ?", id).Update("published_at", now).Error
}

// ProcessedEventRepository provides idempotency for the worker's event
// handling.
type ProcessedEventRepository struct {
	db *gorm.DB
}

func NewProcessedEventRepository(d *gorm.DB) *ProcessedEventRepository {
	return &ProcessedEventRepository{db: d}
}

func (r *ProcessedEventRepository) IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&ProcessedEventRow{}).Where("event_id = ?", eventID).Count(&count).Error
	return count > 0, err
}

func (r *ProcessedEventRepository) MarkProcessed(ctx context.Context, eventID uuid.UUID) error {
	row := ProcessedEventRow{EventID: eventID, ProcessedAt: time.Now().UTC()}
	return r.db.WithContext(ctx).Clauses().Create(&row).Error
}
