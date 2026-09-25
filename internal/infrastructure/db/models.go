package db

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type SagaInstanceRow struct {
	ID          uuid.UUID       `gorm:"type:uuid;primaryKey"`
	SagaType    string          `gorm:"column:saga_type"`
	VideoID     uuid.UUID       `gorm:"column:video_id;type:uuid"`
	State       string          `gorm:"column:state"`
	Context     json.RawMessage `gorm:"column:context;type:jsonb"`
	LastEventID uuid.UUID       `gorm:"column:last_event_id;type:uuid"`
	RetryCount  int             `gorm:"column:retry_count"`
	CreatedAt   time.Time       `gorm:"column:created_at"`
	UpdatedAt   time.Time       `gorm:"column:updated_at"`
}

func (SagaInstanceRow) TableName() string { return "saga_instances" }

type SagaHistoryRow struct {
	ID        int64     `gorm:"primaryKey"`
	SagaID    uuid.UUID `gorm:"column:saga_id;type:uuid"`
	FromState string    `gorm:"column:from_state"`
	ToState   string    `gorm:"column:to_state"`
	EventName string    `gorm:"column:event_name"`
	EventID   uuid.UUID `gorm:"column:event_id;type:uuid"`
	Error     string    `gorm:"column:error"`
	At        time.Time `gorm:"column:at"`
}

func (SagaHistoryRow) TableName() string { return "saga_history" }

type OutboxRow struct {
	ID          int64           `gorm:"primaryKey"`
	EventID     uuid.UUID       `gorm:"column:event_id;type:uuid"`
	AggregateID uuid.UUID       `gorm:"column:aggregate_id;type:uuid"`
	EventName   string          `gorm:"column:event_name"`
	Payload     json.RawMessage `gorm:"column:payload;type:jsonb"`
	Headers     json.RawMessage `gorm:"column:headers;type:jsonb"`
	CreatedAt   time.Time       `gorm:"column:created_at"`
	PublishedAt *time.Time      `gorm:"column:published_at"`
}

func (OutboxRow) TableName() string { return "outbox" }

type ProcessedEventRow struct {
	EventID     uuid.UUID `gorm:"column:event_id;type:uuid;primaryKey"`
	ProcessedAt time.Time `gorm:"column:processed_at"`
}

func (ProcessedEventRow) TableName() string { return "processed_events" }
