package saga

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// SagaInstance is the durable record of one saga's progress. Context
// accumulates fields extracted from incoming event payloads (user_id,
// user_email, source_bucket, zip_object_key, ...) so later transitions can
// build the payloads of the commands they emit.
type SagaInstance struct {
	ID          uuid.UUID
	SagaType    string
	VideoID     uuid.UUID
	State       string
	Context     json.RawMessage
	LastEventID uuid.UUID
	RetryCount  int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CommandToEmit is a command produced by a transition, ready to be wrapped
// in a messaging.Event and written to the outbox.
type CommandToEmit struct {
	Name    string
	Payload map[string]any
}

const SagaTypeVideoProcessing = "video_processing"

// contextMap unmarshals ctx into a plain map, treating nil/empty as {}.
func contextMap(ctx json.RawMessage) (map[string]any, error) {
	m := map[string]any{}
	if len(ctx) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(ctx, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// payloadMap unmarshals an event payload into a plain map.
func payloadMap(payload json.RawMessage) (map[string]any, error) {
	m := map[string]any{}
	if len(payload) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// extractFields copies the given keys from src into dst when present.
func extractFields(dst, src map[string]any, keys ...string) {
	for _, k := range keys {
		if v, ok := src[k]; ok {
			dst[k] = v
		}
	}
}

// contextFieldsByEvent lists, per event name, which payload fields get
// merged into the saga's Context for later use by downstream commands.
// video.uploaded carries everything downstream commands might need
// (source location, owner, original filename); video.processing.completed
// / .failed carry their own result fields.
var contextFieldsByEvent = map[string][]string{
	EventVideoUploaded: {
		"video_id", "user_id", "user_email", "original_filename",
		"content_type", "size_bytes", "source_bucket", "source_object_key",
	},
	EventVideoProcessingCompleted: {
		"job_id", "zip_bucket", "zip_object_key", "frame_count", "completed_at",
	},
	EventVideoProcessingFailed: {
		"job_id", "error_code", "error_message", "failed_at",
	},
}

// buildCommandPayload constructs the payload for a command given the
// saga's accumulated context. outcomeState is the saga's next state
// (StateCompleted or StateFailed) for the two transitions that emit
// CommandNotifyRequested alongside a status command; it is unused/ignored
// by the other command kinds.
func buildCommandPayload(command string, videoID, sagaID uuid.UUID, ctx map[string]any, outcomeState string) map[string]any {
	switch command {
	case CommandProcessVideo:
		p := map[string]any{
			"video_id": videoID.String(),
			"saga_id":  sagaID.String(),
			// Hardcoded 1s interval for this hackathon; a future version
			// could read a per-request preference out of ctx instead.
			"frame_interval_seconds": 1,
		}
		if v, ok := ctx["user_id"]; ok {
			p["user_id"] = v
		}
		if v, ok := ctx["source_bucket"]; ok {
			p["source_bucket"] = v
		}
		if v, ok := ctx["source_object_key"]; ok {
			p["source_object_key"] = v
		}
		return p
	case CommandStatusCompleted:
		p := map[string]any{"video_id": videoID.String()}
		if v, ok := ctx["zip_bucket"]; ok {
			p["zip_bucket"] = v
		}
		if v, ok := ctx["zip_object_key"]; ok {
			p["zip_object_key"] = v
		}
		if v, ok := ctx["frame_count"]; ok {
			p["frame_count"] = v
		}
		if v, ok := ctx["completed_at"]; ok {
			p["completed_at"] = v
		}
		return p
	case CommandStatusFailed:
		p := map[string]any{"video_id": videoID.String()}
		if v, ok := ctx["error_code"]; ok {
			p["error_code"] = v
		}
		if v, ok := ctx["error_message"]; ok {
			p["error_message"] = v
		}
		if v, ok := ctx["failed_at"]; ok {
			p["failed_at"] = v
		}
		return p
	case CommandNotifyRequested:
		notificationType := "COMPLETED"
		if outcomeState == StateFailed {
			notificationType = "FAILED"
		}
		p := map[string]any{
			"video_id":          videoID.String(),
			"notification_type": notificationType,
			"recipient_email":   ctx["user_email"],
			"original_filename": ctx["original_filename"],
		}
		if v, ok := ctx["user_id"]; ok {
			p["user_id"] = v
		}
		if notificationType == "FAILED" {
			if v, ok := ctx["error_message"]; ok {
				p["error_message"] = v
			}
		}
		return p
	default:
		return map[string]any{}
	}
}

// Apply is the pure core of the orchestrator: given the current saga state
// (zero-value SagaInstance with State "" if the saga doesn't exist yet),
// an incoming event name and payload, it returns the next saga state and
// the commands to emit, or ErrInvalidTransition if (state, event) isn't a
// valid row of the transition table.
func Apply(current SagaInstance, eventName string, payload json.RawMessage) (SagaInstance, []CommandToEmit, error) {
	t, ok := lookup(current.State, eventName)
	if !ok {
		return SagaInstance{}, nil, ErrInvalidTransition
	}

	ctx, err := contextMap(current.Context)
	if err != nil {
		return SagaInstance{}, nil, err
	}
	pm, err := payloadMap(payload)
	if err != nil {
		return SagaInstance{}, nil, err
	}
	if fields, ok := contextFieldsByEvent[eventName]; ok {
		extractFields(ctx, pm, fields...)
	}

	videoID := current.VideoID
	if videoID == uuid.Nil {
		if v, ok := pm["video_id"]; ok {
			if s, ok := v.(string); ok {
				if parsed, err := uuid.Parse(s); err == nil {
					videoID = parsed
				}
			}
		}
	}

	newCtxBytes, err := json.Marshal(ctx)
	if err != nil {
		return SagaInstance{}, nil, err
	}

	next := current
	next.VideoID = videoID
	next.State = t.next
	next.Context = newCtxBytes
	if next.SagaType == "" {
		next.SagaType = SagaTypeVideoProcessing
	}

	sagaID := current.ID
	commands := make([]CommandToEmit, 0, len(t.commands))
	for _, cmdName := range t.commands {
		commands = append(commands, CommandToEmit{
			Name:    cmdName,
			Payload: buildCommandPayload(cmdName, videoID, sagaID, ctx, next.State),
		})
	}

	return next, commands, nil
}
