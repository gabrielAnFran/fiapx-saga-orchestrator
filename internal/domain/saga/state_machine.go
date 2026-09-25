// Package saga implements the orchestrator's state machine: a pure,
// side-effect-free transition table plus the command payloads each
// transition emits. Persistence and messaging live outside this package.
package saga

import "errors"

// States. This saga is a linear pipeline (there is no compensation branch
// unlike a multi-service distributed transaction with refunds/cancellations
// to roll back), so the state set is intentionally small: a video is
// UPLOADED, moves to PROCESSING once the orchestrator has told
// processing-service to start, and ends in one of the two terminal states.
const (
	StateUploaded   = "UPLOADED"
	StateProcessing = "PROCESSING"
	StateCompleted  = "COMPLETED"
	StateFailed     = "FAILED"
)

// Events consumed by the orchestrator (the 3 "facts" produced by
// upload-service and processing-service).
const (
	EventVideoUploaded            = "video.uploaded"
	EventVideoProcessingCompleted = "video.processing.completed"
	EventVideoProcessingFailed    = "video.processing.failed"
)

// Commands emitted by the orchestrator (the 4 "commands" consumed by
// upload-service, processing-service and notification-service). This is
// the only service allowed to publish these — sibling services never call
// each other directly.
const (
	CommandProcessVideo    = "video.process.requested"
	CommandStatusCompleted = "video.status.completed"
	CommandStatusFailed    = "video.status.failed"
	CommandNotifyRequested = "video.notify.requested"
)

// TerminalStates are states from which no further transition is valid.
var TerminalStates = map[string]bool{
	StateCompleted: true,
	StateFailed:    true,
}

// ErrInvalidTransition signals a (state, event) pair that isn't in the
// transition table. This is expected/normal for duplicate or out-of-order
// events in an eventually-consistent system and must not be treated as an
// infrastructure failure by callers.
var ErrInvalidTransition = errors.New("saga: invalid state transition")

// transitionKey identifies a row of the transition table.
type transitionKey struct {
	state string
	event string
}

// transition describes the outcome of a valid (state, event) pair.
type transition struct {
	next     string
	commands []string
}

// transitionTable is the exhaustive, authoritative encoding of the saga's
// state machine. A (state, event) pair not present here is invalid.
//
// The zero-value/creation row is keyed on state "" (no saga yet) and
// EventVideoUploaded; it is handled specially by Apply/the use case since
// there is no existing SagaInstance to look up by (state, event) alone.
// It emits ONLY CommandProcessVideo — there is no "notify user of receipt"
// step, since the brief only calls for notification on completion/failure
// (see buildCommandPayload's notification_type branching in instance.go).
var transitionTable = map[transitionKey]transition{
	{"", EventVideoUploaded}: {
		next:     StateProcessing,
		commands: []string{CommandProcessVideo},
	},
	{StateProcessing, EventVideoProcessingCompleted}: {
		next:     StateCompleted,
		commands: []string{CommandStatusCompleted, CommandNotifyRequested},
	},
	{StateProcessing, EventVideoProcessingFailed}: {
		next:     StateFailed,
		commands: []string{CommandStatusFailed, CommandNotifyRequested},
	},
}

// lookup returns the transition for (state, event), or false if invalid.
func lookup(state, event string) (transition, bool) {
	t, ok := transitionTable[transitionKey{state, event}]
	return t, ok
}
