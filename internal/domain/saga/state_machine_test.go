package saga

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// row mirrors one line of the transition table in the spec, so this test
// is the direct evidence the encoded table matches the design exactly.
type row struct {
	name      string
	fromState string
	event     string
	payload   map[string]any
	wantState string
	wantCmds  []string
}

func allRows() []row {
	return []row{
		{
			name:      "VideoUploaded creates saga in PROCESSING",
			fromState: "",
			event:     EventVideoUploaded,
			payload: map[string]any{
				"video_id": uuid.New().String(), "user_id": uuid.New().String(),
				"user_email": "a@b.com", "source_bucket": "videos", "source_object_key": "v1.mp4",
			},
			wantState: StateProcessing,
			wantCmds:  []string{CommandProcessVideo},
		},
		{
			name:      "VideoProcessingCompleted completes the saga",
			fromState: StateProcessing,
			event:     EventVideoProcessingCompleted,
			payload: map[string]any{
				"job_id": uuid.New().String(), "zip_bucket": "zips", "zip_object_key": "v1.zip",
				"frame_count": 42, "completed_at": "2026-09-25T00:00:00Z",
			},
			wantState: StateCompleted,
			wantCmds:  []string{CommandStatusCompleted, CommandNotifyRequested},
		},
		{
			name:      "VideoProcessingFailed fails the saga",
			fromState: StateProcessing,
			event:     EventVideoProcessingFailed,
			payload: map[string]any{
				"job_id": uuid.New().String(), "error_code": "DECODE_ERROR",
				"error_message": "could not decode frame", "failed_at": "2026-09-25T00:00:00Z",
			},
			wantState: StateFailed,
			wantCmds:  []string{CommandStatusFailed, CommandNotifyRequested},
		},
	}
}

func TestApply_AllValidTransitions(t *testing.T) {
	for _, r := range allRows() {
		t.Run(r.name, func(t *testing.T) {
			current := SagaInstance{ID: uuid.New(), State: r.fromState, VideoID: uuid.New()}
			payload, err := json.Marshal(r.payload)
			require.NoError(t, err)

			next, cmds, err := Apply(current, r.event, payload)
			require.NoError(t, err)
			assert.Equal(t, r.wantState, next.State)

			gotNames := make([]string, 0, len(cmds))
			for _, c := range cmds {
				gotNames = append(gotNames, c.Name)
			}
			assert.Equal(t, r.wantCmds, gotNamesOrNil(gotNames))
		})
	}
}

func gotNamesOrNil(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

func TestApply_EveryTableRowCovered(t *testing.T) {
	// Guards against silently adding a table row without a matching test row.
	covered := map[transitionKey]bool{}
	for _, r := range allRows() {
		covered[transitionKey{r.fromState, r.event}] = true
	}
	for k := range transitionTable {
		assert.True(t, covered[k], "table row %+v has no covering test case", k)
	}
	assert.Len(t, covered, len(transitionTable))
}

func TestApply_InvalidTransitions(t *testing.T) {
	cases := []struct {
		name  string
		state string
		event string
	}{
		{"completed saga ignores further events", StateCompleted, EventVideoProcessingFailed},
		{"failed saga ignores further events", StateFailed, EventVideoUploaded},
		{"processing completed arriving before video uploaded", "", EventVideoProcessingCompleted},
		{"processing failed arriving before video uploaded", "", EventVideoProcessingFailed},
		{"duplicate VideoUploaded on an already-processing saga", StateProcessing, EventVideoUploaded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			current := SagaInstance{ID: uuid.New(), State: tc.state, VideoID: uuid.New()}
			_, _, err := Apply(current, tc.event, json.RawMessage(`{}`))
			assert.ErrorIs(t, err, ErrInvalidTransition)
		})
	}
}

func TestApply_CreationCommandCarriesSagaID(t *testing.T) {
	sagaID := uuid.New()
	videoID := uuid.New()
	current := SagaInstance{ID: sagaID}

	next, cmds, err := Apply(current, EventVideoUploaded, mustJSON(map[string]any{
		"video_id": videoID.String(), "user_id": "u1", "source_bucket": "videos", "source_object_key": "v1.mp4",
	}))
	require.NoError(t, err)
	require.Len(t, cmds, 1)
	assert.Equal(t, CommandProcessVideo, cmds[0].Name)
	assert.Equal(t, sagaID.String(), cmds[0].Payload["saga_id"])
	assert.Equal(t, videoID.String(), cmds[0].Payload["video_id"])
	assert.Equal(t, "videos", cmds[0].Payload["source_bucket"])
	assert.Equal(t, StateProcessing, next.State)
}

func TestApply_ContextAccumulatesAcrossTransitions_CompletedNotification(t *testing.T) {
	videoID := uuid.New()

	created, _, err := Apply(SagaInstance{ID: uuid.New()}, EventVideoUploaded, mustJSON(map[string]any{
		"video_id": videoID.String(), "user_id": "u1", "user_email": "u1@example.com",
		"original_filename": "vacation.mp4",
	}))
	require.NoError(t, err)

	_, cmds, err := Apply(created, EventVideoProcessingCompleted, mustJSON(map[string]any{
		"zip_bucket": "zips", "zip_object_key": videoID.String() + ".zip", "frame_count": 10,
	}))
	require.NoError(t, err)
	require.Len(t, cmds, 2)

	var statusCmd, notifyCmd *CommandToEmit
	for i := range cmds {
		switch cmds[i].Name {
		case CommandStatusCompleted:
			statusCmd = &cmds[i]
		case CommandNotifyRequested:
			notifyCmd = &cmds[i]
		}
	}
	require.NotNil(t, statusCmd)
	require.NotNil(t, notifyCmd)
	assert.Equal(t, "zips", statusCmd.Payload["zip_bucket"])
	assert.EqualValues(t, 10, statusCmd.Payload["frame_count"])
	assert.Equal(t, "COMPLETED", notifyCmd.Payload["notification_type"])
	assert.Equal(t, "u1@example.com", notifyCmd.Payload["recipient_email"])
	assert.Equal(t, "vacation.mp4", notifyCmd.Payload["original_filename"])
}

func TestApply_ContextAccumulatesAcrossTransitions_FailedNotification(t *testing.T) {
	videoID := uuid.New()

	created, _, err := Apply(SagaInstance{ID: uuid.New()}, EventVideoUploaded, mustJSON(map[string]any{
		"video_id": videoID.String(), "user_id": "u1", "user_email": "u1@example.com",
	}))
	require.NoError(t, err)

	_, cmds, err := Apply(created, EventVideoProcessingFailed, mustJSON(map[string]any{
		"error_code": "DECODE_ERROR", "error_message": "boom",
	}))
	require.NoError(t, err)
	require.Len(t, cmds, 2)

	var notifyCmd *CommandToEmit
	for i := range cmds {
		if cmds[i].Name == CommandNotifyRequested {
			notifyCmd = &cmds[i]
		}
	}
	require.NotNil(t, notifyCmd)
	assert.Equal(t, "FAILED", notifyCmd.Payload["notification_type"])
	assert.Equal(t, "boom", notifyCmd.Payload["error_message"])
}

func mustJSON(v map[string]any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
