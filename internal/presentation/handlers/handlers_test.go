package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type fakeSagaReader struct {
	byVideoID  map[uuid.UUID]*saga.SagaInstance
	history    map[uuid.UUID][]db.SagaHistoryRow
	findErr    error
	historyErr error
}

func (f *fakeSagaReader) FindByVideoID(ctx context.Context, videoID uuid.UUID) (*saga.SagaInstance, error) {
	if f.findErr != nil {
		return nil, f.findErr
	}
	s, ok := f.byVideoID[videoID]
	if !ok {
		return nil, db.ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (f *fakeSagaReader) History(ctx context.Context, sagaID uuid.UUID) ([]db.SagaHistoryRow, error) {
	if f.historyErr != nil {
		return nil, f.historyErr
	}
	return f.history[sagaID], nil
}

func newTestRouter(t *testing.T, reader SagaReader, sqlDB *gorm.DB) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := New(reader, sqlDB)
	h.Register(r)
	return r
}

func mockGormDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	mock.ExpectPing() // gorm.Open pings once to verify the connection
	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	require.NoError(t, err)
	return gormDB, mock
}

func TestHealthz(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReadyz_DBUp(t *testing.T) {
	gormDB, mock := mockGormDB(t)
	mock.ExpectPing()
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReadyz_DBDown(t *testing.T) {
	gormDB, mock := mockGormDB(t)
	mock.ExpectPing().WillReturnError(assert.AnError)
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestGetSaga_Found(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	id := uuid.New()
	videoID := uuid.New()
	reader := &fakeSagaReader{
		byVideoID: map[uuid.UUID]*saga.SagaInstance{
			videoID: {ID: id, VideoID: videoID, State: saga.StateCompleted},
		},
		history: map[uuid.UUID][]db.SagaHistoryRow{
			id: {{SagaID: id, ToState: saga.StateCompleted}},
		},
	}
	r := newTestRouter(t, reader, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+videoID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.NotNil(t, body["saga"])
	assert.Len(t, body["history"], 1)
}

func TestGetSaga_NotFound(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{byVideoID: map[uuid.UUID]*saga.SagaInstance{}}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetSaga_InvalidID(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetSaga_FindByVideoIDInternalError(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{findErr: assert.AnError}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetSaga_HistoryInternalError(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	id := uuid.New()
	videoID := uuid.New()
	reader := &fakeSagaReader{
		byVideoID:  map[uuid.UUID]*saga.SagaInstance{videoID: {ID: id, VideoID: videoID}},
		historyErr: assert.AnError,
	}
	r := newTestRouter(t, reader, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+videoID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
