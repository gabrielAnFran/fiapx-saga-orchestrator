package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// SagaReader is the narrow read port this debug/demo API needs: look up
// the saga for a video and its full history trail.
type SagaReader interface {
	FindByVideoID(ctx context.Context, videoID uuid.UUID) (*saga.SagaInstance, error)
	History(ctx context.Context, sagaID uuid.UUID) ([]db.SagaHistoryRow, error)
}

type Handlers struct {
	Sagas SagaReader
	SQLDB *gorm.DB
}

func New(sagas SagaReader, sqlDB *gorm.DB) *Handlers {
	return &Handlers{Sagas: sagas, SQLDB: sqlDB}
}

func (h *Handlers) Register(r *gin.Engine) {
	r.GET("/healthz", h.Healthz)
	r.GET("/readyz", h.Readyz)

	v1 := r.Group("/api/v1")
	v1.GET("/sagas/:video_id", h.GetSaga)
}

func (h *Handlers) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handlers) Readyz(c *gin.Context) {
	sqlDB, err := h.SQLDB.DB()
	if err != nil || sqlDB.PingContext(c.Request.Context()) != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// GetSaga is a debug/demo endpoint: given a video_id, returns the saga
// instance coordinating its pipeline plus the full saga_history trail,
// useful for demoing/verifying the UPLOADED -> PROCESSING ->
// COMPLETED|FAILED flow end to end.
func (h *Handlers) GetSaga(c *gin.Context) {
	videoID, err := uuid.Parse(c.Param("video_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid video_id"})
		return
	}

	s, err := h.Sagas.FindByVideoID(c.Request.Context(), videoID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "saga not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	history, err := h.Sagas.History(c.Request.Context(), s.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"saga": s, "history": history})
}
