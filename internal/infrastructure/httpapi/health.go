package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/infrastructure/messaging"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Readiness struct {
	pool     *pgxpool.Pool
	consumer *messaging.SQSConsumer
}

func NewReadiness(pool *pgxpool.Pool, consumer *messaging.SQSConsumer) *Readiness {
	return &Readiness{pool: pool, consumer: consumer}
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	status := http.StatusOK
	checks := map[string]string{"status": "ok", "postgres": "ok", "sqs": "ok"}
	if err := a.readiness.pool.Ping(ctx); err != nil {
		checks["postgres"] = "unavailable"
		status = http.StatusServiceUnavailable
	}
	if err := a.readiness.consumer.CheckReady(ctx); err != nil {
		checks["sqs"] = "unavailable"
		status = http.StatusServiceUnavailable
	}
	if status != http.StatusOK {
		checks["status"] = "unavailable"
	}
	writeJSON(w, status, checks)
}
