package observability

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestConflictClassificationAndDLQSemantics(t *testing.T) {
	m := NewMetrics()
	for _, err := range []error{application.ErrIdempotencyConflict, ports.ErrWalletConcurrentUpdate, &pgconn.PgError{Code: "40P01"}} {
		m.ObserveProcessingError(fmt.Errorf("wrapped: %w", err), time.Millisecond)
	}
	m.ObserveWagerResult("PROCESSED", true, time.Millisecond)
	m.ObserveDeadLetter("invalid_message")
	m.ObserveDeadLetter("invalid_message")
	m.SetDLQDepth(1)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{
		`wager_concurrency_conflicts_total{scope="idempotency_conflict"} 1`,
		`wager_concurrency_conflicts_total{scope="wallet_version"} 1`,
		`wager_concurrency_conflicts_total{scope="database_conflict"} 1`,
		`wager_duplicates_total{source="idempotent_replay"} 1`,
		`wager_dlq_redrive_decisions_total{reason="invalid_message"} 2`,
		"wager_dlq_messages 1", "wager_processing_latency_seconds_count 4",
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("missing %s in %s", want, w.Body.String())
		}
	}
}
