package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type OutboxRepository struct{ db DBTX }

var _ ports.OutboxRepository = (*OutboxRepository)(nil)

func (r *OutboxRepository) Create(ctx context.Context, event ports.IntegrationEvent) error {
	if event == nil {
		return domain.ErrInvalidEvent
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serialize outbox event: %w", err)
	}
	var envelope struct {
		EventID     uuid.UUID `json:"eventId"`
		AggregateID uuid.UUID `json:"aggregateId"`
		EventType   string    `json:"eventType"`
		OccurredAt  time.Time `json:"occurredAt"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("read event envelope: %w", err)
	}
	if envelope.EventID == uuid.Nil || envelope.EventID != event.EventID() || envelope.AggregateID == uuid.Nil || envelope.OccurredAt.IsZero() {
		return domain.ErrInvalidEvent
	}
	result, err := r.db.Exec(ctx, `INSERT INTO outbox (event_id, aggregate_id, event_type, payload, occurred_at)
		VALUES ($1,$2,$3,$4,($4::jsonb->>'occurredAt')::timestamptz)
		ON CONFLICT (event_id) DO NOTHING`, envelope.EventID, envelope.AggregateID, envelope.EventType, string(payload))
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	var same bool
	err = r.db.QueryRow(ctx, `SELECT payload=$2::jsonb FROM outbox WHERE event_id=$1`, envelope.EventID, string(payload)).Scan(&same)
	if err != nil {
		return fmt.Errorf("compare outbox event: %w", err)
	}
	if !same {
		return ports.ErrOutboxConflict
	}
	return nil
}
