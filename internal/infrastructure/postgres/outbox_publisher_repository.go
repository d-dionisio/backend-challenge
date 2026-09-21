package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxPublisherRepository struct{ pool *pgxpool.Pool }

func NewOutboxPublisherRepository(pool *pgxpool.Pool) *OutboxPublisherRepository {
	return &OutboxPublisherRepository{pool: pool}
}

var _ ports.OutboxPublisherRepository = (*OutboxPublisherRepository)(nil)

func (r *OutboxPublisherRepository) Claim(ctx context.Context, lease time.Duration) (*ports.PendingEvent, error) {
	event := &ports.PendingEvent{}
	// A seleção e a reserva acontecem em um único comando, com commit
	// automático. SKIP LOCKED permite que outro publisher escolha outra linha.
	err := r.pool.QueryRow(ctx, `WITH candidate AS (
		SELECT event_id FROM outbox
		WHERE published_at IS NULL AND next_attempt_at <= clock_timestamp()
		  AND (locked_until IS NULL OR locked_until <= clock_timestamp())
		ORDER BY next_attempt_at, event_id
		LIMIT 1 FOR UPDATE SKIP LOCKED
	)
	UPDATE outbox o SET lock_token=$1,
		locked_until=clock_timestamp()+$2::bigint*interval '1 microsecond',
		attempts=LEAST(o.attempts::bigint+1,2147483647)::integer
	FROM candidate c WHERE o.event_id=c.event_id
	RETURNING o.event_id,o.aggregate_id,o.event_type,o.payload->>'correlationId',
		COALESCE(o.payload->'data'->>'transactionId',''),COALESCE(o.payload->'data'->>'providerId',''),
		o.payload,o.attempts,o.occurred_at,o.lock_token`, uuid.New(), lease.Microseconds()).Scan(
		&event.EventID, &event.AggregateID, &event.EventType, &event.CorrelationID,
		&event.TransactionID, &event.ProviderID,
		&event.Payload, &event.Attempts, &event.OccurredAt, &event.LockToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (r *OutboxPublisherRepository) MarkPublished(ctx context.Context, event *ports.PendingEvent) error {
	result, err := r.pool.Exec(ctx, `UPDATE outbox
		SET published_at=clock_timestamp(),locked_until=NULL,lock_token=NULL
		WHERE event_id=$1 AND lock_token=$2 AND published_at IS NULL
		  AND locked_until>clock_timestamp()`, event.EventID, event.LockToken)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ports.ErrOutboxLeaseLost
	}
	return nil
}

func (r *OutboxPublisherRepository) Retry(ctx context.Context, event *ports.PendingEvent, delay time.Duration) error {
	result, err := r.pool.Exec(ctx, `UPDATE outbox
		SET next_attempt_at=clock_timestamp()+$3::bigint*interval '1 microsecond',
		    locked_until=NULL,lock_token=NULL
		WHERE event_id=$1 AND lock_token=$2 AND published_at IS NULL
		  AND locked_until>clock_timestamp()`, event.EventID, event.LockToken, delay.Microseconds())
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ports.ErrOutboxLeaseLost
	}
	return nil
}
