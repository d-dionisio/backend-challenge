package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (r *WagerRepository) ClaimPendingReference(ctx context.Context) (*ports.PendingReference, error) {
	var walletID uuid.UUID
	// O primeiro lock é sempre o da carteira, como no processamento normal.
	err := r.db.QueryRow(ctx, `SELECT w.id FROM wager_transactions t JOIN wallets w ON w.id=t.wallet_id
		WHERE t.status='PENDING_REFERENCE' AND t.next_reference_attempt_at<=clock_timestamp()
		ORDER BY t.next_reference_attempt_at,t.id LIMIT 1 FOR UPDATE OF w SKIP LOCKED`).Scan(&walletID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim reference wallet: %w", err)
	}
	var transactionID uuid.UUID
	var providerID string
	var pending ports.PendingReference
	err = r.db.QueryRow(ctx, `SELECT id,provider_id,reference_attempts FROM wager_transactions
		WHERE wallet_id=$1 AND status='PENDING_REFERENCE' AND next_reference_attempt_at<=clock_timestamp()
		ORDER BY next_reference_attempt_at,id LIMIT 1 FOR UPDATE`, walletID).Scan(&transactionID, &providerID, &pending.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim reference transaction: %w", err)
	}
	pending.Transaction, err = r.FindByID(ctx, providerID, transactionID)
	if err != nil {
		return nil, err
	}
	// O evento de espera já contém o rastreamento original confirmado no banco.
	err = r.db.QueryRow(ctx, `SELECT payload->>'correlationId',COALESCE(payload->>'causationId','')
		FROM outbox WHERE aggregate_id=$1 AND event_type='WagerTransactionPendingReference'
		AND payload->'data'->>'transactionId'=$2 ORDER BY occurred_at,event_id LIMIT 1`, walletID, transactionID.String()).Scan(&pending.CorrelationID, &pending.CausationID)
	if err != nil {
		return nil, fmt.Errorf("read reference event metadata: %w", err)
	}
	return &pending, nil
}

func (r *WagerRepository) ScheduleReferenceRetry(ctx context.Context, id uuid.UUID, attempts int, delay time.Duration) error {
	result, err := r.db.Exec(ctx, `UPDATE wager_transactions SET reference_attempts=$2,
		next_reference_attempt_at=clock_timestamp()+$3::bigint*interval '1 microsecond'
		WHERE id=$1 AND status='PENDING_REFERENCE'`, id, attempts, delay.Microseconds())
	if err != nil {
		return fmt.Errorf("schedule reference retry: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ports.ErrWagerNotPending
	}
	return nil
}
