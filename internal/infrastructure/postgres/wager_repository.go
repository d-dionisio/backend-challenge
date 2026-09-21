package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type WagerRepository struct{ db DBTX }

var _ ports.WagerRepository = (*WagerRepository)(nil)

func (r *WagerRepository) Create(ctx context.Context, transaction *domain.WagerTransaction) error {
	if transaction == nil {
		return domain.ErrInvalidWagerTransaction
	}
	var resultBalance *int64
	if balance := transaction.ResultBalance(); balance != nil {
		amount := balance.Amount()
		resultBalance = &amount
	}
	result, err := r.db.Exec(ctx, `INSERT INTO wager_transactions
		(id, provider_id, external_transaction_id, idempotency_key, payload_hash,
		 wallet_id, player_id, round_id, game_id, kind, amount_cents, currency,
		 reference_external_transaction_id, reference_transaction_id, status,
		 failure_code, result_balance_cents, created_at, updated_at)
		VALUES ($1,NULLIF($2,''),NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),
		 $6,$7,NULLIF($8,''),NULLIF($9,''),$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
		ON CONFLICT DO NOTHING`,
		transaction.ID(), transaction.ProviderID(), transaction.ExternalTransactionID(),
		transaction.IdempotencyKey(), transaction.PayloadHash(), transaction.WalletID(),
		transaction.PlayerID(), transaction.RoundID(), transaction.GameID(), transaction.Kind(),
		transaction.Money().Amount(), transaction.Money().Currency(), transaction.ReferenceExternalTransactionID(),
		transaction.ReferenceTransactionID(), transaction.Status(), transaction.FailureCode(), resultBalance,
		transaction.CreatedAt(), transaction.UpdatedAt())
	if err != nil {
		return fmt.Errorf("insert wager transaction: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ports.ErrWagerConflict
	}
	return nil
}

const wagerColumns = `SELECT id, COALESCE(provider_id,''), COALESCE(external_transaction_id,''),
	COALESCE(idempotency_key,''), COALESCE(payload_hash,''), wallet_id, player_id,
	COALESCE(round_id,''), COALESCE(game_id,''), kind, amount_cents, currency,
	reference_external_transaction_id, reference_transaction_id, status, failure_code,
	result_balance_cents, created_at, updated_at FROM wager_transactions `

func (r *WagerRepository) FindByID(ctx context.Context, providerID string, id uuid.UUID) (*domain.WagerTransaction, error) {
	return scanWager(r.db.QueryRow(ctx, wagerColumns+`WHERE COALESCE(provider_id,'')=$1 AND id=$2`, providerID, id))
}

func (r *WagerRepository) FindByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return scanWager(r.db.QueryRow(ctx, wagerColumns+`WHERE provider_id=$1 AND external_transaction_id=$2`, providerID, externalID))
}

func (r *WagerRepository) FindByIdempotencyKey(ctx context.Context, providerID, key string) (*domain.WagerTransaction, error) {
	return scanWager(r.db.QueryRow(ctx, wagerColumns+`WHERE provider_id=$1 AND idempotency_key=$2`, providerID, key))
}

func scanWager(row pgx.Row) (*domain.WagerTransaction, error) {
	var state domain.WagerTransactionState
	var amount int64
	var currency string
	var resultBalance *int64
	err := row.Scan(&state.ID, &state.ProviderID, &state.ExternalTransactionID,
		&state.IdempotencyKey, &state.PayloadHash, &state.WalletID, &state.PlayerID,
		&state.RoundID, &state.GameID, &state.Kind, &amount, &currency,
		&state.ReferenceExternalTransactionID, &state.ReferenceTransactionID,
		&state.Status, &state.FailureCode, &resultBalance, &state.CreatedAt, &state.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrWagerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read wager transaction: %w", err)
	}
	state.Money, err = domain.MoneyFromMinorUnits(amount, currency)
	if err != nil {
		return nil, fmt.Errorf("rehydrate wager money: %w", err)
	}
	if resultBalance != nil {
		balance, err := domain.MoneyFromMinorUnits(*resultBalance, currency)
		if err != nil {
			return nil, fmt.Errorf("rehydrate wager result: %w", err)
		}
		state.ResultBalance = &balance
	}
	transaction, err := domain.RehydrateWagerTransaction(state)
	if err != nil {
		return nil, fmt.Errorf("rehydrate wager: %w", err)
	}
	return transaction, nil
}

func (r *WagerRepository) HasSuccessfulReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id=$1 AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED')`, referenceID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check reversal: %w", err)
	}
	return exists, nil
}

func (r *WagerRepository) Update(ctx context.Context, transaction *domain.WagerTransaction) error {
	if transaction == nil {
		return domain.ErrInvalidWagerTransaction
	}
	var resultBalance *int64
	if balance := transaction.ResultBalance(); balance != nil {
		amount := balance.Amount()
		resultBalance = &amount
	}
	result, err := r.db.Exec(ctx, `UPDATE wager_transactions SET status=$2,
		reference_transaction_id=$3, failure_code=$4, result_balance_cents=$5, updated_at=$6
		WHERE id=$1 AND status IN ('PENDING','PENDING_REFERENCE')`, transaction.ID(),
		transaction.Status(), transaction.ReferenceTransactionID(), transaction.FailureCode(), resultBalance, transaction.UpdatedAt())
	if err != nil {
		return fmt.Errorf("update wager transaction: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ports.ErrWagerNotPending
	}
	return nil
}
