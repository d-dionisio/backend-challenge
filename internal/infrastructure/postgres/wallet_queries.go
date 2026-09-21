package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WalletQueries struct{ pool *pgxpool.Pool }

func NewWalletQueries(pool *pgxpool.Pool) *WalletQueries { return &WalletQueries{pool: pool} }

var _ ports.WalletQueries = (*WalletQueries)(nil)

func (r *WalletQueries) ListLedger(ctx context.Context, walletID uuid.UUID, position *ports.LedgerPosition, limit int) ([]ports.LedgerRecord, error) {
	// Distingue uma carteira sem lançamentos de uma carteira inexistente.
	if _, err := NewWalletRepository(r.pool).FindByID(ctx, walletID); err != nil {
		return nil, err
	}
	query := `SELECT id, wallet_id, transaction_id, direction, amount_cents, currency,
		balance_before_cents, balance_after_cents, created_at
		FROM wallet_ledger_entries WHERE wallet_id=$1`
	args := []any{walletID, limit}
	if position != nil {
		query += ` AND (created_at,id) < ($3,$4)`
		args = append(args, position.CreatedAt, position.ID)
	}
	query += ` ORDER BY created_at DESC,id DESC LIMIT $2`
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list ledger: %w", err)
	}
	defer rows.Close()
	items := []ports.LedgerRecord{}
	for rows.Next() {
		var entry ports.LedgerRecord
		var amount, before, after int64
		var currency string
		if err := rows.Scan(&entry.ID, &entry.WalletID, &entry.TransactionID, &entry.Direction, &amount, &currency, &before, &after, &entry.CreatedAt); err != nil {
			return nil, err
		}
		entry.Money, err = domain.MoneyFromMinorUnits(amount, currency)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ports.ErrInvalidFinancialState, err)
		}
		entry.BalanceBefore, err = domain.MoneyFromMinorUnits(before, currency)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ports.ErrInvalidFinancialState, err)
		}
		entry.BalanceAfter, err = domain.MoneyFromMinorUnits(after, currency)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ports.ErrInvalidFinancialState, err)
		}
		entry.CreatedAt = entry.CreatedAt.UTC()
		items = append(items, entry)
	}
	return items, rows.Err()
}

func (r *WalletQueries) ReconciliationSnapshot(ctx context.Context, walletID uuid.UUID) (*ports.ReconciliationSnapshot, error) {
	// Um único SELECT vê saldo e ledger no mesmo snapshot do PostgreSQL.
	// NUMERIC evita overflow intermediário quando o histórico movimentou mais
	// que int64, mesmo que o saldo líquido ainda caiba em Money.
	const query = `SELECT w.id,w.balance_cents,w.currency,
		COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount_cents::NUMERIC
			ELSE -l.amount_cents::NUMERIC END),0)::TEXT,COUNT(l.id)
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id
		WHERE w.id=$1 GROUP BY w.id`
	var result ports.ReconciliationSnapshot
	var stored int64
	var calculated, currency string
	err := r.pool.QueryRow(ctx, query, walletID).Scan(&result.WalletID, &stored, &currency, &calculated, &result.CheckedEntries)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrWalletNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reconcile wallet: %w", err)
	}
	calculatedCents, err := strconv.ParseInt(calculated, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: calculated balance out of range", ports.ErrInvalidFinancialState)
	}
	result.StoredBalance, err = domain.MoneyFromMinorUnits(stored, currency)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ports.ErrInvalidFinancialState, err)
	}
	result.CalculatedBalance, err = domain.MoneyFromMinorUnits(calculatedCents, currency)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ports.ErrInvalidFinancialState, err)
	}
	return &result, nil
}
