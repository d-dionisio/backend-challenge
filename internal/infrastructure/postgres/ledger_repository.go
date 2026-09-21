package postgres

import (
	"context"
	"fmt"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
)

type LedgerRepository struct{ db DBTX }

var _ ports.LedgerRepository = (*LedgerRepository)(nil)

func (r *LedgerRepository) Create(ctx context.Context, entry *domain.WalletLedgerEntry) error {
	if entry == nil {
		return domain.ErrInvalidLedgerEntry
	}
	_, err := r.db.Exec(ctx, `INSERT INTO wallet_ledger_entries
		(id, wallet_id, transaction_id, direction, amount_cents, currency,
		 balance_before_cents, balance_after_cents, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		entry.ID(), entry.WalletID(), entry.TransactionID(), entry.Direction(),
		entry.Money().Amount(), entry.Money().Currency(), entry.BalanceBefore().Amount(),
		entry.BalanceAfter().Amount(), entry.CreatedAt())
	if err != nil {
		return fmt.Errorf("insert ledger entry: %w", err)
	}
	return nil
}
