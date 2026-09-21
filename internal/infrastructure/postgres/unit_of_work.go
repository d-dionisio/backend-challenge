package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UnitOfWork struct{ pool *pgxpool.Pool }

func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

var _ ports.UnitOfWork = (*UnitOfWork)(nil)

func (u *UnitOfWork) WithinTransaction(ctx context.Context, work func(ports.Repositories) error) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Também libera a conexão quando o chamador cancela ou ocorre panic.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	repositories := ports.Repositories{
		Wallets: &WalletRepository{db: tx, inTransaction: true},
		Wagers:  &WagerRepository{db: tx},
		Ledger:  &LedgerRepository{db: tx},
		Inbox:   &InboxRepository{db: tx},
		Outbox:  &OutboxRepository{db: tx},
	}
	if err := work(repositories); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
