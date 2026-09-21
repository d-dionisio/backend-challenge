package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)

	QueryRow(context.Context, string, ...any) pgx.Row
}

type WalletRepository struct {
	db            DBTX
	inTransaction bool
}

func NewWalletRepository(pool *pgxpool.Pool) *WalletRepository {
	return &WalletRepository{
		db: pool,
	}
}

var _ ports.WalletRepository = (*WalletRepository)(nil)

func (r *WalletRepository) WithTx(tx pgx.Tx) *WalletRepository {

	return &WalletRepository{
		db:            tx,
		inTransaction: tx != nil,
	}
}

func (r *WalletRepository) Create(ctx context.Context, wallet *domain.Wallet) error {

	const query = `
		INSERT INTO wallets (
			id,
			player_id,
			balance_cents,
			currency,
			version,
			created_at,
			updated_at
		)
		VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7
		)
	`

	_, err := r.db.Exec(
		ctx,
		query,
		wallet.ID(),
		wallet.PlayerID(),
		wallet.Balance().Amount(),
		wallet.Balance().Currency(),
		wallet.Version(),
		wallet.CreatedAt(),
		wallet.UpdatedAt(),
	)

	if err != nil {
		if isUniqueViolation(err) {
			return ports.ErrWalletConflict
		}

		return fmt.Errorf(
			"insert wallet: %w",
			err,
		)
	}

	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError

	if !errors.As(err, &pgErr) {
		return false
	}

	return pgErr.Code == "23505"
}

func (r *WalletRepository) FindByID(ctx context.Context, walletID uuid.UUID) (*domain.Wallet, error) {
	return r.findByID(ctx, walletID, false)
}

// O lock precisa durar até o commit ou rollback da transação do chamador.
func (r *WalletRepository) FindByIDForUpdate(ctx context.Context, walletID uuid.UUID) (*domain.Wallet, error) {
	if !r.inTransaction {
		return nil, ports.ErrTransactionRequired
	}
	return r.findByID(ctx, walletID, true)
}

func (r *WalletRepository) findByID(ctx context.Context, walletID uuid.UUID, lock bool) (*domain.Wallet, error) {

	query := `
		SELECT
			id,
			player_id,
			balance_cents,
			currency,
			version,
			created_at,
			updated_at
		FROM wallets
		WHERE id = $1
	`
	if lock {
		query += " FOR UPDATE"
	}

	var (
		id           uuid.UUID
		playerID     uuid.UUID
		balanceCents int64
		currency     string
		version      int64
		createdAt    time.Time
		updatedAt    time.Time
	)

	err := r.db.QueryRow(
		ctx,
		query,
		walletID,
	).Scan(
		&id,
		&playerID,
		&balanceCents,
		&currency,
		&version,
		&createdAt,
		&updatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ports.ErrWalletNotFound
		}

		return nil, fmt.Errorf(
			"select wallet: %w",
			err,
		)
	}

	money, err := domain.MoneyFromMinorUnits(
		balanceCents,
		currency,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"rehydrate wallet money: %w",
			err,
		)
	}

	wallet, err := domain.RehydrateWallet(
		id,
		playerID,
		money,
		version,
		createdAt,
		updatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"rehydrate wallet: %w",
			err,
		)
	}

	return wallet, nil
}

func (r *WalletRepository) Update(ctx context.Context, wallet *domain.Wallet) error {
	const query = `
		UPDATE wallets
		SET
			balance_cents = $1,
			version = $2,
			updated_at = $3
		WHERE
			id = $4
			AND version = $5
	`

	previousVersion := wallet.Version() - 1

	result, err := r.db.Exec(
		ctx,
		query,
		wallet.Balance().Amount(),
		wallet.Version(),
		wallet.UpdatedAt(),
		wallet.ID(),
		previousVersion,
	)

	if err != nil {
		return fmt.Errorf("update wallet: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ports.ErrWalletConcurrentUpdate
	}

	return nil
}
