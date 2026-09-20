package ports

import (
	"context"
	"errors"

	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

var (
	ErrWalletNotFound         = errors.New("wallet not found")
	ErrWalletConflict         = errors.New("wallet conflict")
	ErrWalletConcurrentUpdate = errors.New("wallet concurrent update")
)

type WalletRepository interface {
	Create(ctx context.Context, wallet *domain.Wallet) error

	FindByID(ctx context.Context, walletID uuid.UUID) (*domain.Wallet, error)

	Update(ctx context.Context, wallet *domain.Wallet) error
}
