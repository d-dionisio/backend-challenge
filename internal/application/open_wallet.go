package application

import (
	"context"
	"strings"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type OpenWalletInput struct {
	PlayerID       uuid.UUID
	InitialBalance domain.Money
	CorrelationID  string
}

type OpenWallet struct {
	unitOfWork ports.UnitOfWork
}

func NewOpenWallet(unitOfWork ports.UnitOfWork) *OpenWallet {
	return &OpenWallet{unitOfWork: unitOfWork}
}

func (useCase *OpenWallet) Execute(ctx context.Context, input OpenWalletInput) (*domain.Wallet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	wallet, err := domain.NewWallet(input.PlayerID, input.InitialBalance)
	if err != nil {
		return nil, err
	}
	correlationID := input.CorrelationID
	if strings.TrimSpace(correlationID) == "" {
		correlationID = uuid.NewString()
	}

	err = useCase.unitOfWork.WithinTransaction(ctx, func(repositories ports.Repositories) error {
		if err := repositories.Wallets.Create(ctx, wallet); err != nil {
			return err
		}
		// Saldo zero não gera operação financeira, ledger nem eventos.
		if wallet.Balance().IsZero() {
			return nil
		}

		opening, err := domain.NewOpeningTransaction(wallet)
		if err != nil {
			return err
		}
		if err := repositories.Wagers.Create(ctx, opening); err != nil {
			return err
		}
		before, err := domain.ZeroMoney(wallet.Balance().Currency())
		if err != nil {
			return err
		}
		entry, err := domain.NewWalletLedgerEntry(wallet.ID(), opening.ID(),
			domain.LedgerDirectionCredit, wallet.Balance(), before, wallet.Balance())
		if err != nil {
			return err
		}
		if err := repositories.Ledger.Create(ctx, entry); err != nil {
			return err
		}

		processed, err := domain.NewWagerTransactionProcessed(opening, correlationID, "")
		if err != nil {
			return err
		}
		changed, err := domain.NewWalletBalanceChanged(opening, entry, wallet.Version(), correlationID, "")
		if err != nil {
			return err
		}
		if err := repositories.Outbox.Create(ctx, processed); err != nil {
			return err
		}
		return repositories.Outbox.Create(ctx, changed)
	})
	if err != nil {
		return nil, err
	}
	// Só devolvemos a carteira depois da confirmação do commit.
	return wallet, nil
}
