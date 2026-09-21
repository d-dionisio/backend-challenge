package application

import (
	"context"
	"fmt"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type ReconciliationResult struct {
	WalletID          uuid.UUID    `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
}

type ReconcileWallet struct{ queries ports.WalletQueries }

func NewReconcileWallet(queries ports.WalletQueries) *ReconcileWallet {
	return &ReconcileWallet{queries: queries}
}

func (useCase *ReconcileWallet) Execute(ctx context.Context, walletID uuid.UUID) (*ReconciliationResult, error) {
	if walletID == uuid.Nil {
		return nil, domain.ErrInvalidWallet
	}
	snapshot, err := useCase.queries.ReconciliationSnapshot(ctx, walletID)
	if err != nil {
		return nil, err
	}
	difference, err := snapshot.StoredBalance.Sub(snapshot.CalculatedBalance)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ports.ErrInvalidFinancialState, err)
	}
	return &ReconciliationResult{
		WalletID:          snapshot.WalletID,
		StoredBalance:     snapshot.StoredBalance,
		CalculatedBalance: snapshot.CalculatedBalance,
		Difference:        difference,
		Consistent:        difference.IsZero(),
		CheckedEntries:    snapshot.CheckedEntries,
	}, nil
}
