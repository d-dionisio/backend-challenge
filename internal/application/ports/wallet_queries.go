package ports

import (
	"context"
	"errors"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

var ErrInvalidFinancialState = errors.New("stored financial state cannot be represented")

// Dados de leitura. Nenhum destes métodos altera carteira ou ledger.
type WalletQueries interface {
	ListLedger(context.Context, uuid.UUID, *LedgerPosition, int) ([]LedgerRecord, error)
	ReconciliationSnapshot(context.Context, uuid.UUID) (*ReconciliationSnapshot, error)
}

type LedgerPosition struct {
	WalletID  uuid.UUID `json:"walletId"`
	CreatedAt time.Time `json:"createdAt"`
	ID        uuid.UUID `json:"id"`
}

type LedgerRecord struct {
	ID            uuid.UUID              `json:"id"`
	WalletID      uuid.UUID              `json:"walletId"`
	TransactionID uuid.UUID              `json:"transactionId"`
	Direction     domain.LedgerDirection `json:"direction"`
	Money         domain.Money           `json:"money"`
	BalanceBefore domain.Money           `json:"balanceBefore"`
	BalanceAfter  domain.Money           `json:"balanceAfter"`
	CreatedAt     time.Time              `json:"createdAt"`
}

type ReconciliationSnapshot struct {
	WalletID          uuid.UUID
	StoredBalance     domain.Money
	CalculatedBalance domain.Money
	CheckedEntries    int64
}
