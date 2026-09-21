package domain

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

// Dados que o repositório lê do banco. Não executam uma operação financeira.
type WagerTransactionState struct {
	ID                             uuid.UUID
	ExternalTransactionID          string
	ProviderID                     string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           WagerKind
	Money                          Money
	ReferenceExternalTransactionID *string
	ReferenceTransactionID         *uuid.UUID
	Status                         WagerStatus
	FailureCode                    *string
	ResultBalance                  *Money
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

func RehydrateWagerTransaction(state WagerTransactionState) (*WagerTransaction, error) {
	transaction := &WagerTransaction{
		id:                             state.ID,
		externalTransactionID:          state.ExternalTransactionID,
		providerID:                     state.ProviderID,
		idempotencyKey:                 state.IdempotencyKey,
		payloadHash:                    state.PayloadHash,
		walletID:                       state.WalletID,
		playerID:                       state.PlayerID,
		roundID:                        state.RoundID,
		gameID:                         state.GameID,
		kind:                           state.Kind,
		money:                          state.Money,
		referenceExternalTransactionID: copyString(state.ReferenceExternalTransactionID),
		status:                         state.Status,
		failureCode:                    copyString(state.FailureCode),
		createdAt:                      state.CreatedAt,
		updatedAt:                      state.UpdatedAt,
	}
	if state.ReferenceTransactionID != nil {
		id := *state.ReferenceTransactionID
		transaction.referenceTransactionID = &id
	}
	if state.ResultBalance != nil {
		balance := *state.ResultBalance
		transaction.resultBalance = &balance
	}
	if err := transaction.validate(); err != nil {
		return nil, err
	}
	return transaction, nil
}

// A abertura usa o saldo já definido na carteira. Não chama Credit novamente.
func NewOpeningTransaction(wallet *Wallet) (*WagerTransaction, error) {
	if wallet == nil || wallet.ID() == uuid.Nil || wallet.PlayerID() == uuid.Nil {
		return nil, ErrInvalidWallet
	}
	if wallet.Version() != 1 || !wallet.Balance().IsPositive() {
		return nil, ErrInvalidWallet
	}
	transaction := &WagerTransaction{
		// Para a mesma carteira, a identidade da abertura é sempre a mesma.
		id:        uuid.NewSHA1(wallet.ID(), []byte("OPENING")),
		walletID:  wallet.ID(),
		playerID:  wallet.PlayerID(),
		kind:      WagerKindOpening,
		money:     wallet.Balance(),
		status:    WagerStatusPending,
		createdAt: wallet.CreatedAt(),
		updatedAt: wallet.CreatedAt(),
	}
	if err := transaction.MarkProcessed(wallet.Balance()); err != nil {
		return nil, err
	}
	return transaction, nil
}

func (w *WagerTransaction) validate() error {
	if w == nil || w.id == uuid.Nil || w.walletID == uuid.Nil || w.playerID == uuid.Nil {
		return ErrInvalidWagerTransaction
	}
	if !w.kind.IsValid() {
		return ErrInvalidWagerKind
	}
	if w.createdAt.IsZero() || w.updatedAt.Before(w.createdAt) {
		return ErrInvalidWagerTransaction
	}
	if err := validateWagerMoney(w.kind, w.money, w.referenceExternalTransactionID); err != nil {
		return err
	}
	if w.kind == WagerKindOpening {
		if w.providerID != "" || w.externalTransactionID != "" || w.idempotencyKey != "" || w.payloadHash != "" {
			return ErrInvalidWagerTransaction
		}
		if w.roundID != "" || w.gameID != "" || w.referenceExternalTransactionID != nil || w.referenceTransactionID != nil {
			return ErrInvalidWagerTransaction
		}
	} else {
		if strings.TrimSpace(w.providerID) == "" || strings.TrimSpace(w.externalTransactionID) == "" {
			return ErrInvalidWagerTransaction
		}
		if strings.TrimSpace(w.idempotencyKey) == "" || strings.TrimSpace(w.payloadHash) == "" {
			return ErrInvalidWagerTransaction
		}
		if strings.TrimSpace(w.roundID) == "" || strings.TrimSpace(w.gameID) == "" {
			return ErrInvalidWagerTransaction
		}
	}
	if w.referenceTransactionID != nil {
		if *w.referenceTransactionID == uuid.Nil || *w.referenceTransactionID == w.id || !w.needsReference() {
			return ErrInvalidWagerReference
		}
	}
	if w.resultBalance != nil {
		if w.resultBalance.Validate() != nil || w.resultBalance.Amount() < 0 {
			return ErrInvalidWagerMoney
		}
		if w.resultBalance.Currency() != w.money.Currency() {
			return ErrCurrencyMismatch
		}
	}
	switch w.status {
	case WagerStatusPending, WagerStatusPendingReference:
		if w.failureCode != nil || w.resultBalance != nil {
			return ErrInvalidWagerStatus
		}
		if w.status == WagerStatusPendingReference && !w.needsReference() {
			return ErrInvalidWagerStatus
		}
	case WagerStatusProcessed:
		if w.failureCode != nil || w.resultBalance == nil {
			return ErrInvalidWagerStatus
		}
		if w.needsReference() && w.referenceTransactionID == nil {
			return ErrReferenceRequired
		}
		if w.kind == WagerKindOpening && *w.resultBalance != w.money {
			return ErrInvalidWagerMoney
		}
	case WagerStatusRejected, WagerStatusFailed:
		if w.failureCode == nil || strings.TrimSpace(*w.failureCode) == "" || w.resultBalance != nil {
			return ErrInvalidWagerStatus
		}
	default:
		return ErrInvalidWagerStatus
	}
	return nil
}
