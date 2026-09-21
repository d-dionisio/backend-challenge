package httpapi

import (
	"time"

	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type moneyRequest struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type openWalletRequest struct {
	PlayerID       uuid.UUID    `json:"playerId"`
	InitialBalance moneyRequest `json:"initialBalance"`
}

type wagerRequest struct {
	ProviderID                     string           `json:"providerId"`
	ExternalTransactionID          string           `json:"externalTransactionId"`
	PlayerID                       uuid.UUID        `json:"playerId"`
	WalletID                       uuid.UUID        `json:"walletId"`
	RoundID                        string           `json:"roundId"`
	GameID                         string           `json:"gameId"`
	Kind                           domain.WagerKind `json:"kind"`
	Money                          moneyRequest     `json:"money"`
	ReferenceExternalTransactionID *string          `json:"referenceExternalTransactionId,omitempty"`
}

type walletResponse struct {
	ID       uuid.UUID    `json:"id"`
	PlayerID uuid.UUID    `json:"playerId"`
	Balance  domain.Money `json:"balance"`
	Version  int64        `json:"version"`
}

func walletBody(wallet *domain.Wallet) walletResponse {
	return walletResponse{ID: wallet.ID(), PlayerID: wallet.PlayerID(), Balance: wallet.Balance(), Version: wallet.Version()}
}

type transactionResponse struct {
	TransactionID                  uuid.UUID          `json:"transactionId"`
	ProviderID                     string             `json:"providerId"`
	ExternalTransactionID          string             `json:"externalTransactionId"`
	IdempotencyKey                 string             `json:"idempotencyKey"`
	PlayerID                       uuid.UUID          `json:"playerId"`
	WalletID                       uuid.UUID          `json:"walletId"`
	RoundID                        string             `json:"roundId"`
	GameID                         string             `json:"gameId"`
	Kind                           domain.WagerKind   `json:"kind"`
	Money                          domain.Money       `json:"money"`
	ReferenceExternalTransactionID *string            `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID         `json:"referenceTransactionId,omitempty"`
	Status                         domain.WagerStatus `json:"status"`
	Balance                        *domain.Money      `json:"balance,omitempty"`
	FailureCode                    *string            `json:"failureCode,omitempty"`
	CreatedAt                      time.Time          `json:"createdAt"`
	UpdatedAt                      time.Time          `json:"updatedAt"`
}

func transactionBody(transaction *domain.WagerTransaction) transactionResponse {

	return transactionResponse{
		TransactionID:                  transaction.ID(),
		ProviderID:                     transaction.ProviderID(),
		ExternalTransactionID:          transaction.ExternalTransactionID(),
		IdempotencyKey:                 transaction.IdempotencyKey(),
		PlayerID:                       transaction.PlayerID(),
		WalletID:                       transaction.WalletID(),
		RoundID:                        transaction.RoundID(),
		GameID:                         transaction.GameID(),
		Kind:                           transaction.Kind(),
		Money:                          transaction.Money(),
		ReferenceExternalTransactionID: transaction.ReferenceExternalTransactionID(),
		ReferenceTransactionID:         transaction.ReferenceTransactionID(),
		Status:                         transaction.Status(),
		Balance:                        transaction.ResultBalance(),
		FailureCode:                    transaction.FailureCode(),
		CreatedAt:                      transaction.CreatedAt().UTC(),
		UpdatedAt:                      transaction.UpdatedAt().UTC(),
	}
}
