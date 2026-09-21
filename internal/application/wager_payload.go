package application

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type ProcessWagerInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           domain.WagerKind
	Money                          domain.Money
	ReferenceExternalTransactionID *string
	CorrelationID                  string
	CausationID                    string
}

// Os campos estão em ordem alfabética, inclusive dentro de money.
// Chave de idempotência e rastreamento não fazem parte do conteúdo financeiro.
func (input ProcessWagerInput) payloadHash() (string, error) {
	if err := input.Money.Validate(); err != nil {
		return "", err
	}
	payload := struct {
		ExternalTransactionID string           `json:"externalTransactionId"`
		GameID                string           `json:"gameId"`
		Kind                  domain.WagerKind `json:"kind"`
		Money                 struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		PlayerID                       string  `json:"playerId"`
		ProviderID                     string  `json:"providerId"`
		ReferenceExternalTransactionID *string `json:"referenceExternalTransactionId,omitempty"`
		RoundID                        string  `json:"roundId"`
		WalletID                       string  `json:"walletId"`
	}{
		ExternalTransactionID:          input.ExternalTransactionID,
		GameID:                         input.GameID,
		Kind:                           input.Kind,
		PlayerID:                       input.PlayerID.String(),
		ProviderID:                     input.ProviderID,
		ReferenceExternalTransactionID: input.ReferenceExternalTransactionID,
		RoundID:                        input.RoundID,
		WalletID:                       input.WalletID.String(),
	}
	payload.Money.Amount = input.Money.String()
	payload.Money.Currency = input.Money.Currency()
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}
