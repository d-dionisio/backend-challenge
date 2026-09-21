package messaging

import (
	"encoding/json"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type WagerMessage struct {
	MessageID     string    `json:"messageId"`
	Type          string    `json:"type"`
	OccurredAt    time.Time `json:"occurredAt"`
	CorrelationID string    `json:"correlationId,omitempty"`
	Data          struct {
		ProviderID            string           `json:"providerId"`
		ExternalTransactionID string           `json:"externalTransactionId"`
		IdempotencyKey        string           `json:"idempotencyKey"`
		PlayerID              uuid.UUID        `json:"playerId"`
		WalletID              uuid.UUID        `json:"walletId"`
		RoundID               string           `json:"roundId"`
		GameID                string           `json:"gameId"`
		Kind                  domain.WagerKind `json:"kind"`
		Money                 struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		ReferenceExternalTransactionID *string `json:"referenceExternalTransactionId,omitempty"`
	} `json:"data"`
}

func ParseWagerMessage(body string) (WagerMessage, application.ProcessWagerInput, error) {
	var message WagerMessage
	var input application.ProcessWagerInput
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	if !utf8.ValidString(body) || decoder.Decode(&message) != nil {
		return message, input, application.ErrInvalidMessage
	}
	var extra json.RawMessage
	if decoder.Decode(&extra) != io.EOF || strings.TrimSpace(message.MessageID) == "" ||
		message.Type != "WagerTransactionRequested" || message.OccurredAt.IsZero() {
		return message, input, application.ErrInvalidMessage
	}
	money, err := domain.NewMoney(message.Data.Money.Amount, message.Data.Money.Currency)
	if err != nil {
		return message, input, application.ErrInvalidMessage
	}
	correlationID := message.CorrelationID
	if strings.TrimSpace(correlationID) == "" {
		correlationID = message.MessageID
	}
	input = application.ProcessWagerInput{
		ProviderID: message.Data.ProviderID, ExternalTransactionID: message.Data.ExternalTransactionID,
		IdempotencyKey: message.Data.IdempotencyKey, PlayerID: message.Data.PlayerID, WalletID: message.Data.WalletID,
		RoundID: message.Data.RoundID, GameID: message.Data.GameID, Kind: message.Data.Kind, Money: money,
		ReferenceExternalTransactionID: message.Data.ReferenceExternalTransactionID,
		CorrelationID:                  correlationID, CausationID: message.MessageID,
	}
	return message, input, nil
}
