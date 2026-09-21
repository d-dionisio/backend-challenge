package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidEvent = errors.New("invalid event")

// Estes dados fazem parte do contrato de todos os eventos.
type eventMetadata struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   uuid.UUID `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
}

func newEventMetadata(eventType string, aggregateID uuid.UUID, occurredAt time.Time, correlationID, causationID string) (eventMetadata, error) {
	if aggregateID == uuid.Nil || occurredAt.IsZero() || strings.TrimSpace(correlationID) == "" {
		return eventMetadata{}, ErrInvalidEvent
	}
	return eventMetadata{
		EventID:       uuid.New(),
		EventType:     eventType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    occurredAt.UTC(),
		Version:       1,
	}, nil
}

// Campos externos vazios são omitidos nos eventos de OPENING.
type wagerEventData struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	WalletID                       uuid.UUID `json:"walletId"`
	PlayerID                       uuid.UUID `json:"playerId"`
	ProviderID                     string    `json:"providerId,omitempty"`
	ExternalTransactionID          string    `json:"externalTransactionId,omitempty"`
	RoundID                        string    `json:"roundId,omitempty"`
	GameID                         string    `json:"gameId,omitempty"`
	Kind                           WagerKind `json:"kind"`
	Money                          Money     `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

func newWagerEventData(transaction *WagerTransaction) wagerEventData {
	data := wagerEventData{
		TransactionID:         transaction.ID(),
		WalletID:              transaction.WalletID(),
		PlayerID:              transaction.PlayerID(),
		ProviderID:            transaction.ProviderID(),
		ExternalTransactionID: transaction.ExternalTransactionID(),
		RoundID:               transaction.RoundID(),
		GameID:                transaction.GameID(),
		Kind:                  transaction.Kind(),
		Money:                 transaction.Money(),
	}
	if transaction.referenceExternalTransactionID != nil {
		data.ReferenceExternalTransactionID = *transaction.referenceExternalTransactionID
	}
	return data
}

type wagerTransactionProcessedPayload struct {
	eventMetadata
	Data struct {
		wagerEventData
		Balance Money `json:"balance"`
	} `json:"data"`
}

// O conteúdo privado é uma cópia dos valores no instante da construção.
type WagerTransactionProcessed struct {
	payload wagerTransactionProcessedPayload
}

func NewWagerTransactionProcessed(transaction *WagerTransaction, correlationID, causationID string) (WagerTransactionProcessed, error) {
	if transaction.validate() != nil || transaction.Status() != WagerStatusProcessed {
		return WagerTransactionProcessed{}, ErrInvalidEvent
	}
	metadata, err := newEventMetadata("WagerTransactionProcessed", transaction.WalletID(), transaction.UpdatedAt(), correlationID, causationID)
	if err != nil {
		return WagerTransactionProcessed{}, err
	}
	payload := wagerTransactionProcessedPayload{eventMetadata: metadata}
	payload.Data.wagerEventData = newWagerEventData(transaction)
	payload.Data.Balance = *transaction.resultBalance
	return WagerTransactionProcessed{payload: payload}, nil
}

func (event WagerTransactionProcessed) EventID() uuid.UUID { return event.payload.EventID }

func (event WagerTransactionProcessed) MarshalJSON() ([]byte, error) {
	if event.payload.EventID == uuid.Nil {
		return nil, ErrInvalidEvent
	}
	return json.Marshal(event.payload)
}

type wagerTransactionRejectedPayload struct {
	eventMetadata
	Data struct {
		wagerEventData
		FailureCode string `json:"failureCode"`
	} `json:"data"`
}

// O conteúdo privado é uma cópia dos valores no instante da construção.
type WagerTransactionRejected struct {
	payload wagerTransactionRejectedPayload
}

func NewWagerTransactionRejected(transaction *WagerTransaction, correlationID, causationID string) (WagerTransactionRejected, error) {
	if transaction.validate() != nil || transaction.Status() != WagerStatusRejected {
		return WagerTransactionRejected{}, ErrInvalidEvent
	}
	metadata, err := newEventMetadata("WagerTransactionRejected", transaction.WalletID(), transaction.UpdatedAt(), correlationID, causationID)
	if err != nil {
		return WagerTransactionRejected{}, err
	}
	payload := wagerTransactionRejectedPayload{eventMetadata: metadata}
	payload.Data.wagerEventData = newWagerEventData(transaction)
	payload.Data.FailureCode = *transaction.failureCode
	return WagerTransactionRejected{payload: payload}, nil
}

func (event WagerTransactionRejected) EventID() uuid.UUID { return event.payload.EventID }

func (event WagerTransactionRejected) MarshalJSON() ([]byte, error) {
	if event.payload.EventID == uuid.Nil {
		return nil, ErrInvalidEvent
	}
	return json.Marshal(event.payload)
}

type wagerTransactionPendingReferencePayload struct {
	eventMetadata
	Data struct {
		wagerEventData
	} `json:"data"`
}

// O conteúdo privado é uma cópia dos valores no instante da construção.
type WagerTransactionPendingReference struct {
	payload wagerTransactionPendingReferencePayload
}

func NewWagerTransactionPendingReference(transaction *WagerTransaction, correlationID, causationID string) (WagerTransactionPendingReference, error) {
	if transaction.validate() != nil || transaction.Status() != WagerStatusPendingReference {
		return WagerTransactionPendingReference{}, ErrInvalidEvent
	}
	metadata, err := newEventMetadata("WagerTransactionPendingReference", transaction.WalletID(), transaction.UpdatedAt(), correlationID, causationID)
	if err != nil {
		return WagerTransactionPendingReference{}, err
	}
	payload := wagerTransactionPendingReferencePayload{eventMetadata: metadata}
	payload.Data.wagerEventData = newWagerEventData(transaction)

	return WagerTransactionPendingReference{payload: payload}, nil
}

func (event WagerTransactionPendingReference) EventID() uuid.UUID { return event.payload.EventID }

func (event WagerTransactionPendingReference) MarshalJSON() ([]byte, error) {
	if event.payload.EventID == uuid.Nil {
		return nil, ErrInvalidEvent
	}
	return json.Marshal(event.payload)
}

type walletBalanceChangedPayload struct {
	eventMetadata
	Data struct {
		WalletID      uuid.UUID       `json:"walletId"`
		TransactionID uuid.UUID       `json:"transactionId"`
		Direction     LedgerDirection `json:"direction"`
		Money         Money           `json:"money"`
		BalanceBefore Money           `json:"balanceBefore"`
		BalanceAfter  Money           `json:"balanceAfter"`
		WalletVersion int64           `json:"walletVersion"`
	} `json:"data"`
}

type WalletBalanceChanged struct {
	payload walletBalanceChangedPayload
}

func NewWalletBalanceChanged(transaction *WagerTransaction, entry *WalletLedgerEntry, walletVersion int64, correlationID, causationID string) (WalletBalanceChanged, error) {
	if transaction.validate() != nil || transaction.Status() != WagerStatusProcessed {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if entry == nil || entry.id == uuid.Nil || entry.createdAt.IsZero() || !entry.money.IsPositive() {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if entry.walletID != transaction.walletID || entry.transactionID != transaction.id || entry.money != transaction.money {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if entry.balanceAfter != *transaction.resultBalance {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if entry.balanceBefore.Validate() != nil || entry.balanceAfter.Validate() != nil {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if entry.balanceBefore.Amount() < 0 || entry.balanceAfter.Amount() < 0 {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if err := validateLedgerBalance(entry.direction, entry.money, entry.balanceBefore, entry.balanceAfter); err != nil {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	switch transaction.kind {
	case WagerKindOpening:
		if walletVersion != 1 || !entry.balanceBefore.IsZero() || entry.direction != LedgerDirectionCredit {
			return WalletBalanceChanged{}, ErrInvalidEvent
		}
	case WagerKindBet:
		if entry.direction != LedgerDirectionDebit {
			return WalletBalanceChanged{}, ErrInvalidEvent
		}
	case WagerKindWin, WagerKindRefund:
		if entry.direction != LedgerDirectionCredit {
			return WalletBalanceChanged{}, ErrInvalidEvent
		}
	case WagerKindLoss:
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if walletVersion < 1 || (transaction.kind != WagerKindOpening && walletVersion < 2) {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	metadata, err := newEventMetadata("WalletBalanceChanged", transaction.WalletID(), transaction.UpdatedAt(), correlationID, causationID)
	if err != nil {
		return WalletBalanceChanged{}, err
	}
	payload := walletBalanceChangedPayload{eventMetadata: metadata}
	payload.Data.WalletID = entry.walletID
	payload.Data.TransactionID = entry.transactionID
	payload.Data.Direction = entry.direction
	payload.Data.Money = entry.money
	payload.Data.BalanceBefore = entry.balanceBefore
	payload.Data.BalanceAfter = entry.balanceAfter
	payload.Data.WalletVersion = walletVersion
	return WalletBalanceChanged{payload: payload}, nil
}

func (event WalletBalanceChanged) EventID() uuid.UUID { return event.payload.EventID }

func (event WalletBalanceChanged) MarshalJSON() ([]byte, error) {
	if event.payload.EventID == uuid.Nil {
		return nil, ErrInvalidEvent
	}
	return json.Marshal(event.payload)
}
