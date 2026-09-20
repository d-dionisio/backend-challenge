package domain

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

type WagerKind string

var (
	WagerKindOpening  WagerKind = "OPENING"
	WagerKindBet      WagerKind = "BET"
	WagerKindWin      WagerKind = "WIN"
	WagerKindLoss     WagerKind = "LOSS"
	WagerKindRefund   WagerKind = "REFUND"
	WagerKindRollback WagerKind = "ROLLBACK"
)

type WagerStatus string

var (
	WagerStatusPending          WagerStatus = "PENDING"
	WagerStatusPendingReference WagerStatus = "PEDING_REFERENCE"
	WagerStatusProcessed        WagerStatus = "PROCESSED"
	WagerStatusRejected         WagerStatus = "REJECTED"
	WagerStatusFailed           WagerStatus = "FAILED"
)

var (
	ErrInvalidWagerKind        = errors.New("invalid wager kind")
	ErrInvalidWagerStatus      = errors.New("invalid wager status")
	ErrInvalidWagerTransaction = errors.New("invalid wager transaction")
	ErrInvalidWagerMoney       = errors.New("invalid wager money")
	ErrReferenceRequired       = errors.New("reference transaction required")
	ErrTerminalTransaction     = errors.New("transaction is already terminal")
	ErrExternalOpening         = errors.New("opening cannot be created externally")
)

type WagerTransaction struct {
	id                             uuid.UUID
	externalTransactionID          string
	providerID                     string
	idempotencyKey                 string
	payloadHash                    string
	walletID                       uuid.UUID
	playerID                       uuid.UUID
	roundID                        string
	gameID                         string
	kind                           WagerKind
	money                          Money
	referenceExternalTransactionID *string
	referenceTransactionID         *uuid.UUID
	status                         WagerStatus
	failureCode                    *string
	resultBalance                  *Money
	createdAt                      time.Time
	updatedAt                      time.Time
}

func NewWagerTransaction(
	providerID string,
	externalTransactionID string,
	idempotencyKey string,
	payloadHash string,
	walletID uuid.UUID,
	playerID uuid.UUID,
	roundID string,
	gameID string,
	kind WagerKind,
	money Money,
	referenceExternalTransactionID *string,
) (*WagerTransaction, error) {

	if strings.TrimSpace(providerID) == "" || strings.TrimSpace(externalTransactionID) == "" || strings.TrimSpace(idempotencyKey) == "" || strings.TrimSpace(payloadHash) == "" || walletID == uuid.Nil || playerID == uuid.Nil || strings.TrimSpace(roundID) == "" || strings.TrimSpace(gameID) == "" {
		return nil, ErrInvalidWagerTransaction
	}

	if !kind.IsValid() {
		return nil, ErrInvalidWagerKind
	}

	if kind == WagerKindOpening {
		return nil, ErrExternalOpening
	}

	if err := validateWagerMoney(
		kind,
		money,
		referenceExternalTransactionID,
	); err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	return &WagerTransaction{
		id:                             uuid.New(),
		externalTransactionID:          externalTransactionID,
		providerID:                     providerID,
		idempotencyKey:                 idempotencyKey,
		payloadHash:                    payloadHash,
		walletID:                       walletID,
		playerID:                       playerID,
		roundID:                        roundID,
		gameID:                         gameID,
		kind:                           kind,
		money:                          money,
		referenceExternalTransactionID: referenceExternalTransactionID,
		status:                         WagerStatusPending,
		createdAt:                      now,
		updatedAt:                      now,
	}, nil
}

func (k WagerKind) IsValid() bool {
	switch k {
	case WagerKindOpening,
		WagerKindBet,
		WagerKindWin,
		WagerKindLoss,
		WagerKindRefund,
		WagerKindRollback:
		return true

	default:
		return false
	}
}

func validateWagerMoney(kind WagerKind, money Money, referenceExternalTransactionID *string) error {
	switch kind {
	case WagerKindBet:
		if !money.IsPositive() {
			return ErrInvalidWagerMoney
		}

	case WagerKindWin:
		if !money.IsPositive() {
			return ErrInvalidWagerMoney
		}

	case WagerKindLoss:
		if !money.IsZero() {
			return ErrInvalidWagerMoney
		}

	case WagerKindRefund, WagerKindRollback:
		if !money.IsPositive() {
			return ErrInvalidWagerMoney
		}

		if referenceExternalTransactionID == nil || strings.TrimSpace(*referenceExternalTransactionID) == "" {
			return ErrReferenceRequired
		}

	default:
		return ErrInvalidWagerKind
	}

	return nil
}

func (w *WagerTransaction) ID() uuid.UUID {
	return w.id
}

func (w *WagerTransaction) ExternalTransactionID() string {
	return w.externalTransactionID
}

func (w *WagerTransaction) ProviderID() string {
	return w.providerID
}

func (w *WagerTransaction) IdempotencyKey() string {
	return w.idempotencyKey
}

func (w *WagerTransaction) PayloadHash() string {
	return w.payloadHash
}

func (w *WagerTransaction) WalletID() uuid.UUID {
	return w.walletID
}

func (w *WagerTransaction) PlayerID() uuid.UUID {
	return w.playerID
}

func (w *WagerTransaction) RoundID() string {
	return w.roundID
}

func (w *WagerTransaction) GameID() string {
	return w.gameID
}

func (w *WagerTransaction) Kind() WagerKind {
	return w.kind
}

func (w *WagerTransaction) Money() Money {
	return w.money
}

func (w *WagerTransaction) Status() WagerStatus {
	return w.status
}

func (w *WagerTransaction) FailureCode() *string {
	return w.failureCode
}

func (w *WagerTransaction) ResultBalance() *Money {
	return w.resultBalance
}

func (w *WagerTransaction) ReferenceExternalTransactionID() *string {
	return w.referenceExternalTransactionID
}

func (w *WagerTransaction) ReferenceTransactionID() *uuid.UUID {
	return w.referenceTransactionID
}

func (w *WagerTransaction) IsTerminal() bool {
	switch w.status {
	case WagerStatusProcessed,
		WagerStatusRejected,
		WagerStatusFailed:
		return true

	default:
		return false

	}
}

func (w *WagerTransaction) MarkPendingReference() error {
	if w.IsTerminal() {
		return ErrTerminalTransaction
	}

	if w.kind != WagerKindRefund && w.kind != WagerKindRollback {
		return ErrInvalidWagerStatus
	}

	w.status = WagerStatusPendingReference
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *WagerTransaction) ResolveReference(transactionID uuid.UUID) error {
	if w.IsTerminal() {
		return ErrTerminalTransaction
	}

	if transactionID == uuid.Nil {
		return ErrInvalidWagerTransaction
	}

	w.referenceTransactionID = &transactionID
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *WagerTransaction) MarkProcessed(balance Money) error {
	if w.IsTerminal() {
		return ErrTerminalTransaction
	}

	if balance.Currency() != w.money.Currency() {
		return ErrCurrencyMismatch
	}

	w.status = WagerStatusProcessed
	w.resultBalance = &balance
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *WagerTransaction) Reject(failureCode string) error {
	if w.IsTerminal() {
		return ErrTerminalTransaction
	}

	if strings.TrimSpace(failureCode) == "" {
		return ErrInvalidWagerTransaction
	}

	w.status = WagerStatusRejected
	w.failureCode = &failureCode
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *WagerTransaction) Fail(failureCode string) error {
	if w.IsTerminal() {
		return ErrTerminalTransaction
	}

	if strings.TrimSpace(failureCode) == "" {
		return ErrInvalidWagerTransaction
	}

	w.status = WagerStatusFailed
	w.failureCode = &failureCode
	w.updatedAt = time.Now().UTC()

	return nil
}
