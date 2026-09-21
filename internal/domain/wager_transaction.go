package domain

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

type WagerKind string

const (
	WagerKindOpening  WagerKind = "OPENING"
	WagerKindBet      WagerKind = "BET"
	WagerKindWin      WagerKind = "WIN"
	WagerKindLoss     WagerKind = "LOSS"
	WagerKindRefund   WagerKind = "REFUND"
	WagerKindRollback WagerKind = "ROLLBACK"
)

type WagerStatus string

const (
	WagerStatusPending          WagerStatus = "PENDING"
	WagerStatusPendingReference WagerStatus = "PENDING_REFERENCE"
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
	ErrInvalidWagerReference   = errors.New("invalid wager reference")
	ErrReferenceNotProcessed   = errors.New("reference is not processed")
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

	transaction := &WagerTransaction{
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
		referenceExternalTransactionID: copyString(referenceExternalTransactionID),
		status:                         WagerStatusPending,
		createdAt:                      now,
		updatedAt:                      now,
	}
	if err := transaction.validate(); err != nil {
		return nil, err
	}
	return transaction, nil
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
	if money.Validate() != nil {
		return ErrInvalidWagerMoney
	}
	if referenceExternalTransactionID != nil && strings.TrimSpace(*referenceExternalTransactionID) == "" {
		return ErrReferenceRequired
	}
	switch kind {
	case WagerKindOpening, WagerKindBet, WagerKindWin:
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
	return copyString(w.failureCode)
}

func (w *WagerTransaction) ResultBalance() *Money {
	if w.resultBalance == nil {
		return nil
	}
	balance := *w.resultBalance
	return &balance
}

func (w *WagerTransaction) ReferenceExternalTransactionID() *string {
	return copyString(w.referenceExternalTransactionID)
}

func (w *WagerTransaction) ReferenceTransactionID() *uuid.UUID {
	if w.referenceTransactionID == nil {
		return nil
	}
	id := *w.referenceTransactionID
	return &id
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
	if err := w.canChange(); err != nil {
		return err
	}

	if w.status != WagerStatusPending || !w.needsReference() || w.referenceTransactionID != nil {
		return ErrInvalidWagerStatus
	}

	w.status = WagerStatusPendingReference
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *WagerTransaction) ResolveReference(reference *WagerTransaction) error {
	if err := w.canChange(); err != nil {
		return err
	}
	if err := w.validateReference(reference); err != nil {
		return err
	}
	transactionID := reference.ID()
	w.referenceTransactionID = &transactionID
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *WagerTransaction) MarkProcessed(balance Money) error {
	if err := w.canChange(); err != nil {
		return err
	}
	if balance.Validate() != nil || balance.Amount() < 0 {
		return ErrInvalidWagerMoney
	}
	if w.needsReference() && w.referenceTransactionID == nil {
		return ErrReferenceRequired
	}

	if balance.Currency() != w.money.Currency() {
		return ErrCurrencyMismatch
	}
	if w.kind == WagerKindOpening && balance != w.money {
		return ErrInvalidWagerMoney
	}

	w.status = WagerStatusProcessed
	w.resultBalance = &balance
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *WagerTransaction) Reject(failureCode string) error {
	if err := w.canChange(); err != nil {
		return err
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
	if err := w.canChange(); err != nil {
		return err
	}

	if strings.TrimSpace(failureCode) == "" {
		return ErrInvalidWagerTransaction
	}

	w.status = WagerStatusFailed
	w.failureCode = &failureCode
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *WagerTransaction) CreatedAt() time.Time { return w.createdAt }

func (w *WagerTransaction) UpdatedAt() time.Time { return w.updatedAt }

func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (w *WagerTransaction) needsReference() bool {
	return w.kind == WagerKindRefund || w.kind == WagerKindRollback ||
		(w.kind == WagerKindWin && w.referenceExternalTransactionID != nil)
}

func (w *WagerTransaction) canChange() error {
	if err := w.validate(); err != nil {
		return err
	}
	if w.IsTerminal() {
		return ErrTerminalTransaction
	}
	return nil
}

func (w *WagerTransaction) validateReference(reference *WagerTransaction) error {
	if !w.needsReference() || reference == nil {
		return ErrInvalidWagerReference
	}
	if reference.validate() != nil || reference.id == w.id {
		return ErrInvalidWagerReference
	}
	if reference.providerID != w.providerID || reference.externalTransactionID != *w.referenceExternalTransactionID {
		return ErrInvalidWagerReference
	}
	if reference.walletID != w.walletID || reference.playerID != w.playerID {
		return ErrInvalidWagerReference
	}
	if reference.money.Currency() != w.money.Currency() || reference.roundID != w.roundID {
		return ErrInvalidWagerReference
	}
	if w.referenceTransactionID != nil && *w.referenceTransactionID != reference.id {
		return ErrInvalidWagerReference
	}
	if reference.status != WagerStatusProcessed {
		return ErrReferenceNotProcessed
	}
	switch w.kind {
	case WagerKindWin, WagerKindRefund:
		if reference.kind != WagerKindBet {
			return ErrInvalidWagerReference
		}
	case WagerKindRollback:
		if reference.kind != WagerKindBet && reference.kind != WagerKindWin && reference.kind != WagerKindRefund {
			return ErrInvalidWagerReference
		}
	}
	if w.kind != WagerKindWin && reference.money != w.money {
		return ErrInvalidWagerReference
	}
	return nil
}
