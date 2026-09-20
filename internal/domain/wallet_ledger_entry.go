package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type LedgerDirection string

const (
	LedgerDirectionDebit  LedgerDirection = "DEBIT"
	LedgerDirectionCredit LedgerDirection = "CREDIT"
)

var (
	ErrInvalidLedgerDirection = errors.New("invalid ledger direction")
	ErrInvalidLedgerEntry     = errors.New("invalid ledger entry")
	ErrInvalidLedgerBalance   = errors.New("invalid ledger balance")
)

type WalletLedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     LedgerDirection
	money         Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

func (d LedgerDirection) IsValid() bool {
	switch d {
	case LedgerDirectionDebit, LedgerDirectionCredit:
		return true
	default:
		return false
	}
}

func NewWalletLedgerEntry(walletID uuid.UUID, transactionID uuid.UUID, direction LedgerDirection, money Money, balanceBefore Money, balanceAfter Money) (*WalletLedgerEntry, error) {
	if walletID == uuid.Nil || transactionID == uuid.Nil {
		return nil, ErrInvalidLedgerEntry
	}

	if !direction.IsValid() {
		return nil, ErrInvalidLedgerDirection
	}

	if !money.IsPositive() {
		return nil, ErrInvalidLedgerEntry
	}

	if money.Currency() != balanceBefore.Currency() || money.Currency() != balanceAfter.Currency() {
		return nil, ErrCurrencyMismatch
	}

	if err := validateLedgerBalance(
		direction,
		money,
		balanceBefore,
		balanceAfter,
	); err != nil {
		return nil, err
	}

	return &WalletLedgerEntry{
		id:            uuid.New(),
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		money:         money,
		balanceBefore: balanceBefore,
		balanceAfter:  balanceAfter,
		createdAt:     time.Now().UTC(),
	}, nil
}

func validateLedgerBalance(direction LedgerDirection, money Money, balanceBefore Money, balanceAfter Money) error {
	switch direction {

	case LedgerDirectionDebit:
		expected, err := balanceBefore.Sub(money)
		if err != nil {
			return err
		}

		if expected.Amount() != balanceAfter.Amount() {
			return ErrInvalidLedgerBalance
		}

	case LedgerDirectionCredit:
		expected, err := balanceBefore.Add(money)
		if err != nil {
			return err
		}

		if expected.Amount() != balanceAfter.Amount() {
			return ErrInvalidLedgerBalance
		}

	default:
		return ErrInvalidLedgerDirection
	}

	return nil
}

func (e *WalletLedgerEntry) ID() uuid.UUID {
	return e.id
}

func (e *WalletLedgerEntry) WalletID() uuid.UUID {
	return e.walletID
}

func (e *WalletLedgerEntry) TransactionID() uuid.UUID {
	return e.transactionID
}

func (e *WalletLedgerEntry) Direction() LedgerDirection {
	return e.direction
}

func (e *WalletLedgerEntry) Money() Money {
	return e.money
}

func (e *WalletLedgerEntry) BalanceBefore() Money {
	return e.balanceBefore
}

func (e *WalletLedgerEntry) BalanceAfter() Money {
	return e.balanceAfter
}

func (e *WalletLedgerEntry) CreatedAt() time.Time {
	return e.createdAt
}
