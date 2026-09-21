package domain

import (
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidWallet       = errors.New("invalid wallet")
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrWalletCurrency      = errors.New("wallet currency mismatch")
)

type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func NewWallet(playerID uuid.UUID, initialBalance Money) (*Wallet, error) {
	if playerID == uuid.Nil {
		return nil, ErrInvalidWallet
	}
	if initialBalance.Validate() != nil {
		return nil, ErrInvalidWallet
	}
	if initialBalance.Amount() < 0 {
		return nil, ErrInvalidWallet
	}
	now := time.Now().UTC()

	return &Wallet{
		id:        uuid.New(),
		playerID:  playerID,
		balance:   initialBalance,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}, nil
}

func (w *Wallet) ID() uuid.UUID {
	return w.id
}

func (w *Wallet) PlayerID() uuid.UUID {
	return w.playerID
}

func (w *Wallet) Balance() Money {
	return w.balance
}

func (w *Wallet) Version() int64 {
	return w.version
}

func (w *Wallet) CreatedAt() time.Time {
	return w.createdAt
}

func (w *Wallet) UpdatedAt() time.Time {
	return w.updatedAt
}

func (w *Wallet) Credit(money Money) error {
	if err := w.validateMovement(money); err != nil {
		return err
	}
	if money.Currency() != w.balance.Currency() {
		return ErrWalletCurrency
	}

	newBalance, err := w.balance.Add(money)
	if err != nil {
		return err
	}

	w.balance = newBalance
	w.version++
	w.updatedAt = time.Now().UTC()

	return nil
}

func (w *Wallet) Debit(money Money) error {
	if err := w.validateMovement(money); err != nil {
		return err
	}
	if money.Currency() != w.balance.Currency() {
		return ErrWalletCurrency
	}

	insufficient, err := w.balance.LessThan(money)
	if err != nil {
		return err
	}

	if insufficient {
		return ErrInsufficientBalance
	}

	newBalance, err := w.balance.Sub(money)
	if err != nil {
		return err
	}

	w.balance = newBalance
	w.version++
	w.updatedAt = time.Now().UTC()

	return nil
}

func RehydrateWallet(id uuid.UUID, playerID uuid.UUID, balance Money, version int64, createdAt time.Time, updatedAt time.Time) (*Wallet, error) {

	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, ErrInvalidWallet
	}

	if version < 1 {
		return nil, ErrInvalidWallet
	}

	if balance.Validate() != nil {
		return nil, ErrInvalidWallet
	}
	if balance.Amount() < 0 {
		return nil, ErrInvalidWallet
	}
	if createdAt.IsZero() || updatedAt.Before(createdAt) {
		return nil, ErrInvalidWallet
	}

	return &Wallet{
		id:        id,
		playerID:  playerID,
		balance:   balance,
		version:   version,
		createdAt: createdAt,
		updatedAt: updatedAt,
	}, nil
}

func (w *Wallet) validateMovement(money Money) error {
	if w == nil {
		return ErrInvalidWallet
	}
	if w.id == uuid.Nil || w.playerID == uuid.Nil {
		return ErrInvalidWallet
	}
	if w.balance.Validate() != nil || w.balance.Amount() < 0 {
		return ErrInvalidWallet
	}
	if w.version < 1 || w.version == math.MaxInt64 {
		return ErrInvalidWallet
	}
	if !money.IsPositive() {
		return ErrInvalidMoney
	}
	return nil
}
