package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
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

func (w *Wallet) Credit(money Money) error {
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
