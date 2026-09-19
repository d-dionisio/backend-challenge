package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestWalletDebit(t *testing.T) {
	initialBalance, _ := NewMoney("100.00", "BRL")

	wallet, err := NewWallet(uuid.New(), initialBalance)
	if err != nil {
		t.Fatal(err)
	}

	bet, _ := NewMoney("80.00", "BRL")

	err = wallet.Debit(bet)
	if err != nil {
		t.Fatal(err)
	}

	if wallet.Balance().String() != "20.00" {
		t.Fatalf("expected balance 20.00, got %s", wallet.Balance().String())
	}
}

func TestWalletDoesNotAllowNegativeBalance(t *testing.T) {
	initialBalance, _ := NewMoney("100.00", "BRL")

	wallet, err := NewWallet(uuid.New(), initialBalance)
	if err != nil {
		t.Fatal(err)
	}

	bet, _ := NewMoney("120.00", "BRL")

	err = wallet.Debit(bet)
	if !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("expected insufficient balance")
	}

	if wallet.Balance().String() != "100.00" {
		t.Fatalf("balance should not change")
	}
}
