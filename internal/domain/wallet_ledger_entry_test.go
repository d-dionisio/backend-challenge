package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestCreateDebitLedgerEntry(t *testing.T) {
	before := newTestMoney(t, "100.00")
	money := newTestMoney(t, "25.00")
	after := newTestMoney(t, "75.00")

	entry, err := NewWalletLedgerEntry(
		uuid.New(),
		uuid.New(),
		LedgerDirectionDebit,
		money,
		before,
		after,
	)

	if err != nil {
		t.Fatal(err)
	}

	if entry.Direction() != LedgerDirectionDebit {
		t.Fatalf("expected DEBIT, got %s", entry.Direction())
	}

	if entry.BalanceAfter().String() != "75.00" {
		t.Fatalf("expected 75.00, got %s", entry.BalanceAfter().String())
	}
}

func TestCreateCreditLedgerEntry(t *testing.T) {
	before := newTestMoney(t, "100.00")
	money := newTestMoney(t, "50.00")
	after := newTestMoney(t, "150.00")

	entry, err := NewWalletLedgerEntry(
		uuid.New(),
		uuid.New(),
		LedgerDirectionCredit,
		money,
		before,
		after,
	)

	if err != nil {
		t.Fatal(err)
	}

	if entry.BalanceAfter().String() != "150.00" {
		t.Fatalf("expected 150.00, got %s", entry.BalanceAfter().String())
	}
}

func TestDebitLedgerRejectsInvalidBalance(t *testing.T) {
	before := newTestMoney(t, "100.00")
	money := newTestMoney(t, "25.00")

	// Errado propositalmente.
	after := newTestMoney(t, "90.00")

	_, err := NewWalletLedgerEntry(
		uuid.New(),
		uuid.New(),
		LedgerDirectionDebit,
		money,
		before,
		after,
	)

	if !errors.Is(err, ErrInvalidLedgerBalance) {
		t.Fatalf("expected invalid ledger balance, got %v", err)
	}
}

func TestCreditLedgerRejectsInvalidBalance(t *testing.T) {
	before := newTestMoney(t, "100.00")
	money := newTestMoney(t, "50.00")

	// Errado propositalmente.
	after := newTestMoney(t, "120.00")

	_, err := NewWalletLedgerEntry(
		uuid.New(),
		uuid.New(),
		LedgerDirectionCredit,
		money,
		before,
		after,
	)

	if !errors.Is(err, ErrInvalidLedgerBalance) {
		t.Fatalf("expected invalid ledger balance, got %v", err)
	}
}

func TestLedgerRejectsCurrencyMismatch(t *testing.T) {
	money, err := NewMoney("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	before, err := NewMoney("100.00", "USD")
	if err != nil {
		t.Fatal(err)
	}

	after, err := NewMoney("75.00", "USD")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewWalletLedgerEntry(
		uuid.New(),
		uuid.New(),
		LedgerDirectionDebit,
		money,
		before,
		after,
	)

	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("expected currency mismatch, got %v", err)
	}
}

func TestLedgerRejectsInvalidDirection(t *testing.T) {
	before := newTestMoney(t, "100.00")
	money := newTestMoney(t, "25.00")
	after := newTestMoney(t, "75.00")

	_, err := NewWalletLedgerEntry(
		uuid.New(),
		uuid.New(),
		LedgerDirection("INVALID"),
		money,
		before,
		after,
	)

	if !errors.Is(err, ErrInvalidLedgerDirection) {
		t.Fatalf("expected invalid ledger direction, got %v", err)
	}
}
