package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func newTestMoney(t *testing.T, value string) Money {
	t.Helper()

	money, err := NewMoney(value, "BRL")
	if err != nil {
		t.Fatal(err)
	}

	return money
}

func TestNewBetTransaction(t *testing.T) {
	money := newTestMoney(t, "25.00")

	transaction, err := NewWagerTransaction(
		"provider-a",
		"transaction-123",
		"provider-a:transaction-123",
		"hash-123",
		uuid.New(),
		uuid.New(),
		"round-123",
		"game-123",
		WagerKindBet,
		money,
		nil,
	)

	if err != nil {
		t.Fatal(err)
	}

	if transaction.Status() != WagerStatusPending {
		t.Fatalf("expected PENDING, got %s", transaction.Status())
	}

	if transaction.Kind() != WagerKindBet {
		t.Fatalf("expected BET, got %s", transaction.Kind())
	}
}

func TestLossRequiresZeroMoney(t *testing.T) {
	money := newTestMoney(t, "10.00")

	_, err := NewWagerTransaction(
		"provider-a",
		"transaction-123",
		"key",
		"hash",
		uuid.New(),
		uuid.New(),
		"round",
		"game",
		WagerKindLoss,
		money,
		nil,
	)

	if !errors.Is(err, ErrInvalidWagerMoney) {
		t.Fatalf("expected invalid wager money, got %v", err)
	}
}

func TestLossAcceptsZeroMoney(t *testing.T) {
	money := newTestMoney(t, "0.00")

	transaction, err := NewWagerTransaction(
		"provider-a",
		"transaction-123",
		"key",
		"hash",
		uuid.New(),
		uuid.New(),
		"round",
		"game",
		WagerKindLoss,
		money,
		nil,
	)

	if err != nil {
		t.Fatal(err)
	}

	if transaction.Status() != WagerStatusPending {
		t.Fatalf("expected PENDING, got %s", transaction.Status())
	}
}

func TestRefundRequiresReference(t *testing.T) {
	money := newTestMoney(t, "25.00")

	_, err := NewWagerTransaction(
		"provider-a",
		"refund-123",
		"key",
		"hash",
		uuid.New(),
		uuid.New(),
		"round",
		"game",
		WagerKindRefund,
		money,
		nil,
	)

	if !errors.Is(err, ErrReferenceRequired) {
		t.Fatalf(
			"expected reference required, got %v",
			err,
		)
	}
}

func TestRefundWithReference(t *testing.T) {
	money := newTestMoney(t, "25.00")

	reference := "bet-123"

	transaction, err := NewWagerTransaction(
		"provider-a",
		"refund-123",
		"key",
		"hash",
		uuid.New(),
		uuid.New(),
		"round",
		"game",
		WagerKindRefund,
		money,
		&reference,
	)

	if err != nil {
		t.Fatal(err)
	}

	if transaction.ReferenceExternalTransactionID() == nil {
		t.Fatal("expected reference")
	}
}

func TestOpeningCannotBeExternal(t *testing.T) {
	money := newTestMoney(t, "100.00")

	_, err := NewWagerTransaction(
		"provider-a",
		"opening-123",
		"key",
		"hash",
		uuid.New(),
		uuid.New(),
		"round",
		"game",
		WagerKindOpening,
		money,
		nil,
	)

	if !errors.Is(err, ErrExternalOpening) {
		t.Fatalf(
			"expected external opening error, got %v",
			err,
		)
	}
}

func TestTransactionCanBeProcessed(t *testing.T) {
	money := newTestMoney(t, "25.00")

	transaction, err := NewWagerTransaction(
		"provider-a",
		"transaction-123",
		"key",
		"hash",
		uuid.New(),
		uuid.New(),
		"round",
		"game",
		WagerKindBet,
		money,
		nil,
	)

	if err != nil {
		t.Fatal(err)
	}

	balance := newTestMoney(t, "75.00")

	err = transaction.MarkProcessed(balance)
	if err != nil {
		t.Fatal(err)
	}

	if transaction.Status() != WagerStatusProcessed {
		t.Fatalf(
			"expected PROCESSED, got %s",
			transaction.Status(),
		)
	}
}

func TestTerminalTransactionCannotChangeState(t *testing.T) {
	money := newTestMoney(t, "25.00")

	transaction, err := NewWagerTransaction(
		"provider-a",
		"transaction-123",
		"key",
		"hash",
		uuid.New(),
		uuid.New(),
		"round",
		"game",
		WagerKindBet,
		money,
		nil,
	)

	if err != nil {
		t.Fatal(err)
	}

	balance := newTestMoney(t, "75.00")

	if err := transaction.MarkProcessed(balance); err != nil {
		t.Fatal(err)
	}

	err = transaction.Reject("INSUFFICIENT_BALANCE")

	if !errors.Is(err, ErrTerminalTransaction) {
		t.Fatalf(
			"expected terminal transaction error, got %v",
			err,
		)
	}
}
