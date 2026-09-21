package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestOpeningEvents(t *testing.T) {
	wallet, _ := NewWallet(uuid.New(), newTestMoney(t, "100"))
	opening, err := NewOpeningTransaction(wallet)
	if err != nil {
		t.Fatal(err)
	}
	processed, err := NewWagerTransactionProcessed(opening, "request-1", "")
	if err != nil {
		t.Fatal(err)
	}
	zero, _ := ZeroMoney("BRL")
	entry, err := NewWalletLedgerEntry(wallet.ID(), opening.ID(), LedgerDirectionCredit, wallet.Balance(), zero, wallet.Balance())
	if err != nil {
		t.Fatal(err)
	}
	changed, err := NewWalletBalanceChanged(opening, entry, wallet.Version(), "request-1", "")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(processed)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		EventID       uuid.UUID `json:"eventId"`
		EventType     string    `json:"eventType"`
		AggregateID   uuid.UUID `json:"aggregateId"`
		CorrelationID string    `json:"correlationId"`
		OccurredAt    string    `json:"occurredAt"`
		Version       int       `json:"version"`
		Data          struct {
			Kind    string `json:"kind"`
			Balance struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			} `json:"balance"`
		} `json:"data"`
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.EventID != processed.EventID() || envelope.EventID == uuid.Nil || envelope.AggregateID != wallet.ID() || envelope.CorrelationID != "request-1" {
		t.Fatal("incorrect event metadata")
	}
	if envelope.EventType != "WagerTransactionProcessed" || envelope.Version != 1 || envelope.Data.Kind != "OPENING" {
		t.Fatal("incorrect event type/version")
	}
	if envelope.Data.Balance.Amount != "100.00" || envelope.Data.Balance.Currency != "BRL" {
		t.Fatal("incorrect money contract")
	}
	if _, err := time.Parse(time.RFC3339Nano, envelope.OccurredAt); err != nil {
		t.Fatal(err)
	}
	if envelope.OccurredAt[len(envelope.OccurredAt)-1:] != "Z" {
		t.Fatal("timestamp must use UTC")
	}
	for _, field := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "referenceExternalTransactionId", "causationId"} {
		if bytes.Contains(encoded, []byte(`"`+field+`"`)) {
			t.Fatalf("unexpected opening field %s", field)
		}
	}
	balanceJSON, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"walletVersion":1`, `"direction":"CREDIT"`, `"balanceBefore":{"amount":"0.00","currency":"BRL"}`, `"balanceAfter":{"amount":"100.00","currency":"BRL"}`} {
		if !bytes.Contains(balanceJSON, []byte(field)) {
			t.Fatalf("missing field %s in %s", field, balanceJSON)
		}
	}
	if changed.EventID() == processed.EventID() {
		t.Fatal("different events must have different IDs")
	}
	if _, err := NewWalletBalanceChanged(opening, entry, 2, "request-1", ""); err == nil {
		t.Fatal("opening must retain wallet version 1")
	}
}

func TestPendingEventIsSnapshot(t *testing.T) {
	bet := newWagerForTest(t, WagerKindBet, "25", nil)
	refund := newReferencedWagerForTest(t, WagerKindRefund, bet)
	if err := refund.MarkPendingReference(); err != nil {
		t.Fatal(err)
	}
	event, err := NewWagerTransactionPendingReference(refund, "request-1", "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := refund.Reject("REFERENCE_NOT_FOUND"); err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("event changed with transaction")
	}
	if !bytes.Contains(after, []byte(`"causationId":"msg-1"`)) || !bytes.Contains(after, []byte(`"eventType":"WagerTransactionPendingReference"`)) {
		t.Fatal(string(after))
	}
	rejected, err := NewWagerTransactionRejected(refund, "request-1", "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	rejectionJSON, err := json.Marshal(rejected)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(rejectionJSON, []byte(`"failureCode":"REFERENCE_NOT_FOUND"`)) {
		t.Fatal(string(rejectionJSON))
	}
	if _, err := NewWagerTransactionProcessed(refund, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
}

func TestLossProducesOnlyProcessedEvent(t *testing.T) {
	loss := newWagerForTest(t, WagerKindLoss, "0", nil)
	if err := loss.MarkProcessed(newTestMoney(t, "100")); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWagerTransactionProcessed(loss, "request-1", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWalletBalanceChanged(loss, nil, 1, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err := NewWagerTransactionProcessed(loss, " ", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err := NewWagerTransactionRejected(loss, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err := NewWagerTransactionPendingReference(loss, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
}

func TestBalanceChangedRequiresMatchingLedger(t *testing.T) {
	bet := newWagerForTest(t, WagerKindBet, "25", nil)
	before := newTestMoney(t, "100")
	after := newTestMoney(t, "75")
	if err := bet.MarkProcessed(after); err != nil {
		t.Fatal(err)
	}
	entry, err := NewWalletLedgerEntry(bet.WalletID(), bet.ID(), LedgerDirectionDebit, bet.Money(), before, after)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewWalletBalanceChanged(bet, entry, 2, "request-1", ""); err != nil {
		t.Fatal(err)
	}
	wrong, err := NewWalletLedgerEntry(uuid.New(), bet.ID(), LedgerDirectionDebit, bet.Money(), before, after)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewWalletBalanceChanged(bet, wrong, 2, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err := NewWalletBalanceChanged(bet, entry, 1, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
}

func TestEventsRejectUninitializedValues(t *testing.T) {
	for _, event := range []json.Marshaler{WagerTransactionProcessed{}, WagerTransactionRejected{}, WagerTransactionPendingReference{}, WalletBalanceChanged{}} {
		if _, err := event.MarshalJSON(); !errors.Is(err, ErrInvalidEvent) {
			t.Fatal(err)
		}
	}
	if _, err := NewWagerTransactionProcessed(nil, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err := NewWagerTransactionRejected(nil, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err := NewWagerTransactionPendingReference(nil, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err := NewWalletBalanceChanged(nil, nil, 1, "request-1", ""); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
}
