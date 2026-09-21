package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func newWagerForTest(t *testing.T, kind WagerKind, amount string, reference *string) *WagerTransaction {
	t.Helper()
	transaction, err := NewWagerTransaction("provider-a", uuid.NewString(), "key", "hash",
		uuid.New(), uuid.New(), "round-1", "game-1", kind, newTestMoney(t, amount), reference)
	if err != nil {
		t.Fatal(err)
	}
	return transaction
}

func wagerStateForTest(transaction *WagerTransaction) WagerTransactionState {
	return WagerTransactionState{
		ID: transaction.ID(), ExternalTransactionID: transaction.ExternalTransactionID(),
		ProviderID: transaction.ProviderID(), IdempotencyKey: transaction.IdempotencyKey(),
		PayloadHash: transaction.PayloadHash(), WalletID: transaction.WalletID(), PlayerID: transaction.PlayerID(),
		RoundID: transaction.RoundID(), GameID: transaction.GameID(), Kind: transaction.Kind(), Money: transaction.Money(),
		ReferenceExternalTransactionID: transaction.ReferenceExternalTransactionID(),
		ReferenceTransactionID:         transaction.ReferenceTransactionID(), Status: transaction.Status(),
		FailureCode: transaction.FailureCode(), ResultBalance: transaction.ResultBalance(),
		CreatedAt: transaction.CreatedAt(), UpdatedAt: transaction.UpdatedAt(),
	}
}

func newReferencedWagerForTest(t *testing.T, kind WagerKind, reference *WagerTransaction) *WagerTransaction {
	t.Helper()
	externalID := reference.ExternalTransactionID()
	transaction, err := NewWagerTransaction(reference.ProviderID(), uuid.NewString(), "key", "hash",
		reference.WalletID(), reference.PlayerID(), reference.RoundID(), reference.GameID(), kind, reference.Money(), &externalID)
	if err != nil {
		t.Fatal(err)
	}
	return transaction
}

func TestWagerAmountRules(t *testing.T) {
	reference := "bet-1"
	negative, _ := MoneyFromMinorUnits(-1, "BRL")
	for _, kind := range []WagerKind{WagerKindBet, WagerKindWin, WagerKindLoss, WagerKindRefund, WagerKindRollback} {
		for _, money := range []Money{newTestMoney(t, "0"), newTestMoney(t, "1"), negative, {}} {
			_, err := NewWagerTransaction("provider", "external", "key", "hash", uuid.New(), uuid.New(), "round", "game", kind, money, &reference)
			valid := money.IsPositive()
			if kind == WagerKindLoss {
				valid = money.IsZero()
			}
			if valid && err != nil {
				t.Fatalf("%s %v: %v", kind, money, err)
			}
			if !valid && !errors.Is(err, ErrInvalidWagerMoney) {
				t.Fatalf("%s %v: %v", kind, money, err)
			}
		}
	}
	for _, kind := range []WagerKind{WagerKindRefund, WagerKindRollback} {
		_, err := NewWagerTransaction("provider", "external", "key", "hash", uuid.New(), uuid.New(), "round", "game", kind, newTestMoney(t, "1"), nil)
		if !errors.Is(err, ErrReferenceRequired) {
			t.Fatal(kind, err)
		}
	}
}

func TestRollbackReferenceKinds(t *testing.T) {
	bet := newWagerForTest(t, WagerKindBet, "25", nil)
	if err := bet.MarkProcessed(newTestMoney(t, "75")); err != nil {
		t.Fatal(err)
	}
	win := newWagerForTest(t, WagerKindWin, "25", nil)
	if err := win.MarkProcessed(newTestMoney(t, "125")); err != nil {
		t.Fatal(err)
	}
	refund := newReferencedWagerForTest(t, WagerKindRefund, bet)
	if err := refund.ResolveReference(bet); err != nil {
		t.Fatal(err)
	}
	if err := refund.MarkProcessed(newTestMoney(t, "100")); err != nil {
		t.Fatal(err)
	}
	for _, reference := range []*WagerTransaction{bet, win, refund} {
		rollback := newReferencedWagerForTest(t, WagerKindRollback, reference)
		if err := rollback.ResolveReference(reference); err != nil {
			t.Fatal(reference.Kind(), err)
		}
		if err := rollback.MarkProcessed(newTestMoney(t, "100")); err != nil {
			t.Fatal(err)
		}
		secondRollback := newReferencedWagerForTest(t, WagerKindRollback, rollback)
		if err := secondRollback.ResolveReference(rollback); !errors.Is(err, ErrInvalidWagerReference) {
			t.Fatal("rollback cannot reference another rollback", err)
		}
	}
}

func TestWinReferenceAllowsDifferentAmount(t *testing.T) {
	bet := newWagerForTest(t, WagerKindBet, "25", nil)
	if err := bet.MarkProcessed(newTestMoney(t, "75")); err != nil {
		t.Fatal(err)
	}
	externalID := bet.ExternalTransactionID()
	win, err := NewWagerTransaction(bet.ProviderID(), "win-1", "key", "hash",
		bet.WalletID(), bet.PlayerID(), bet.RoundID(), bet.GameID(), WagerKindWin,
		newTestMoney(t, "50"), &externalID)
	if err != nil {
		t.Fatal(err)
	}
	if err := win.ResolveReference(bet); err != nil {
		t.Fatal(err)
	}
	if err := win.MarkProcessed(newTestMoney(t, "125")); err != nil {
		t.Fatal(err)
	}
}

func TestWagerCopiesConstructorReferenceAndFailureCode(t *testing.T) {
	reference := "bet-1"
	refund := newWagerForTest(t, WagerKindRefund, "25", &reference)
	reference = "changed"
	if *refund.ReferenceExternalTransactionID() != "bet-1" {
		t.Fatal("constructor retained a mutable pointer")
	}
	if err := refund.Reject("REFERENCE_NOT_FOUND"); err != nil {
		t.Fatal(err)
	}
	state := wagerStateForTest(refund)
	loaded, err := RehydrateWagerTransaction(state)
	if err != nil {
		t.Fatal(err)
	}
	*state.FailureCode = "changed"
	*loaded.FailureCode() = "changed again"
	if *loaded.FailureCode() != "REFERENCE_NOT_FOUND" {
		t.Fatal("failure code is mutable outside the transaction")
	}
}

func TestOpeningHasOnlyInternalMetadata(t *testing.T) {
	wallet, _ := NewWallet(uuid.New(), newTestMoney(t, "100"))
	opening, err := NewOpeningTransaction(wallet)
	if err != nil {
		t.Fatal(err)
	}
	if opening.Status() != WagerStatusProcessed || opening.Kind() != WagerKindOpening {
		t.Fatal("incorrect opening")
	}
	if opening.ProviderID() != "" || opening.ExternalTransactionID() != "" || opening.IdempotencyKey() != "" || opening.PayloadHash() != "" {
		t.Fatal("external metadata on opening")
	}
	if opening.RoundID() != "" || opening.GameID() != "" || opening.ReferenceExternalTransactionID() != nil || opening.ReferenceTransactionID() != nil {
		t.Fatal("external reference on opening")
	}
	if opening.WalletID() != wallet.ID() || opening.PlayerID() != wallet.PlayerID() || *opening.ResultBalance() != wallet.Balance() {
		t.Fatal("incorrect opening identity/balance")
	}
	if wallet.Version() != 1 || wallet.Balance().String() != "100.00" {
		t.Fatal("opening changed wallet")
	}
	second, err := NewOpeningTransaction(wallet)
	if err != nil || second.ID() != opening.ID() {
		t.Fatal("unstable opening identity", err)
	}
	zeroWallet, _ := NewWallet(uuid.New(), newTestMoney(t, "0"))
	if _, err := NewOpeningTransaction(zeroWallet); err == nil {
		t.Fatal("zero opening must not exist")
	}
	if err := wallet.Credit(newTestMoney(t, "1")); err != nil {
		t.Fatal(err)
	}
	if _, err := NewOpeningTransaction(wallet); err == nil {
		t.Fatal("opening requires initial wallet")
	}
}

func TestWagerPendingReferenceAndResolution(t *testing.T) {
	bet := newWagerForTest(t, WagerKindBet, "25", nil)
	refund := newReferencedWagerForTest(t, WagerKindRefund, bet)
	if err := refund.MarkPendingReference(); err != nil {
		t.Fatal(err)
	}
	if string(refund.Status()) != "PENDING_REFERENCE" {
		t.Fatal(refund.Status())
	}
	if err := refund.MarkPendingReference(); !errors.Is(err, ErrInvalidWagerStatus) {
		t.Fatal(err)
	}
	if err := refund.MarkProcessed(newTestMoney(t, "100")); !errors.Is(err, ErrReferenceRequired) {
		t.Fatal(err)
	}
	if err := refund.ResolveReference(bet); !errors.Is(err, ErrReferenceNotProcessed) {
		t.Fatal(err)
	}
	if err := bet.MarkProcessed(newTestMoney(t, "75")); err != nil {
		t.Fatal(err)
	}
	if err := refund.ResolveReference(bet); err != nil {
		t.Fatal(err)
	}
	if err := refund.MarkProcessed(newTestMoney(t, "100")); err != nil {
		t.Fatal(err)
	}
	if *refund.ReferenceTransactionID() != bet.ID() {
		t.Fatal("reference not saved")
	}
	win := newReferencedWagerForTest(t, WagerKindWin, bet)
	if err := win.MarkPendingReference(); err != nil {
		t.Fatal(err)
	}
	if err := win.ResolveReference(bet); err != nil {
		t.Fatal(err)
	}
	if err := win.MarkProcessed(newTestMoney(t, "100")); err != nil {
		t.Fatal(err)
	}
}

func TestReferenceMustMatchTransaction(t *testing.T) {
	bet := newWagerForTest(t, WagerKindBet, "25", nil)
	if err := bet.MarkProcessed(newTestMoney(t, "75")); err != nil {
		t.Fatal(err)
	}
	changes := []struct {
		name   string
		change func(*WagerTransactionState)
	}{
		{"provider", func(s *WagerTransactionState) { s.ProviderID = "other" }},
		{"external id", func(s *WagerTransactionState) { s.ExternalTransactionID = "other" }},
		{"wallet", func(s *WagerTransactionState) { s.WalletID = uuid.New() }},
		{"player", func(s *WagerTransactionState) { s.PlayerID = uuid.New() }},
		{"round", func(s *WagerTransactionState) { s.RoundID = "other" }},
		{"kind", func(s *WagerTransactionState) { s.Kind = WagerKindWin }},
		{"amount", func(s *WagerTransactionState) { s.Money = newTestMoney(t, "10") }},
		{"currency", func(s *WagerTransactionState) {
			s.Money, _ = NewMoney("25", "USD")
			balance, _ := NewMoney("75", "USD")
			s.ResultBalance = &balance
		}},
	}
	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			refund := newReferencedWagerForTest(t, WagerKindRefund, bet)
			state := wagerStateForTest(bet)
			tc.change(&state)
			reference, err := RehydrateWagerTransaction(state)
			if err != nil {
				t.Fatal(err)
			}
			if err := refund.ResolveReference(reference); !errors.Is(err, ErrInvalidWagerReference) {
				t.Fatal(err)
			}
			if refund.ReferenceTransactionID() != nil {
				t.Fatal("invalid reference was saved")
			}
		})
	}
}

func TestWagerTerminalStatesCannotChange(t *testing.T) {
	for _, status := range []WagerStatus{WagerStatusProcessed, WagerStatusRejected, WagerStatusFailed} {
		transaction := newWagerForTest(t, WagerKindBet, "25", nil)
		var err error
		switch status {
		case WagerStatusProcessed:
			err = transaction.MarkProcessed(newTestMoney(t, "75"))
		case WagerStatusRejected:
			err = transaction.Reject("INSUFFICIENT_BALANCE")
		case WagerStatusFailed:
			err = transaction.Fail("PERMANENT_INFRASTRUCTURE_FAILURE")
		}
		if err != nil {
			t.Fatal(err)
		}
		before := transaction.UpdatedAt()
		for _, transition := range []func() error{
			func() error { return transaction.MarkProcessed(newTestMoney(t, "75")) },
			func() error { return transaction.Reject("OTHER") },
			func() error { return transaction.Fail("OTHER") },
			transaction.MarkPendingReference,
			func() error { return transaction.ResolveReference(nil) },
		} {
			if err := transition(); !errors.Is(err, ErrTerminalTransaction) {
				t.Fatal(status, err)
			}
		}
		if transaction.Status() != status || transaction.UpdatedAt() != before {
			t.Fatal("terminal state changed")
		}
	}
}

func TestWagerRejectsUninitializedAndInvalidTransitions(t *testing.T) {
	var transaction WagerTransaction
	for _, err := range []error{transaction.MarkProcessed(newTestMoney(t, "1")), transaction.MarkPendingReference(), transaction.ResolveReference(nil), transaction.Reject("CODE"), transaction.Fail("CODE")} {
		if !errors.Is(err, ErrInvalidWagerTransaction) {
			t.Fatal(err)
		}
	}
	bet := newWagerForTest(t, WagerKindBet, "1", nil)
	before := bet.UpdatedAt()
	negative, _ := MoneyFromMinorUnits(-1, "BRL")
	usd, _ := NewMoney("1", "USD")
	for _, balance := range []Money{{}, negative, usd} {
		if err := bet.MarkProcessed(balance); err == nil {
			t.Fatal("invalid balance accepted")
		}
	}
	if err := bet.MarkPendingReference(); !errors.Is(err, ErrInvalidWagerStatus) {
		t.Fatal(err)
	}
	if err := bet.Reject(" "); err == nil {
		t.Fatal("empty rejection code accepted")
	}
	if err := bet.Fail(" "); err == nil {
		t.Fatal("empty failure code accepted")
	}
	if bet.Status() != WagerStatusPending || bet.UpdatedAt() != before {
		t.Fatal("invalid transition changed state")
	}
}

func TestRehydrateWagerPreservesStateAndCopiesPointers(t *testing.T) {
	bet := newWagerForTest(t, WagerKindBet, "25", nil)
	if err := bet.MarkProcessed(newTestMoney(t, "75")); err != nil {
		t.Fatal(err)
	}
	refund := newReferencedWagerForTest(t, WagerKindRefund, bet)
	if err := refund.ResolveReference(bet); err != nil {
		t.Fatal(err)
	}
	if err := refund.MarkProcessed(newTestMoney(t, "100")); err != nil {
		t.Fatal(err)
	}
	state := wagerStateForTest(refund)
	loaded, err := RehydrateWagerTransaction(state)
	if err != nil {
		t.Fatal(err)
	}
	*state.ReferenceExternalTransactionID = "changed"
	*state.ReferenceTransactionID = uuid.New()
	*state.ResultBalance = newTestMoney(t, "999")
	*loaded.ResultBalance() = newTestMoney(t, "888")
	*loaded.ReferenceTransactionID() = uuid.New()
	*loaded.ReferenceExternalTransactionID() = "changed again"
	if loaded.ID() != refund.ID() || loaded.Status() != refund.Status() || loaded.CreatedAt() != refund.CreatedAt() || loaded.UpdatedAt() != refund.UpdatedAt() {
		t.Fatal("rehydration changed state")
	}
	if *loaded.ResultBalance() != *refund.ResultBalance() || *loaded.ReferenceTransactionID() != bet.ID() || *loaded.ReferenceExternalTransactionID() != bet.ExternalTransactionID() {
		t.Fatal("pointer leaked internal state")
	}
}

func TestRehydrateWagerRejectsInconsistentState(t *testing.T) {
	bet := newWagerForTest(t, WagerKindBet, "25", nil)
	for _, change := range []func(*WagerTransactionState){
		func(s *WagerTransactionState) { s.ID = uuid.Nil },
		func(s *WagerTransactionState) { s.ProviderID = "" },
		func(s *WagerTransactionState) { s.Kind = "INVALID" },
		func(s *WagerTransactionState) { s.Status = "INVALID" },
		func(s *WagerTransactionState) { s.Status = WagerStatusProcessed },
		func(s *WagerTransactionState) { s.Status = WagerStatusRejected },
		func(s *WagerTransactionState) { s.Status = WagerStatusFailed },
		func(s *WagerTransactionState) { s.Status = WagerStatusPendingReference },
		func(s *WagerTransactionState) { s.Money = Money{} },
		func(s *WagerTransactionState) { s.CreatedAt = time.Time{} },
		func(s *WagerTransactionState) { s.UpdatedAt = s.CreatedAt.Add(-time.Second) },
		func(s *WagerTransactionState) { balance := newTestMoney(t, "1"); s.ResultBalance = &balance },
	} {
		state := wagerStateForTest(bet)
		change(&state)
		if _, err := RehydrateWagerTransaction(state); err == nil {
			t.Fatal("inconsistent state accepted")
		}
	}
}
