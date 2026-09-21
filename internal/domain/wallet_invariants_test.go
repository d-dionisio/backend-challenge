package domain

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWalletRejectsInvalidCreationAndRehydration(t *testing.T) {
	negative, _ := MoneyFromMinorUnits(-1, "BRL")
	zero, _ := ZeroMoney("BRL")
	for _, balance := range []Money{{}, negative} {
		if _, err := NewWallet(uuid.New(), balance); !errors.Is(err, ErrInvalidWallet) {
			t.Fatal(err)
		}
	}
	if _, err := NewWallet(uuid.Nil, zero); !errors.Is(err, ErrInvalidWallet) {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, tc := range []struct {
		balance          Money
		version          int64
		created, updated time.Time
	}{
		{Money{}, 1, now, now}, {negative, 1, now, now}, {zero, 0, now, now}, {zero, 1, time.Time{}, now}, {zero, 1, now, now.Add(-time.Second)},
	} {
		if _, err := RehydrateWallet(uuid.New(), uuid.New(), tc.balance, tc.version, tc.created, tc.updated); !errors.Is(err, ErrInvalidWallet) {
			t.Fatal(err)
		}
	}
	w, err := RehydrateWallet(uuid.New(), uuid.New(), zero, 42, now, now)
	if err != nil || w.Version() != 42 || w.Balance() != zero || !w.UpdatedAt().Equal(now) {
		t.Fatal("rehydration changed state", err)
	}
}

func TestWalletRejectedMovementsDoNotMutate(t *testing.T) {
	negative, _ := MoneyFromMinorUnits(-1, "BRL")
	zero, _ := ZeroMoney("BRL")
	usd, _ := NewMoney("1", "USD")
	for _, amount := range []Money{{}, negative, zero, usd} {
		for _, debit := range []bool{false, true} {
			w, _ := NewWallet(uuid.New(), newTestMoney(t, "100"))
			before := *w
			var err error
			if debit {
				err = w.Debit(amount)
			} else {
				err = w.Credit(amount)
			}
			if err == nil || *w != before {
				t.Fatalf("invalid movement changed wallet: %v", err)
			}
		}
	}
	var w Wallet
	if err := w.Credit(newTestMoney(t, "1")); !errors.Is(err, ErrInvalidWallet) {
		t.Fatal(err)
	}
	if err := w.Debit(newTestMoney(t, "1")); !errors.Is(err, ErrInvalidWallet) {
		t.Fatal(err)
	}
}

func TestWalletVersionAndOverflow(t *testing.T) {
	w, _ := NewWallet(uuid.New(), newTestMoney(t, "100"))
	if err := w.Debit(newTestMoney(t, "80")); err != nil {
		t.Fatal(err)
	}
	if w.Version() != 2 || w.Balance().String() != "20.00" {
		t.Fatal("invalid debit")
	}
	if err := w.Credit(newTestMoney(t, "5")); err != nil {
		t.Fatal(err)
	}
	if w.Version() != 3 || w.Balance().String() != "25.00" {
		t.Fatal("invalid credit")
	}
	now := time.Now().UTC()
	maximum, _ := MoneyFromMinorUnits(math.MaxInt64, "BRL")
	w, _ = NewWallet(uuid.New(), maximum)
	before := *w
	if err := w.Credit(newTestMoney(t, "0.01")); !errors.Is(err, ErrMoneyOverflow) || *w != before {
		t.Fatal("overflow mutated wallet", err)
	}
	w, _ = RehydrateWallet(uuid.New(), uuid.New(), maximum, math.MaxInt64, now, now)
	before = *w
	if err := w.Debit(newTestMoney(t, "0.01")); !errors.Is(err, ErrInvalidWallet) || *w != before {
		t.Fatal("version overflow mutated wallet", err)
	}
}
