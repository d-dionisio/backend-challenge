package application

import (
	"context"
	"errors"
	"testing"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type failingUnitOfWork struct {
	called bool
	err    error
}

func (unit *failingUnitOfWork) WithinTransaction(context.Context, func(ports.Repositories) error) error {
	unit.called = true
	return unit.err
}

func TestOpenWalletRejectsInvalidInputBeforeTransaction(t *testing.T) {
	zero, _ := domain.ZeroMoney("BRL")
	negative, _ := domain.MoneyFromMinorUnits(-1, "BRL")
	for _, input := range []OpenWalletInput{
		{PlayerID: uuid.Nil, InitialBalance: zero},
		{PlayerID: uuid.New(), InitialBalance: domain.Money{}},
		{PlayerID: uuid.New(), InitialBalance: negative},
	} {
		unit := &failingUnitOfWork{}
		wallet, err := NewOpenWallet(unit).Execute(context.Background(), input)
		if !errors.Is(err, domain.ErrInvalidWallet) || wallet != nil || unit.called {
			t.Fatalf("invalid input reached persistence: wallet=%v err=%v called=%v", wallet, err, unit.called)
		}
	}
}

func TestOpenWalletDoesNotReturnUnconfirmedWallet(t *testing.T) {
	zero, _ := domain.ZeroMoney("BRL")
	for _, failure := range []error{ports.ErrWalletConflict, errors.New("database unavailable")} {
		unit := &failingUnitOfWork{err: failure}
		wallet, err := NewOpenWallet(unit).Execute(context.Background(), OpenWalletInput{PlayerID: uuid.New(), InitialBalance: zero})
		if !unit.called || !errors.Is(err, failure) || wallet != nil {
			t.Fatalf("unconfirmed wallet returned: wallet=%v err=%v", wallet, err)
		}
	}
}

func TestOpenWalletRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	unit := &failingUnitOfWork{}
	zero, _ := domain.ZeroMoney("BRL")
	wallet, err := NewOpenWallet(unit).Execute(ctx, OpenWalletInput{PlayerID: uuid.New(), InitialBalance: zero})
	if !errors.Is(err, context.Canceled) || wallet != nil || unit.called {
		t.Fatalf("canceled request reached persistence: %v", err)
	}
}
