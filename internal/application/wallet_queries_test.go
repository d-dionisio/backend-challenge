package application

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type walletQueriesStub struct {
	list     func(context.Context, uuid.UUID, *ports.LedgerPosition, int) ([]ports.LedgerRecord, error)
	snapshot *ports.ReconciliationSnapshot
	err      error
}

func (s walletQueriesStub) ListLedger(ctx context.Context, id uuid.UUID, position *ports.LedgerPosition, limit int) ([]ports.LedgerRecord, error) {
	return s.list(ctx, id, position, limit)
}
func (s walletQueriesStub) ReconciliationSnapshot(ctx context.Context, id uuid.UUID) (*ports.ReconciliationSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.snapshot, s.err
}

func TestReconcileWalletDifference(t *testing.T) {
	for _, test := range []struct{ stored, calculated, want int64 }{
		{0, 0, 0}, {97500, 97500, 0}, {97501, 97500, 1}, {97499, 97500, -1},
		{9007199254740993, 9007199254740992, 1},
	} {
		stored, _ := domain.MoneyFromMinorUnits(test.stored, "BRL")
		calculated, _ := domain.MoneyFromMinorUnits(test.calculated, "BRL")
		id := uuid.New()
		queries := walletQueriesStub{snapshot: &ports.ReconciliationSnapshot{WalletID: id, StoredBalance: stored, CalculatedBalance: calculated, CheckedEntries: 2}}
		result, err := NewReconcileWallet(queries).Execute(context.Background(), id)
		if err != nil || result.Difference.Amount() != test.want || result.Consistent != (test.want == 0) || result.CheckedEntries != 2 {
			t.Fatal(result, err)
		}
		if queries.snapshot.StoredBalance != stored || queries.snapshot.CalculatedBalance != calculated {
			t.Fatal("reconciliation mutated input")
		}
	}
}

func TestReconcileWalletErrors(t *testing.T) {
	if _, err := NewReconcileWallet(walletQueriesStub{}).Execute(context.Background(), uuid.Nil); !errors.Is(err, domain.ErrInvalidWallet) {
		t.Fatal(err)
	}
	for _, expected := range []error{ports.ErrWalletNotFound, context.DeadlineExceeded} {
		_, err := NewReconcileWallet(walletQueriesStub{err: expected}).Execute(context.Background(), uuid.New())
		if !errors.Is(err, expected) {
			t.Fatal(err)
		}
	}
	stored, _ := domain.MoneyFromMinorUnits(math.MaxInt64, "BRL")
	negative, _ := domain.MoneyFromMinorUnits(-1, "BRL")
	queries := walletQueriesStub{snapshot: &ports.ReconciliationSnapshot{StoredBalance: stored, CalculatedBalance: negative}}
	_, err := NewReconcileWallet(queries).Execute(context.Background(), uuid.New())
	if !errors.Is(err, ports.ErrInvalidFinancialState) || !errors.Is(err, domain.ErrMoneyOverflow) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewReconcileWallet(queries).Execute(ctx, uuid.New()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLedgerCursorContinuation(t *testing.T) {
	walletID := uuid.New()
	created := time.Date(2026, 9, 21, 12, 0, 0, 123456000, time.UTC)
	first, second := uuid.New(), uuid.New()
	queries := walletQueriesStub{list: func(ctx context.Context, id uuid.UUID, position *ports.LedgerPosition, limit int) ([]ports.LedgerRecord, error) {
		if id != walletID || limit != 2 {
			t.Fatal("wrong wallet or lookahead limit")
		}
		if position == nil {
			return []ports.LedgerRecord{{ID: first, WalletID: id, CreatedAt: created}, {ID: second, WalletID: id, CreatedAt: created}}, nil
		}
		if position.ID != first || !position.CreatedAt.Equal(created) {
			t.Fatal("cursor did not retain exact last item")
		}
		return []ports.LedgerRecord{{ID: second, WalletID: id, CreatedAt: created}}, nil
	}}
	page, err := NewListLedger(queries).Execute(context.Background(), walletID, "", 1)
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatal(page, err)
	}
	// Um novo caso de uso recebe o mesmo cursor: nenhum estado local é necessário.
	next, err := NewListLedger(queries).Execute(context.Background(), walletID, page.NextCursor, 1)
	if err != nil || len(next.Items) != 1 || next.Items[0].ID != second || next.NextCursor != "" {
		t.Fatal(next, err)
	}
	if _, err := NewListLedger(queries).Execute(context.Background(), uuid.New(), page.NextCursor, 1); !errors.Is(err, ErrInvalidLedgerPage) {
		t.Fatal(err)
	}
}

func TestInvalidLedgerPagesDoNotQueryStorage(t *testing.T) {
	id := uuid.New()
	queries := walletQueriesStub{list: func(context.Context, uuid.UUID, *ports.LedgerPosition, int) ([]ports.LedgerRecord, error) {
		t.Fatal("invalid input reached storage")
		return nil, nil
	}}
	encode := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	for _, cursor := range []string{"!invalid", encode(`{}`), encode(`null`), encode(`{} {}`), strings.Repeat("a", 513), encode(`{"id":"invalid"}`)} {
		if _, err := NewListLedger(queries).Execute(context.Background(), id, cursor, 50); !errors.Is(err, ErrInvalidLedgerPage) {
			t.Fatal(cursor, err)
		}
	}
	for _, limit := range []int{-1, 0, 101} {
		if _, err := NewListLedger(queries).Execute(context.Background(), id, "", limit); !errors.Is(err, ErrInvalidLedgerPage) {
			t.Fatal(limit, err)
		}
	}
	if _, err := NewListLedger(queries).Execute(context.Background(), uuid.Nil, "", 50); !errors.Is(err, domain.ErrInvalidWallet) {
		t.Fatal(err)
	}
}

func TestEmptyLedgerPageIsJSONArray(t *testing.T) {
	queries := walletQueriesStub{list: func(context.Context, uuid.UUID, *ports.LedgerPosition, int) ([]ports.LedgerRecord, error) {
		return nil, nil
	}}
	page, err := NewListLedger(queries).Execute(context.Background(), uuid.New(), "", 50)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(page)
	if err != nil || string(body) != `{"items":[]}` {
		t.Fatal(string(body), err)
	}
}
