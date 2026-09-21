//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type ledgerHTTPPage struct {
	Items []struct {
		ID, WalletID, TransactionID        uuid.UUID
		Direction                          string
		Money, BalanceBefore, BalanceAfter struct{ Amount, Currency string }
		CreatedAt                          time.Time
	} `json:"items"`
	NextCursor string `json:"nextCursor"`
}

func decodeLedgerPage(t *testing.T, body string) ledgerHTTPPage {
	t.Helper()
	var page ledgerHTTPPage
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func TestHTTPLedgerPagination(t *testing.T) {
	f := startHTTPAPI(t, false)
	ctx := context.Background()
	token := keycloakToken(t, "wallet-service")
	// Fixture: todos os lançamentos têm o mesmo timestamp para testar o desempate.
	if _, err := f.pool.Exec(ctx, `CREATE FUNCTION fixed_ledger_date() RETURNS TRIGGER AS $$ BEGIN NEW.created_at := '2026-01-01T00:00:00Z'; RETURN NEW; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fixed_ledger_date BEFORE INSERT ON wallet_ledger_entries FOR EACH ROW EXECUTE FUNCTION fixed_ledger_date();`); err != nil {
		t.Fatal(err)
	}
	wallet := openWagerWallet(t, ctx, f.pool, "100")
	useCase := application.NewProcessWager(NewUnitOfWork(f.pool))
	for i := 0; i < 3; i++ {
		if _, err := useCase.Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindBet, "1", fmt.Sprintf("page-bet-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	path := f.address + "/wallets/" + wallet.ID().String() + "/ledger"
	complete := decodeLedgerPage(t, expectHTTP(t, "GET", path, token, "", "", 200))
	if len(complete.Items) != 4 || complete.NextCursor != "" {
		t.Fatal(complete)
	}
	for i, item := range complete.Items {
		if item.WalletID != wallet.ID() || item.TransactionID == uuid.Nil || item.Money.Currency != "BRL" || item.CreatedAt.Format(time.RFC3339) != "2026-01-01T00:00:00Z" {
			t.Fatal(item)
		}
		if i > 0 && complete.Items[i-1].ID.String() <= item.ID.String() {
			t.Fatal("unstable UUID tie ordering")
		}
	}
	first := decodeLedgerPage(t, expectHTTP(t, "GET", path+"?limit=2", token, "", "", 200))
	if len(first.Items) != 2 || first.NextCursor == "" {
		t.Fatal(first)
	}
	if _, err := f.pool.Exec(ctx, `DROP TRIGGER fixed_ledger_date ON wallet_ledger_entries`); err != nil {
		t.Fatal(err)
	}
	// Um lançamento novo fica no início. Ele não desloca a continuação da página.
	if _, err := useCase.Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindWin, "1", "new-page-win")); err != nil {
		t.Fatal(err)
	}
	next := decodeLedgerPage(t, expectHTTP(t, "GET", path+"?limit=2&cursor="+first.NextCursor, token, "", "", 200))
	if len(next.Items) != 2 || next.NextCursor != "" {
		t.Fatal(next)
	}
	for i, item := range append(first.Items, next.Items...) {
		if item.ID != complete.Items[i].ID {
			t.Fatal("pagination skipped or repeated an entry")
		}
	}
	for _, query := range []string{"limit=0", "limit=-1", "limit=101", "limit=abc", "limit=", "limit=1&limit=2", "cursor=bad!", "cursor=x&cursor=y", "cursor=%ZZ"} {
		expectHTTP(t, "GET", path+"?"+query, token, "", "", 400)
	}
	other := openWagerWallet(t, ctx, f.pool, "0")
	otherPath := f.address + "/wallets/" + other.ID().String() + "/ledger"
	empty := expectHTTP(t, "GET", otherPath, token, "", "", 200)
	if strings.TrimSpace(empty) != `{"items":[]}` {
		t.Fatal(empty)
	}
	expectHTTP(t, "GET", otherPath+"?cursor="+first.NextCursor, token, "", "", 400)
	expectHTTP(t, "GET", f.address+"/wallets/"+uuid.NewString()+"/ledger", token, "", "", 404)
	expectHTTP(t, "GET", f.address+"/wallets/invalid/ledger", token, "", "", 400)
	expectHTTP(t, "GET", path, "", "", "", 401)
	expectHTTP(t, "GET", path, keycloakToken(t, "provider-a"), "", "", 403)
	assertWalletLedger(t, ctx, f.pool, wallet.ID(), 9800)
}

type reconciliationHTTPResult struct {
	WalletID                                     uuid.UUID
	StoredBalance, CalculatedBalance, Difference struct{ Amount, Currency string }
	Consistent                                   bool
	CheckedEntries                               int64
}

func decodeReconciliation(t *testing.T, body string) reconciliationHTTPResult {
	t.Helper()
	var result reconciliationHTTPResult
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestHTTPReconciliationAndDivergenceMetric(t *testing.T) {
	f := startHTTPAPI(t, false)
	ctx := context.Background()
	token := keycloakToken(t, "wallet-service")
	wallet := openWagerWallet(t, ctx, f.pool, "1000")
	if _, err := application.NewProcessWager(NewUnitOfWork(f.pool)).Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindBet, "25", "reconcile-bet")); err != nil {
		t.Fatal(err)
	}
	path := f.address + "/wallets/" + wallet.ID().String() + "/reconciliation"
	result := decodeReconciliation(t, expectHTTP(t, "POST", path, token, "", "", 200))
	if !result.Consistent || result.WalletID != wallet.ID() || result.StoredBalance.Amount != "975.00" || result.CalculatedBalance.Amount != "975.00" || result.Difference.Amount != "0.00" || result.CheckedEntries != 2 {
		t.Fatal(result)
	}
	metrics := expectHTTP(t, "GET", f.address+"/metrics", "", "", "", 200)
	if !strings.Contains(metrics, "wallet_reconciliation_divergences_total 0\n") {
		t.Fatal(metrics)
	}
	expectHTTP(t, "POST", path, "", "", "", 401)
	expectHTTP(t, "POST", path, keycloakToken(t, "provider-a"), "", "", 403)
	expectHTTP(t, "POST", f.address+"/wallets/"+uuid.NewString()+"/reconciliation", token, "", "", 404)
	expectHTTP(t, "POST", f.address+"/wallets/invalid/reconciliation", token, "", "", 400)
	zero := openWagerWallet(t, ctx, f.pool, "0")
	empty := decodeReconciliation(t, expectHTTP(t, "POST", f.address+"/wallets/"+zero.ID().String()+"/reconciliation", token, "", "", 200))
	if !empty.Consistent || empty.CalculatedBalance.Amount != "0.00" || empty.CheckedEntries != 0 {
		t.Fatal(empty)
	}
	// Corrupção proposital APENAS no schema isolado do teste. A aplicação
	// normal não pode confirmar essa alteração por causa da constraint.
	if _, err := f.pool.Exec(ctx, `UPDATE wallets SET balance_cents=balance_cents+1 WHERE id=$1`, wallet.ID()); err == nil {
		t.Fatal("schema accepted inconsistent balance")
	}
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `ALTER TABLE wallets DISABLE TRIGGER check_wallet_financial_state`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance_cents=balance_cents+1 WHERE id=$1`, wallet.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE wallets ENABLE TRIGGER check_wallet_financial_state`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		result = decodeReconciliation(t, expectHTTP(t, "POST", path, token, "", "", 200))
		if result.Consistent || result.StoredBalance.Amount != "975.01" || result.CalculatedBalance.Amount != "975.00" || result.Difference.Amount != "0.01" || result.CheckedEntries != 2 {
			t.Fatal(result)
		}
		metrics = expectHTTP(t, "GET", f.address+"/metrics", "", "", "", 200)
		if !strings.Contains(metrics, fmt.Sprintf("wallet_reconciliation_divergences_total %d\n", i)) {
			t.Fatal(metrics)
		}
	}
	stored, err := NewWalletRepository(f.pool).FindByID(ctx, wallet.ID())
	if err != nil || stored.Balance().Amount() != 97501 || stored.Version() != 2 {
		t.Fatal("reconciliation changed wallet", err)
	}
	if countRows(t, f.pool, "wallet_ledger_entries") != 2 || countRows(t, f.pool, "outbox") != 4 || countRows(t, f.pool, "wager_transactions") != 2 {
		t.Fatal("reconciliation changed financial records")
	}
}

type pauseWalletCommit struct {
	unit     ports.UnitOfWork
	prepared chan struct{}
	release  chan struct{}
}

func (p pauseWalletCommit) WithinTransaction(ctx context.Context, work func(ports.Repositories) error) error {
	return p.unit.WithinTransaction(ctx, func(r ports.Repositories) error {
		if err := work(r); err != nil {
			return err
		}
		close(p.prepared)
		select {
		case <-p.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func TestReconciliationConsistentSnapshotDuringCommit(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, pool, "100")
	pause := pauseWalletCommit{unit: NewUnitOfWork(pool), prepared: make(chan struct{}), release: make(chan struct{})}
	completed := make(chan error, 1)
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "uncommitted-bet")
	go func() {
		_, err := application.NewProcessWager(pause).Execute(ctx, "provider-a", input)
		completed <- err
	}()
	select {
	case <-pause.prepared:
	case err := <-completed:
		t.Fatal("operation failed before commit", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	useCase := application.NewReconcileWallet(NewWalletQueries(pool))
	// A consulta não espera o lock do escritor nem vê seu trabalho incompleto.
	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	before, err := useCase.Execute(readCtx, wallet.ID())
	readCancel()
	close(pause.release)
	if err != nil || !before.Consistent || before.StoredBalance.Amount() != 10000 || before.CheckedEntries != 1 {
		t.Fatal(before, err)
	}
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	after, err := useCase.Execute(ctx, wallet.ID())
	if err != nil || !after.Consistent || after.StoredBalance.Amount() != 7500 || after.CheckedEntries != 2 {
		t.Fatal(after, err)
	}
	// Commits e reconciliações simultâneos: saldo e contagem precisam corresponder.
	inputs := make([]application.ProcessWagerInput, 20)
	for i := range inputs {
		inputs[i] = wagerInput(t, wallet, domain.WagerKindWin, "1", fmt.Sprintf("snapshot-win-%d", i))
	}
	go func() {
		for _, input := range inputs {
			if _, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input); err != nil {
				completed <- err
				return
			}
		}
		completed <- nil
	}()
	for i := 0; i < 30; i++ {
		result, err := useCase.Execute(ctx, wallet.ID())
		if err != nil || !result.Consistent || result.StoredBalance.Amount() != 7500+(result.CheckedEntries-2)*100 {
			t.Fatal("mixed financial snapshots", result, err)
		}
	}
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 9500)
}

func TestReconciliationLargeHistoryAndReadOnly(t *testing.T) {
	pool := storageTestPool(t)
	ctx := context.Background()
	const maximum = "92233720368547758.07"
	wallet := openWagerWallet(t, ctx, pool, maximum)
	useCase := application.NewProcessWager(NewUnitOfWork(pool))
	for i, kind := range []domain.WagerKind{domain.WagerKindBet, domain.WagerKindWin, domain.WagerKindBet, domain.WagerKindWin} {
		if _, err := useCase.Execute(ctx, "provider-a", wagerInput(t, wallet, kind, maximum, fmt.Sprintf("large-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := useCase.Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindLoss, "0", "large-loss")); err != nil {
		t.Fatal(err)
	}
	// O saldo já está no máximo: o WIN é rejeitado por overflow, sem ledger.
	if _, err := useCase.Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindWin, "1", "large-rejected")); err != nil {
		t.Fatal(err)
	}
	queries := NewWalletQueries(pool)
	result, err := application.NewReconcileWallet(queries).Execute(ctx, wallet.ID())
	if err != nil || !result.Consistent || result.CalculatedBalance.String() != maximum || result.CheckedEntries != 5 {
		t.Fatal(result, err)
	}
	if _, err := queries.ReconciliationSnapshot(ctx, uuid.New()); !errors.Is(err, ports.ErrWalletNotFound) {
		t.Fatal(err)
	}
	if _, err := queries.ListLedger(ctx, uuid.New(), nil, 50); !errors.Is(err, ports.ErrWalletNotFound) {
		t.Fatal(err)
	}
	entries, err := queries.ListLedger(ctx, wallet.ID(), nil, 50)
	if err != nil || len(entries) != 5 {
		t.Fatal(len(entries), err)
	}
	for _, entry := range entries {
		if entry.Money.String() != maximum {
			t.Fatal("ledger lost precision")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := queries.ReconciliationSnapshot(ctx, wallet.ID()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
