//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func applyStorageMigration(t *testing.T, pool *pgxpool.Pool, direction string) {
	t.Helper()
	content, err := os.ReadFile("../../../migrations/000002_transactional_storage." + direction + ".sql")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, string(content)); err != nil {
		t.Fatal(err)
	}
}

func storageTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := repositoryTestPool(t)
	applyStorageMigration(t, pool, "up")
	return pool
}

func storageMoney(t *testing.T, amount string) domain.Money {
	t.Helper()
	money, err := domain.NewMoney(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return money
}

func storeOpening(ctx context.Context, repositories ports.Repositories, wallet *domain.Wallet) error {
	if err := repositories.Wallets.Create(ctx, wallet); err != nil {
		return err
	}
	opening, err := domain.NewOpeningTransaction(wallet)
	if err != nil {
		return err
	}
	if err := repositories.Wagers.Create(ctx, opening); err != nil {
		return err
	}
	zero, err := domain.ZeroMoney("BRL")
	if err != nil {
		return err
	}
	entry, err := domain.NewWalletLedgerEntry(wallet.ID(), opening.ID(), domain.LedgerDirectionCredit, wallet.Balance(), zero, wallet.Balance())
	if err != nil {
		return err
	}
	if err := repositories.Ledger.Create(ctx, entry); err != nil {
		return err
	}
	processed, err := domain.NewWagerTransactionProcessed(opening, "request-1", "")
	if err != nil {
		return err
	}
	changed, err := domain.NewWalletBalanceChanged(opening, entry, 1, "request-1", "")
	if err != nil {
		return err
	}
	if err := repositories.Outbox.Create(ctx, processed); err != nil {
		return err
	}
	return repositories.Outbox.Create(ctx, changed)
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	// Somente nomes fixos definidos pelos testes.
	var count int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestStorageCommitAndRollback(t *testing.T) {
	pool := storageTestPool(t)
	uow := NewUnitOfWork(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	forced := errors.New("failure after all inserts")
	for _, fail := range []bool{true, false} {
		wallet, err := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
		if err != nil {
			t.Fatal(err)
		}
		err = uow.WithinTransaction(ctx, func(r ports.Repositories) error {
			duplicate, err := r.Inbox.Register(ctx, "consumer", "msg-1", "hash-1")
			if err != nil {
				return err
			}
			if duplicate {
				return errors.New("unexpected duplicate")
			}
			if err := storeOpening(ctx, r, wallet); err != nil {
				return err
			}
			if err := r.Inbox.Complete(ctx, "consumer", "msg-1"); err != nil {
				return err
			}
			if fail {
				return forced
			}
			return nil
		})
		if fail && !errors.Is(err, forced) {
			t.Fatal(err)
		}
		if !fail && err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"wallets", "wager_transactions", "wallet_ledger_entries", "inbox", "outbox"} {
			want := 0
			if !fail {
				want = 1
				if table == "outbox" {
					want = 2
				}
			}
			if got := countRows(t, pool, table); got != want {
				t.Fatalf("%s: got %d want %d", table, got, want)
			}
		}
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error {
		duplicate, err := r.Inbox.Register(ctx, "consumer", "msg-1", "hash-1")
		if err != nil {
			return err
		}
		if !duplicate {
			return errors.New("expected completed replay")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error {
		_, err := r.Inbox.Register(ctx, "consumer", "msg-1", "different")
		return err
	}); !errors.Is(err, ports.ErrInboxConflict) {
		t.Fatal(err)
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error {
		_, err := r.Inbox.Register(ctx, "consumer", "incomplete", "hash")
		return err
	}); err == nil {
		t.Fatal("incomplete inbox committed")
	}
	if countRows(t, pool, "inbox") != 1 {
		t.Fatal("failed commit persisted inbox")
	}
}

func TestWagerPersistenceAndProviderScope(t *testing.T) {
	pool := storageTestPool(t)
	uow := NewUnitOfWork(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wallet, _ := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return storeOpening(ctx, r, wallet) }); err != nil {
		t.Fatal(err)
	}
	bet, err := domain.NewWagerTransaction("provider-a", "bet-1", "key-1", "hash-1", wallet.ID(), wallet.PlayerID(), "round", "game", domain.WagerKindBet, storageMoney(t, "25"), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = uow.WithinTransaction(ctx, func(r ports.Repositories) error {
		locked, err := r.Wallets.FindByIDForUpdate(ctx, wallet.ID())
		if err != nil {
			return err
		}
		if err := r.Wagers.Create(ctx, bet); err != nil {
			return err
		}
		before := locked.Balance()
		if err := locked.Debit(bet.Money()); err != nil {
			return err
		}
		if err := bet.MarkProcessed(locked.Balance()); err != nil {
			return err
		}
		if err := r.Wallets.Update(ctx, locked); err != nil {
			return err
		}
		if err := r.Wagers.Update(ctx, bet); err != nil {
			return err
		}
		entry, err := domain.NewWalletLedgerEntry(wallet.ID(), bet.ID(), domain.LedgerDirectionDebit, bet.Money(), before, locked.Balance())
		if err != nil {
			return err
		}
		if err := r.Ledger.Create(ctx, entry); err != nil {
			return err
		}
		processed, err := domain.NewWagerTransactionProcessed(bet, "request-2", "")
		if err != nil {
			return err
		}
		changed, err := domain.NewWalletBalanceChanged(bet, entry, locked.Version(), "request-2", "")
		if err != nil {
			return err
		}
		if err := r.Outbox.Create(ctx, processed); err != nil {
			return err
		}
		if err := r.Outbox.Create(ctx, processed); err != nil {
			return err
		} // Mesma identidade, mesmo snapshot.
		return r.Outbox.Create(ctx, changed)
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRows(t, pool, "outbox") != 4 {
		t.Fatal("duplicate event persisted")
	}
	err = uow.WithinTransaction(ctx, func(r ports.Repositories) error {
		for _, find := range []func() (*domain.WagerTransaction, error){
			func() (*domain.WagerTransaction, error) { return r.Wagers.FindByID(ctx, "provider-a", bet.ID()) },
			func() (*domain.WagerTransaction, error) { return r.Wagers.FindByExternalID(ctx, "provider-a", "bet-1") },
			func() (*domain.WagerTransaction, error) {
				return r.Wagers.FindByIdempotencyKey(ctx, "provider-a", "key-1")
			},
		} {
			loaded, err := find()
			if err != nil {
				return err
			}
			if loaded.ID() != bet.ID() || loaded.ResultBalance().String() != "75.00" || loaded.Status() != domain.WagerStatusProcessed {
				return errors.New("incorrect rehydration")
			}
		}
		if _, err := r.Wagers.FindByID(ctx, "provider-b", bet.ID()); !errors.Is(err, ports.ErrWagerNotFound) {
			return errors.New("provider isolation failed")
		}
		if err := r.Wagers.Create(ctx, bet); !errors.Is(err, ports.ErrWagerConflict) {
			return errors.New("duplicate transaction accepted")
		}
		// ON CONFLICT não invalida a transação SQL: ainda podemos consultar.
		_, err := r.Wagers.FindByIdempotencyKey(ctx, "provider-a", "key-1")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return r.Wagers.Update(ctx, bet) }); !errors.Is(err, ports.ErrWagerNotPending) {
		t.Fatal(err)
	}
	var stored, calculated int64
	err = pool.QueryRow(ctx, `SELECT w.balance_cents, sum(CASE WHEN l.direction='CREDIT' THEN l.amount_cents ELSE -l.amount_cents END)::bigint
		FROM wallets w JOIN wallet_ledger_entries l ON l.wallet_id=w.id WHERE w.id=$1 GROUP BY w.id`, wallet.ID()).Scan(&stored, &calculated)
	if err != nil || stored != 7500 || calculated != stored {
		t.Fatal(stored, calculated, err)
	}
}

func TestStorageSchemaProtections(t *testing.T) {
	pool := storageTestPool(t)
	uow := NewUnitOfWork(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wallet, _ := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return storeOpening(ctx, r, wallet) }); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`UPDATE wallet_ledger_entries SET amount_cents=1`,
		`DELETE FROM wallet_ledger_entries`,
		`TRUNCATE wallet_ledger_entries`,
		`UPDATE wager_transactions SET result_balance_cents=999`,
		`UPDATE outbox SET payload=jsonb_set(payload,'{data}','{}'::jsonb)`,
		`UPDATE wallets SET balance_cents=-1`,
		`UPDATE wallets SET balance_cents=balance_cents+1`,
		`UPDATE wallets SET version=version+1`,
		`INSERT INTO wallet_ledger_entries SELECT gen_random_uuid(), wallet_id, transaction_id, direction, amount_cents, currency, balance_before_cents, balance_after_cents, created_at FROM wallet_ledger_entries`,
	} {
		if _, err := pool.Exec(ctx, query); err == nil {
			t.Fatalf("schema accepted: %s", query)
		}
	}
}

func TestStorageMigrationDownAndUp(t *testing.T) {
	pool := storageTestPool(t)
	applyStorageMigration(t, pool, "down")
	applyStorageMigration(t, pool, "up")
	if countRows(t, pool, "inbox") != 0 || countRows(t, pool, "outbox") != 0 {
		t.Fatal("unexpected data")
	}
}

func TestZeroOpeningAndProcessedWithoutLedger(t *testing.T) {
	pool := storageTestPool(t)
	uow := NewUnitOfWork(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wallet, _ := domain.NewWallet(uuid.New(), storageMoney(t, "0"))
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return r.Wallets.Create(ctx, wallet) }); err != nil {
		t.Fatal(err)
	}
	if countRows(t, pool, "wager_transactions") != 0 || countRows(t, pool, "outbox") != 0 || countRows(t, pool, "wallet_ledger_entries") != 0 {
		t.Fatal("zero opening generated financial records")
	}
	win, err := domain.NewWagerTransaction("provider", "win", "win-key", "hash", wallet.ID(), wallet.PlayerID(), "round", "game", domain.WagerKindWin, storageMoney(t, "25"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := win.MarkProcessed(storageMoney(t, "25")); err != nil {
		t.Fatal(err)
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return r.Wagers.Create(ctx, win) }); err == nil {
		t.Fatal("processed operation committed without ledger")
	}
	if countRows(t, pool, "wager_transactions") != 0 {
		t.Fatal("failed commit retained transaction")
	}
	positive, _ := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return r.Wallets.Create(ctx, positive) }); err == nil {
		t.Fatal("positive wallet committed without opening")
	}
	if countRows(t, pool, "wallets") != 1 {
		t.Fatal("invalid opening persisted")
	}
}

func TestPostgresFxLifecycle(t *testing.T) {
	t.Setenv("DATABASE_URL", os.Getenv("TEST_DATABASE_URL"))
	var unit ports.UnitOfWork
	var pool *pgxpool.Pool
	app := fx.New(Module, fx.Populate(&unit, &pool), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if unit == nil {
		t.Fatal("unit of work was not injected")
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err == nil {
		t.Fatal("pool was not closed")
	}
}

func TestConcurrentInboxDelivery(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wallet, _ := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
	const deliveries = 10
	errorsFound := make(chan error, deliveries)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			uow := NewUnitOfWork(pool)
			errorsFound <- uow.WithinTransaction(ctx, func(r ports.Repositories) error {
				duplicate, err := r.Inbox.Register(ctx, "consumer", "same-message", "same-hash")
				if err != nil {
					return err
				}
				if duplicate {
					return nil
				}
				if err := storeOpening(ctx, r, wallet); err != nil {
					return err
				}
				return r.Inbox.Complete(ctx, "consumer", "same-message")
			})
		}()
	}
	close(start)
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	if countRows(t, pool, "inbox") != 1 || countRows(t, pool, "wallet_ledger_entries") != 1 || countRows(t, pool, "outbox") != 2 {
		t.Fatal("duplicate delivery repeated financial writes")
	}
}

func TestCanceledTransactionRollsBack(t *testing.T) {
	pool := storageTestPool(t)
	uow := NewUnitOfWork(pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wallet, _ := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
	err := uow.WithinTransaction(ctx, func(r ports.Repositories) error {
		if err := storeOpening(ctx, r, wallet); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if countRows(t, pool, "wallets") != 0 || countRows(t, pool, "outbox") != 0 {
		t.Fatal("canceled operation committed")
	}
	if pool.Stat().AcquiredConns() != 0 {
		t.Fatal("transaction did not release connection")
	}
}

func storeMovement(ctx context.Context, r ports.Repositories, operation *domain.WagerTransaction, direction domain.LedgerDirection) error {
	wallet, err := r.Wallets.FindByIDForUpdate(ctx, operation.WalletID())
	if err != nil {
		return err
	}
	if err := r.Wagers.Create(ctx, operation); err != nil {
		return err
	}
	before := wallet.Balance()
	if direction == domain.LedgerDirectionDebit {
		err = wallet.Debit(operation.Money())
	} else {
		err = wallet.Credit(operation.Money())
	}
	if err != nil {
		return err
	}
	if err := operation.MarkProcessed(wallet.Balance()); err != nil {
		return err
	}
	if err := r.Wagers.Update(ctx, operation); err != nil {
		return err
	}
	if err := r.Wallets.Update(ctx, wallet); err != nil {
		return err
	}
	entry, err := domain.NewWalletLedgerEntry(wallet.ID(), operation.ID(), direction, operation.Money(), before, wallet.Balance())
	if err != nil {
		return err
	}
	if err := r.Ledger.Create(ctx, entry); err != nil {
		return err
	}
	processed, err := domain.NewWagerTransactionProcessed(operation, "request", "")
	if err != nil {
		return err
	}
	changed, err := domain.NewWalletBalanceChanged(operation, entry, wallet.Version(), "request", "")
	if err != nil {
		return err
	}
	if err := r.Outbox.Create(ctx, processed); err != nil {
		return err
	}
	return r.Outbox.Create(ctx, changed)
}

func TestReversalUniquenessAndLedgerLink(t *testing.T) {
	pool := storageTestPool(t)
	uow := NewUnitOfWork(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wallet, _ := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return storeOpening(ctx, r, wallet) }); err != nil {
		t.Fatal(err)
	}
	bet, err := domain.NewWagerTransaction("provider", "bet", "bet-key", "hash", wallet.ID(), wallet.PlayerID(), "round", "game", domain.WagerKindBet, storageMoney(t, "25"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return storeMovement(ctx, r, bet, domain.LedgerDirectionDebit) }); err != nil {
		t.Fatal(err)
	}
	betID := bet.ExternalTransactionID()
	refund, err := domain.NewWagerTransaction("provider", "refund", "refund-key", "hash", wallet.ID(), wallet.PlayerID(), "round", "game", domain.WagerKindRefund, bet.Money(), &betID)
	if err != nil {
		t.Fatal(err)
	}
	if err := refund.ResolveReference(bet); err != nil {
		t.Fatal(err)
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return storeMovement(ctx, r, refund, domain.LedgerDirectionCredit) }); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []domain.WagerKind{domain.WagerKindRefund, domain.WagerKindRollback} {
		duplicate, err := domain.NewWagerTransaction("provider", uuid.NewString(), uuid.NewString(), "hash", wallet.ID(), wallet.PlayerID(), "round", "game", kind, bet.Money(), &betID)
		if err != nil {
			t.Fatal(err)
		}
		if err := duplicate.ResolveReference(bet); err != nil {
			t.Fatal(err)
		}
		if err := duplicate.MarkProcessed(storageMoney(t, "125")); err != nil {
			t.Fatal(err)
		}
		if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return r.Wagers.Create(ctx, duplicate) }); !errors.Is(err, ports.ErrWagerConflict) {
			t.Fatal("duplicate reversal accepted", err)
		}
	}
	refundID := refund.ExternalTransactionID()
	rollback, err := domain.NewWagerTransaction("provider", "undo-refund", "undo-key", "hash", wallet.ID(), wallet.PlayerID(), "round", "game", domain.WagerKindRollback, refund.Money(), &refundID)
	if err != nil {
		t.Fatal(err)
	}
	if err := rollback.ResolveReference(refund); err != nil {
		t.Fatal(err)
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error { return storeMovement(ctx, r, rollback, domain.LedgerDirectionDebit) }); err != nil {
		t.Fatal(err)
	}
	if err := uow.WithinTransaction(ctx, func(r ports.Repositories) error {
		found, err := r.Wagers.HasSuccessfulReversal(ctx, bet.ID())
		if err != nil {
			return err
		}
		if !found {
			return errors.New("reversal claim disappeared")
		}
		loaded, err := r.Wagers.FindByID(ctx, "provider", refund.ID())
		if err != nil {
			return err
		}
		if *loaded.ReferenceTransactionID() != bet.ID() || loaded.ResultBalance().String() != "100.00" {
			return errors.New("replay result changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if countRows(t, pool, "wallet_ledger_entries") != 4 {
		t.Fatal("incorrect reversal ledger count")
	}
	var balance int64
	if err := pool.QueryRow(ctx, `SELECT balance_cents FROM wallets WHERE id=$1`, wallet.ID()).Scan(&balance); err != nil || balance != 7500 {
		t.Fatal(balance, err)
	}
}
