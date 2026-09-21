//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/workers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func pendingRefund(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reference string) (*domain.Wallet, application.ProcessWagerInput, *application.WagerResult) {
	t.Helper()
	wallet := openWagerWallet(t, ctx, pool, "100")
	input := wagerInput(t, wallet, domain.WagerKindRefund, "25", "refund-"+reference)
	input.ReferenceExternalTransactionID = &reference
	input.CorrelationID = "original-request"
	input.CausationID = "original-message"
	result, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
	if err != nil || result.Status != domain.WagerStatusPendingReference {
		t.Fatal(result, err)
	}
	return wallet, input, result
}

func TestReferenceRetryBackoffAndExpiration(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wallet, input, pending := pendingRefund(t, ctx, pool, "absent")
	policy := application.ReferenceRetryPolicy{MaxAttempts: 2, InitialDelay: time.Hour, MaxDelay: 2 * time.Hour}
	result, err := application.NewRetryReferences(NewUnitOfWork(pool)).Execute(ctx, policy)
	if err != nil || result.Status != domain.WagerStatusPendingReference || result.Attempts != 1 {
		t.Fatal(result, err)
	}
	var attempts int
	var delayed bool
	err = pool.QueryRow(ctx, `SELECT reference_attempts,next_reference_attempt_at>clock_timestamp()+interval '59 minutes'
		FROM wager_transactions WHERE id=$1`, pending.TransactionID).Scan(&attempts, &delayed)
	if err != nil || attempts != 1 || !delayed {
		t.Fatal(attempts, delayed, err)
	}
	// Um caso de uso novo continua respeitando o prazo persistido.
	result, err = application.NewRetryReferences(NewUnitOfWork(pool)).Execute(ctx, policy)
	if err != nil || result != nil {
		t.Fatal("backoff ignored", result, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET next_reference_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1`, pending.TransactionID); err != nil {
		t.Fatal(err)
	}
	result, err = application.NewRetryReferences(NewUnitOfWork(pool)).Execute(ctx, policy)
	if err != nil || result.Status != domain.WagerStatusRejected || result.Attempts != 2 {
		t.Fatal(result, err)
	}
	replay, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
	if err != nil || !replay.IdempotentReplay || *replay.FailureCode != application.FailureReferenceNotFound {
		t.Fatal(replay, err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
	if countRows(t, pool, "outbox") != 4 || countRows(t, pool, "wallet_ledger_entries") != 1 {
		t.Fatal("retry duplicated events or ledger")
	}
}

func TestTwoReferenceWorkersResolveOnce(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wallet, input, pending := pendingRefund(t, ctx, pool, "late-bet")
	bet := wagerInput(t, wallet, domain.WagerKindBet, "25", "late-bet")
	if _, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", bet); err != nil {
		t.Fatal(err)
	}
	policy := application.ReferenceRetryPolicy{MaxAttempts: 2, InitialDelay: time.Second, MaxDelay: time.Minute}
	type outcome struct {
		result *application.ReferenceRetryResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			result, err := application.NewRetryReferences(NewUnitOfWork(pool)).Execute(ctx, policy)
			outcomes <- outcome{result, err}
		}()
	}
	close(start)
	completed := 0
	for i := 0; i < 2; i++ {
		got := <-outcomes
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.result != nil {
			if got.result.Status != domain.WagerStatusProcessed || got.result.TransactionID != pending.TransactionID {
				t.Fatal(got.result)
			}
			completed++
		}
	}
	if completed != 1 {
		t.Fatal("multiple workers processed reference", completed)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
	replay, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
	if err != nil || replay.Status != domain.WagerStatusProcessed || !replay.IdempotentReplay {
		t.Fatal(replay, err)
	}
	var events int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE payload->'data'->>'transactionId'=$1
		AND payload->>'correlationId'='original-request' AND payload->>'causationId'='original-message'`, pending.TransactionID.String()).Scan(&events)
	if err != nil || events != 3 {
		t.Fatal("original tracing not preserved", events, err)
	}
	if countRows(t, pool, "wallet_ledger_entries") != 3 {
		t.Fatal("duplicate refund")
	}
}

func TestReferenceClaimSkipsLockedWalletAndRollbackReleases(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	first, _, _ := pendingRefund(t, ctx, pool, "first")
	second, _, _ := pendingRefund(t, ctx, pool, "second")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := NewWalletRepository(pool).WithTx(tx).FindByIDForUpdate(ctx, first.ID()); err != nil {
		t.Fatal(err)
	}
	released := errors.New("release claim")
	err = NewUnitOfWork(pool).WithinTransaction(ctx, func(r ports.Repositories) error {
		job, err := r.Wagers.ClaimPendingReference(ctx)
		if err != nil {
			return err
		}
		if job == nil || job.Transaction.WalletID() != second.ID() {
			return errors.New("locked wallet prevented independent progress")
		}
		return released
	})
	if !errors.Is(err, released) {
		t.Fatal(err)
	}
	if err := NewUnitOfWork(pool).WithinTransaction(ctx, func(r ports.Repositories) error {
		job, err := r.Wagers.ClaimPendingReference(ctx)
		if err != nil {
			return err
		}
		if job == nil || job.Transaction.WalletID() != second.ID() {
			return errors.New("claim was not released by rollback")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReferenceWorkerShutdownReleasesInFlightWork(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wallet, _, pending := pendingRefund(t, ctx, pool, "late")
	if _, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindBet, "25", "late")); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `CREATE FUNCTION slow_reference_event() RETURNS TRIGGER AS $$ BEGIN
		PERFORM pg_sleep(10); RETURN NEW; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER slow_reference_event BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION slow_reference_event();`)
	if err != nil {
		t.Fatal(err)
	}
	config := pool.Config()
	name := "reference-test-" + uuid.NewString()
	config.ConnConfig.RuntimeParams["application_name"] = name
	workerPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer workerPool.Close()
	workerConfig := workers.ReferenceConfig{PollInterval: time.Hour, AttemptTimeout: 15 * time.Second,
		Policy: application.ReferenceRetryPolicy{MaxAttempts: 2, InitialDelay: time.Second, MaxDelay: time.Minute}}
	app := fx.New(application.Module, fx.Supply(workerConfig), fx.Provide(func(lc fx.Lifecycle) ports.UnitOfWork {
		lc.Append(fx.Hook{OnStop: func(context.Context) error { workerPool.Close(); return nil }})
		return NewUnitOfWork(workerPool)
	}), fx.Invoke(workers.RegisterReferenceWorker), fx.NopLogger)
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	})
	deadline := time.Now().Add(5 * time.Second)
	inFlight := false
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep')`, name).Scan(&inFlight); err != nil {
			t.Fatal(err)
		}
		if inFlight {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !inFlight {
		t.Fatal("worker did not start pending operation")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := workerPool.Ping(ctx); err == nil {
		t.Fatal("dependency pool was not closed")
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT reference_attempts FROM wager_transactions WHERE id=$1`, pending.TransactionID).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatal("canceled attempt committed", attempts, err)
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER slow_reference_event ON outbox`); err != nil {
		t.Fatal(err)
	}
	result, err := application.NewRetryReferences(NewUnitOfWork(pool)).Execute(ctx, workerConfig.Policy)
	if err != nil || result.Status != domain.WagerStatusProcessed {
		t.Fatal(result, err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
}
