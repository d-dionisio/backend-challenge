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
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestOpenWalletPositiveBalanceIntegration(t *testing.T) {
	pool := storageTestPool(t)
	useCase := application.NewOpenWallet(NewUnitOfWork(pool))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	playerID := uuid.New()
	wallet, err := useCase.Execute(ctx, application.OpenWalletInput{
		PlayerID: playerID, InitialBalance: storageMoney(t, "100"), CorrelationID: "request-opening",
	})
	if err != nil {
		t.Fatal(err)
	}
	if wallet.PlayerID() != playerID || wallet.Balance().String() != "100.00" || wallet.Version() != 1 {
		t.Fatal("incorrect opening result")
	}
	stored, err := NewWalletRepository(pool).FindByID(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	if stored.Balance() != wallet.Balance() || stored.Version() != 1 {
		t.Fatal("wallet returned before persistence")
	}

	var kind, status, direction string
	var amount, result, before, after int64
	var internalMetadata bool
	err = pool.QueryRow(ctx, `SELECT w.kind, w.status, w.amount_cents, w.result_balance_cents,
		w.provider_id IS NULL AND w.external_transaction_id IS NULL AND w.idempotency_key IS NULL
		AND w.payload_hash IS NULL AND w.round_id IS NULL AND w.game_id IS NULL
		AND w.reference_external_transaction_id IS NULL AND w.reference_transaction_id IS NULL,
		l.direction, l.balance_before_cents, l.balance_after_cents
		FROM wager_transactions w JOIN wallet_ledger_entries l ON l.transaction_id=w.id
		WHERE w.wallet_id=$1`, wallet.ID()).Scan(&kind, &status, &amount, &result, &internalMetadata, &direction, &before, &after)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "OPENING" || status != "PROCESSED" || !internalMetadata || direction != "CREDIT" || amount != 10000 || result != 10000 || before != 0 || after != 10000 {
		t.Fatal("opening and ledger do not match initial balance")
	}
	var processed, changed int
	err = pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE event_type='WagerTransactionProcessed'),
		count(*) FILTER (WHERE event_type='WalletBalanceChanged' AND payload->'data'->>'walletVersion'='1')
		FROM outbox WHERE aggregate_id=$1 AND payload->>'correlationId'=$2 AND published_at IS NULL`, wallet.ID(), "request-opening").Scan(&processed, &changed)
	if err != nil || processed != 1 || changed != 1 {
		t.Fatal("missing opening events", processed, changed, err)
	}
	if countRows(t, pool, "wallets") != 1 || countRows(t, pool, "wager_transactions") != 1 || countRows(t, pool, "wallet_ledger_entries") != 1 || countRows(t, pool, "outbox") != 2 || countRows(t, pool, "inbox") != 0 {
		t.Fatal("unexpected opening records")
	}
}

func TestOpenWalletZeroBalanceIntegration(t *testing.T) {
	pool := storageTestPool(t)
	useCase := application.NewOpenWallet(NewUnitOfWork(pool))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wallet, err := useCase.Execute(ctx, application.OpenWalletInput{PlayerID: uuid.New(), InitialBalance: storageMoney(t, "0")})
	if err != nil {
		t.Fatal(err)
	}
	if !wallet.Balance().IsZero() || wallet.Version() != 1 {
		t.Fatal("incorrect zero opening")
	}
	if countRows(t, pool, "wallets") != 1 {
		t.Fatal("wallet was not saved")
	}
	for _, table := range []string{"wager_transactions", "wallet_ledger_entries", "outbox", "inbox"} {
		if countRows(t, pool, table) != 0 {
			t.Fatalf("zero opening wrote %s", table)
		}
	}
}

func TestOpenWalletConcurrentDuplicateIntegration(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	input := application.OpenWalletInput{PlayerID: uuid.New(), InitialBalance: storageMoney(t, "100")}
	type outcome struct {
		wallet *domain.Wallet
		err    error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			useCase := application.NewOpenWallet(NewUnitOfWork(pool))
			wallet, err := useCase.Execute(ctx, input)
			results <- outcome{wallet: wallet, err: err}
		}()
	}
	close(start)
	var successes, conflicts int
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err == nil && result.wallet != nil {
			successes++
		} else if errors.Is(result.err, ports.ErrWalletConflict) && result.wallet == nil {
			conflicts++
		} else {
			t.Fatalf("unexpected outcome: %+v", result)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatal(successes, conflicts)
	}
	if countRows(t, pool, "wallets") != 1 || countRows(t, pool, "wager_transactions") != 1 || countRows(t, pool, "wallet_ledger_entries") != 1 || countRows(t, pool, "outbox") != 2 {
		t.Fatal("duplicate opening produced extra financial records")
	}
	var distinctCorrelations int
	err := pool.QueryRow(ctx, `SELECT count(DISTINCT payload->>'correlationId') FROM outbox
		WHERE btrim(payload->>'correlationId')<>''`).Scan(&distinctCorrelations)
	if err != nil || distinctCorrelations != 1 {
		t.Fatal("generated correlation must be shared by both events", err)
	}

	// A unicidade é jogador + moeda, não somente jogador.
	usd, err := domain.NewMoney("10", "USD")
	if err != nil {
		t.Fatal(err)
	}
	input.InitialBalance = usd
	if _, err := application.NewOpenWallet(NewUnitOfWork(pool)).Execute(ctx, input); err != nil {
		t.Fatal(err)
	}
	if countRows(t, pool, "wallets") != 2 {
		t.Fatal("different currency should allow another wallet")
	}
}

func TestOpenWalletRollsBackWhenSecondEventFails(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Falha controlada somente no schema temporário do teste, usando PostgreSQL real.
	_, err := pool.Exec(ctx, `CREATE FUNCTION fail_balance_event() RETURNS TRIGGER AS $$
		BEGIN
			IF NEW.event_type='WalletBalanceChanged' THEN
				RAISE EXCEPTION 'forced outbox failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_balance_event BEFORE INSERT ON outbox
		FOR EACH ROW EXECUTE FUNCTION fail_balance_event();`)
	if err != nil {
		t.Fatal(err)
	}
	useCase := application.NewOpenWallet(NewUnitOfWork(pool))
	wallet, err := useCase.Execute(ctx, application.OpenWalletInput{PlayerID: uuid.New(), InitialBalance: storageMoney(t, "100")})
	var databaseError *pgconn.PgError
	if wallet != nil || !errors.As(err, &databaseError) || databaseError.Message != "forced outbox failure" {
		t.Fatalf("expected controlled database failure: wallet=%v err=%v", wallet, err)
	}
	for _, table := range []string{"wallets", "wager_transactions", "wallet_ledger_entries", "outbox"} {
		if countRows(t, pool, table) != 0 {
			t.Fatalf("failed opening left data in %s", table)
		}
	}
}
