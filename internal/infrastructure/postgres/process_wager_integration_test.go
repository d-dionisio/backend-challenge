//go:build integration

package postgres

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func openWagerWallet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, amount string) *domain.Wallet {
	t.Helper()
	wallet, err := application.NewOpenWallet(NewUnitOfWork(pool)).Execute(ctx, application.OpenWalletInput{
		PlayerID: uuid.New(), InitialBalance: storageMoney(t, amount)})
	if err != nil {
		t.Fatal(err)
	}
	return wallet
}

func wagerInput(t *testing.T, wallet *domain.Wallet, kind domain.WagerKind, amount, externalID string) application.ProcessWagerInput {
	t.Helper()
	return application.ProcessWagerInput{ProviderID: "provider-a", ExternalTransactionID: externalID, IdempotencyKey: "key:" + externalID,
		PlayerID: wallet.PlayerID(), WalletID: wallet.ID(), RoundID: "round", GameID: "game", Kind: kind, Money: storageMoney(t, amount)}
}

func assertWalletLedger(t *testing.T, ctx context.Context, pool *pgxpool.Pool, walletID uuid.UUID, amount int64) {
	t.Helper()
	var stored, calculated int64
	err := pool.QueryRow(ctx, `SELECT w.balance_cents,COALESCE(sum(CASE WHEN l.direction='CREDIT' THEN l.amount_cents::numeric ELSE -l.amount_cents::numeric END),0)::bigint
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id WHERE w.id=$1 GROUP BY w.id`, walletID).Scan(&stored, &calculated)
	if err != nil || stored != amount || calculated != amount {
		t.Fatal(stored, calculated, err)
	}
}

func TestProcessWagerFiftyDuplicatesAndOriginalReplay(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, pool, "100")
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1")
	type outcome struct {
		result *application.WagerResult
		err    error
	}
	results := make(chan outcome, 50)
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func() {
			<-start
			result, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
			results <- outcome{result, err}
		}()
	}
	close(start)
	var processed, replays int
	var transactionID uuid.UUID
	for i := 0; i < 50; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.result.Status != domain.WagerStatusProcessed || got.result.Balance.String() != "75.00" {
			t.Fatal(got.result)
		}
		if transactionID != uuid.Nil && transactionID != got.result.TransactionID {
			t.Fatal("duplicate transaction identity")
		}
		transactionID = got.result.TransactionID
		if got.result.IdempotentReplay {
			replays++
		} else {
			processed++
		}
	}
	if processed != 1 || replays != 49 {
		t.Fatal(processed, replays)
	}
	if countRows(t, pool, "wallet_ledger_entries") != 2 || countRows(t, pool, "outbox") != 4 {
		t.Fatal("duplicate financial effects")
	}
	useCase := application.NewProcessWager(NewUnitOfWork(pool))
	win := wagerInput(t, wallet, domain.WagerKindWin, "10", "win-1")
	if _, err := useCase.Execute(ctx, "provider-a", win); err != nil {
		t.Fatal(err)
	}
	replay, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
	if err != nil || !replay.IdempotentReplay || replay.Balance.String() != "75.00" {
		t.Fatal(replay, err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 8500)
	changed := input
	changed.Money = storageMoney(t, "26")
	if result, err := useCase.Execute(ctx, "provider-a", changed); !errors.Is(err, application.ErrIdempotencyConflict) || result != nil {
		t.Fatal(result, err)
	}
	changed = input
	changed.IdempotencyKey = "different-key"
	if _, err := useCase.Execute(ctx, "provider-a", changed); !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatal(err)
	}
	if _, err := useCase.Execute(ctx, "provider-b", input); !errors.Is(err, application.ErrProviderNotAuthorized) {
		t.Fatal(err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 8500)
}

func TestProcessTwoBetsOfEighty(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, pool, "100")
	inputs := []application.ProcessWagerInput{wagerInput(t, wallet, domain.WagerKindBet, "80", "bet-a"), wagerInput(t, wallet, domain.WagerKindBet, "80", "bet-b")}
	type outcome struct {
		result *application.WagerResult
		err    error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for _, input := range inputs {
		go func() {
			<-start
			result, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
			results <- outcome{result, err}
		}()
	}
	close(start)
	var processed, rejected int
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.result.Status == domain.WagerStatusProcessed {
			processed++
		} else if got.result.Status == domain.WagerStatusRejected && *got.result.FailureCode == application.FailureInsufficientBalance {
			rejected++
		} else {
			t.Fatal(got.result)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatal(processed, rejected)
	}
	for _, input := range inputs {
		result, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
		if err != nil || !result.IdempotentReplay {
			t.Fatal(result, err)
		}
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 2000)
	if countRows(t, pool, "wallet_ledger_entries") != 2 || countRows(t, pool, "outbox") != 5 {
		t.Fatal("incorrect financial/rejection events")
	}
}

func TestProcessWagerThreeIndependentProcessesCompeteForBalance(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, pool, "100")
	type outcome struct {
		Status      domain.WagerStatus `json:"status"`
		FailureCode *string            `json:"failureCode"`
		Error       string             `json:"error"`
	}
	results := make(chan outcome, 3)
	ready := make(chan error, 3)
	start := make(chan struct{})
	var children sync.WaitGroup
	defer func() {
		cancel()
		children.Wait()
	}()
	for i := 0; i < 3; i++ {
		i := i
		children.Add(1)
		go func() {
			defer children.Done()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessWagerThreeProcessHelper$", "-test.v")
			command.Env = append(os.Environ(),
				"THREE_PROCESS_HELPER=1",
				"THREE_PROCESS_DSN="+os.Getenv("TEST_DATABASE_URL"),
				"THREE_PROCESS_SCHEMA="+pool.Config().ConnConfig.RuntimeParams["search_path"],
				"THREE_PROCESS_PROVIDER=provider-a",
				"THREE_PROCESS_WALLET_ID="+wallet.ID().String(),
				"THREE_PROCESS_PLAYER_ID="+wallet.PlayerID().String(),
				"THREE_PROCESS_EXTERNAL_ID="+fmt.Sprintf("three-process-bet-%d", i),
				"THREE_PROCESS_AMOUNT=40",
			)
			stdin, err := command.StdinPipe()
			if err != nil {
				ready <- err
				return
			}
			defer stdin.Close()
			stdout, err := command.StdoutPipe()
			if err != nil {
				ready <- err
				return
			}
			if err := command.Start(); err != nil {
				ready <- err
				return
			}
			scanner := bufio.NewScanner(stdout)
			var output strings.Builder
			isReady := false
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Fprintln(&output, line)
				if line == "THREE_PROCESS_READY" {
					isReady = true
					break
				}
			}
			if !isReady {
				waitErr := command.Wait()
				ready <- fmt.Errorf("helper not ready: %v: %s", waitErr, output.String())
				return
			}
			ready <- nil
			select {
			case <-start:
				fmt.Fprintln(stdin, "start")
			case <-ctx.Done():
				command.Wait()
				return
			}
			var payload string
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Fprintln(&output, line)
				if strings.HasPrefix(line, "THREE_PROCESS_RESULT ") {
					payload = strings.TrimPrefix(line, "THREE_PROCESS_RESULT ")
				}
			}
			waitErr := command.Wait()
			if waitErr != nil || scanner.Err() != nil {
				results <- outcome{Error: fmt.Sprintf("helper: %v; scan: %v; output: %s", waitErr, scanner.Err(), output.String())}
				return
			}
			var result outcome
			if err := json.Unmarshal([]byte(payload), &result); err != nil {
				results <- outcome{Error: err.Error() + ": " + output.String()}
				return
			}
			results <- result
		}()
	}
	for i := 0; i < 3; i++ {
		if err := <-ready; err != nil {
			t.Fatal(err)
		}
	}
	close(start)
	var processed, rejected int
	for i := 0; i < 3; i++ {
		result := <-results
		if result.Error != "" {
			t.Fatal(result.Error)
		}
		if result.Status == domain.WagerStatusProcessed {
			processed++
		} else if result.Status == domain.WagerStatusRejected && result.FailureCode != nil && *result.FailureCode == application.FailureInsufficientBalance {
			rejected++
		} else {
			t.Fatal(result)
		}
	}
	if processed != 2 || rejected != 1 {
		t.Fatal(processed, rejected)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 2000)
	if countRows(t, pool, "wager_transactions") != 4 || countRows(t, pool, "wallet_ledger_entries") != 3 || countRows(t, pool, "outbox") != 7 {
		t.Fatal("three processes produced inconsistent financial records")
	}
}

func TestProcessWagerThreeProcessHelper(t *testing.T) {
	if os.Getenv("THREE_PROCESS_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(os.Getenv("THREE_PROCESS_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = os.Getenv("THREE_PROCESS_SCHEMA")
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	walletID, err := uuid.Parse(os.Getenv("THREE_PROCESS_WALLET_ID"))
	if err != nil {
		t.Fatal(err)
	}
	playerID, err := uuid.Parse(os.Getenv("THREE_PROCESS_PLAYER_ID"))
	if err != nil {
		t.Fatal(err)
	}
	money, err := domain.NewMoney(os.Getenv("THREE_PROCESS_AMOUNT"), "BRL")
	if err != nil {
		t.Fatal(err)
	}
	externalID := os.Getenv("THREE_PROCESS_EXTERNAL_ID")
	input := application.ProcessWagerInput{ProviderID: os.Getenv("THREE_PROCESS_PROVIDER"), ExternalTransactionID: externalID, IdempotencyKey: "key:" + externalID,
		PlayerID: playerID, WalletID: walletID, RoundID: "round", GameID: "game", Kind: domain.WagerKindBet, Money: money}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("THREE_PROCESS_READY")
	if line, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil || line != "start\n" {
		t.Fatalf("start barrier: %q, %v", line, err)
	}
	result, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, input.ProviderID, input)
	if err != nil {
		t.Fatal(err)
	}
	response, err := json.Marshal(struct {
		Status      domain.WagerStatus `json:"status"`
		FailureCode *string            `json:"failureCode"`
	}{Status: result.Status, FailureCode: result.FailureCode})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("THREE_PROCESS_RESULT " + string(response))
}

func TestProcessWagerReferencesAndLoss(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, pool, "100")
	useCase := application.NewProcessWager(NewUnitOfWork(pool))
	execute := func(input application.ProcessWagerInput) *application.WagerResult {
		t.Helper()
		result, err := useCase.Execute(ctx, "provider-a", input)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	bet := wagerInput(t, wallet, domain.WagerKindBet, "25", "bet")
	if execute(bet).Status != domain.WagerStatusProcessed {
		t.Fatal("bet failed")
	}
	refund := wagerInput(t, wallet, domain.WagerKindRefund, "25", "refund")
	refund.ReferenceExternalTransactionID = &bet.ExternalTransactionID
	if execute(refund).Balance.String() != "100.00" {
		t.Fatal("refund failed")
	}
	duplicate := wagerInput(t, wallet, domain.WagerKindRollback, "25", "duplicate")
	duplicate.ReferenceExternalTransactionID = &bet.ExternalTransactionID
	if result := execute(duplicate); result.Status != domain.WagerStatusRejected || *result.FailureCode != application.FailureAlreadyReversed {
		t.Fatal(result)
	}
	rollback := wagerInput(t, wallet, domain.WagerKindRollback, "25", "rollback")
	rollback.ReferenceExternalTransactionID = &refund.ExternalTransactionID
	if execute(rollback).Balance.String() != "75.00" {
		t.Fatal("rollback of refund failed")
	}
	versionBefore, err := NewWalletRepository(pool).FindByID(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	entries := countRows(t, pool, "wallet_ledger_entries")
	events := countRows(t, pool, "outbox")
	loss := wagerInput(t, wallet, domain.WagerKindLoss, "0", "loss")
	if result := execute(loss); result.Status != domain.WagerStatusProcessed || result.Balance.String() != "75.00" {
		t.Fatal(result)
	}
	versionAfter, err := NewWalletRepository(pool).FindByID(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	if versionBefore.Version() != versionAfter.Version() || countRows(t, pool, "wallet_ledger_entries") != entries || countRows(t, pool, "outbox") != events+1 {
		t.Fatal("LOSS changed financial state")
	}
	missing := "future-bet"
	pending := wagerInput(t, wallet, domain.WagerKindRefund, "10", "early-refund")
	pending.ReferenceExternalTransactionID = &missing
	if execute(pending).Status != domain.WagerStatusPendingReference {
		t.Fatal("missing reference did not persist pending state")
	}
	events = countRows(t, pool, "outbox")
	if result := execute(pending); !result.IdempotentReplay || result.Status != domain.WagerStatusPendingReference {
		t.Fatal(result)
	}
	if countRows(t, pool, "outbox") != events {
		t.Fatal("pending replay emitted another event")
	}
	invalid := wagerInput(t, wallet, domain.WagerKindRefund, "24", "partial-refund")
	invalid.ReferenceExternalTransactionID = &bet.ExternalTransactionID
	if result := execute(invalid); result.Status != domain.WagerStatusRejected || *result.FailureCode != application.FailureInvalidReference {
		t.Fatal(result)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
}

func TestReversalWithoutBalanceAndUnsuccessfulReference(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, pool, "0")
	useCase := application.NewProcessWager(NewUnitOfWork(pool))
	win := wagerInput(t, wallet, domain.WagerKindWin, "25", "win")
	for _, input := range []application.ProcessWagerInput{win, wagerInput(t, wallet, domain.WagerKindBet, "25", "bet")} {
		result, err := useCase.Execute(ctx, "provider-a", input)
		if err != nil || result.Status != domain.WagerStatusProcessed {
			t.Fatal(result, err)
		}
	}
	rollback := wagerInput(t, wallet, domain.WagerKindRollback, "25", "rollback")
	rollback.ReferenceExternalTransactionID = &win.ExternalTransactionID
	result, err := useCase.Execute(ctx, "provider-a", rollback)
	if err != nil || result.Status != domain.WagerStatusRejected || *result.FailureCode != application.FailureReversalInsufficientBalance {
		t.Fatal(result, err)
	}
	failedBet := wagerInput(t, wallet, domain.WagerKindBet, "25", "no-balance")
	if result, err := useCase.Execute(ctx, "provider-a", failedBet); err != nil || result.Status != domain.WagerStatusRejected {
		t.Fatal(result, err)
	}
	refund := wagerInput(t, wallet, domain.WagerKindRefund, "25", "refund-failed")
	refund.ReferenceExternalTransactionID = &failedBet.ExternalTransactionID
	result, err = useCase.Execute(ctx, "provider-a", refund)
	if err != nil || result.Status != domain.WagerStatusRejected || *result.FailureCode != application.FailureReferenceUnsuccessful {
		t.Fatal(result, err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 0)
}

func TestProcessWagerRollsBackAndCanRetry(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, pool, "100")
	_, err := pool.Exec(ctx, `CREATE FUNCTION fail_wager_event() RETURNS TRIGGER AS $$ BEGIN
		IF NEW.event_type='WalletBalanceChanged' THEN RAISE EXCEPTION 'forced wager event failure'; END IF;
		RETURN NEW; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_wager_event BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION fail_wager_event();`)
	if err != nil {
		t.Fatal(err)
	}
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "bet")
	useCase := application.NewProcessWager(NewUnitOfWork(pool))
	if result, err := useCase.Execute(ctx, "provider-a", input); err == nil || result != nil {
		t.Fatal(result, err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
	if countRows(t, pool, "wager_transactions") != 1 || countRows(t, pool, "outbox") != 2 {
		t.Fatal("failed operation was committed")
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER fail_wager_event ON outbox`); err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Execute(ctx, "provider-a", input)
	if err != nil || result.IdempotentReplay || result.Status != domain.WagerStatusProcessed {
		t.Fatal(result, err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
}

func TestSameKeyOnDifferentWallets(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	first := openWagerWallet(t, ctx, pool, "100")
	second := openWagerWallet(t, ctx, pool, "100")
	inputs := []application.ProcessWagerInput{
		wagerInput(t, first, domain.WagerKindBet, "25", "same-external"),
		wagerInput(t, second, domain.WagerKindBet, "25", "same-external"),
	}
	errorsFound := make(chan error, 2)
	start := make(chan struct{})
	for _, input := range inputs {
		go func() {
			<-start
			_, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
			errorsFound <- err
		}()
	}
	close(start)
	var successes, conflicts int
	for i := 0; i < 2; i++ {
		err := <-errorsFound
		if err == nil {
			successes++
		} else if errors.Is(err, application.ErrIdempotencyConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatal(successes, conflicts)
	}
	var total int64
	if err := pool.QueryRow(ctx, `SELECT sum(balance_cents)::bigint FROM wallets`).Scan(&total); err != nil || total != 17500 {
		t.Fatal(total, err)
	}
	if countRows(t, pool, "wager_transactions") != 3 || countRows(t, pool, "wallet_ledger_entries") != 3 {
		t.Fatal("cross-wallet key was applied twice")
	}
}

func TestProviderScopeAndMoneyOverflow(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, pool, "100")
	useCase := application.NewProcessWager(NewUnitOfWork(pool))
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "same-external")
	first, err := useCase.Execute(ctx, "provider-a", input)
	if err != nil {
		t.Fatal(err)
	}
	input.ProviderID = "provider-b"
	second, err := useCase.Execute(ctx, "provider-b", input)
	if err != nil || second.IdempotentReplay || second.TransactionID == first.TransactionID || second.Balance.String() != "50.00" {
		t.Fatal(second, err)
	}
	input.PlayerID = uuid.New()
	input.ExternalTransactionID = "bad-player"
	input.IdempotencyKey = "bad-player"
	if _, err := useCase.Execute(ctx, "provider-b", input); !errors.Is(err, application.ErrWalletIdentityMismatch) {
		t.Fatal(err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 5000)
	maximum := openWagerWallet(t, ctx, pool, "92233720368547758.07")
	win := wagerInput(t, maximum, domain.WagerKindWin, "0.01", "overflow")
	result, err := useCase.Execute(ctx, "provider-a", win)
	if err != nil || result.Status != domain.WagerStatusRejected || *result.FailureCode != application.FailureMoneyOverflow {
		t.Fatal(result, err)
	}
	assertWalletLedger(t, ctx, pool, maximum.ID(), int64(9223372036854775807))
}
