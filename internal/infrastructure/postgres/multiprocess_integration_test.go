//go:build integration

package postgres

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

type processJob struct {
	WalletID, PlayerID uuid.UUID
	ExternalID, Amount string
}

func job(wallet *domain.Wallet, externalID, amount string) processJob {
	return processJob{wallet.ID(), wallet.PlayerID(), externalID, amount}
}

type processResult struct {
	ID      uuid.UUID
	Status  domain.WagerStatus
	Balance string
	Replay  bool
	Failure string
	Error   string
}

// Each batch is owned by a distinct OS process. No application pools or locks
// are shared. A second invocation creates three new processes (restart replay).
func runThreeProcesses(t *testing.T, pool *pgxpool.Pool, batches [3][]processJob, afterStart func(context.Context)) []processResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	var children sync.WaitGroup
	defer func() { cancel(); children.Wait() }()
	ready := make(chan error, 3)
	start := make(chan struct{})
	type outcome struct {
		results []processResult
		err     error
	}
	finished := make(chan outcome, 3)
	for _, batch := range batches {
		children.Add(1)
		go func() {
			defer children.Done()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFinancialProcessHelper$", "-test.v")
			command.Env = append(os.Environ(), "FINANCIAL_PROCESS_HELPER=1", "FINANCIAL_SCHEMA="+pool.Config().ConnConfig.RuntimeParams["search_path"])
			var stderr bytes.Buffer
			command.Stderr = &stderr
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
			var log strings.Builder
			isReady := false
			for scanner.Scan() {
				fmt.Fprintln(&log, scanner.Text())
				if scanner.Text() == "FINANCIAL_READY" {
					isReady = true
					break
				}
			}
			if !isReady {
				err := command.Wait()
				ready <- fmt.Errorf("helper startup: %v: %s %s", err, log.String(), stderr.String())
				return
			}
			ready <- nil
			select {
			case <-start:
			case <-ctx.Done():
				command.Wait()
				return
			}
			if err := json.NewEncoder(stdin).Encode(batch); err != nil {
				cancel()
				command.Wait()
				finished <- outcome{err: err}
				return
			}
			stdin.Close()
			var results []processResult
			var decodeErr error
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Fprintln(&log, line)
				if strings.HasPrefix(line, "FINANCIAL_RESULTS ") {
					decodeErr = json.Unmarshal([]byte(strings.TrimPrefix(line, "FINANCIAL_RESULTS ")), &results)
				}
			}
			if err := command.Wait(); err != nil {
				finished <- outcome{err: fmt.Errorf("helper failed: %v: %s %s", err, log.String(), stderr.String())}
				return
			}
			if scanner.Err() != nil {
				finished <- outcome{err: scanner.Err()}
				return
			}
			if decodeErr != nil || len(results) != len(batch) {
				finished <- outcome{err: fmt.Errorf("helper result: %v: %s", decodeErr, log.String())}
				return
			}
			finished <- outcome{results: results}
		}()
	}
	for range 3 {
		if err := <-ready; err != nil {
			t.Fatal(err)
		}
	}
	close(start)
	if afterStart != nil {
		afterStart(ctx)
	}
	var results []processResult
	for range 3 {
		got := <-finished
		if got.err != nil {
			t.Fatal(got.err)
		}
		for _, result := range got.results {
			if result.Error != "" {
				t.Fatal(result.Error)
			}
		}
		results = append(results, got.results...)
	}
	return results
}

func TestFinancialProcessHelper(t *testing.T) {
	if os.Getenv("FINANCIAL_PROCESS_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = os.Getenv("FINANCIAL_SCHEMA")
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("FINANCIAL_READY")
	var batch []processJob
	if err := json.NewDecoder(os.Stdin).Decode(&batch); err != nil {
		t.Fatal(err)
	}
	results := make([]processResult, len(batch))
	var work sync.WaitGroup
	for i, item := range batch {
		work.Add(1)
		go func() {
			defer work.Done()
			money, err := domain.NewMoney(item.Amount, "BRL")
			if err != nil {
				results[i].Error = err.Error()
				return
			}
			input := application.ProcessWagerInput{ProviderID: "provider-a", WalletID: item.WalletID, PlayerID: item.PlayerID,
				ExternalTransactionID: item.ExternalID, IdempotencyKey: "key:" + item.ExternalID, RoundID: "round", GameID: "game", Kind: domain.WagerKindBet, Money: money}
			result, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
			if err != nil {
				results[i].Error = err.Error()
				return
			}
			results[i] = processResult{ID: result.TransactionID, Status: result.Status, Replay: result.IdempotentReplay}
			if result.Balance != nil {
				results[i].Balance = result.Balance.String()
			}
			if result.FailureCode != nil {
				results[i].Failure = *result.FailureCode
			}
		}()
	}
	work.Wait()
	payload, err := json.Marshal(results)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("FINANCIAL_RESULTS " + string(payload))
}

func TestThreeProcessesTwoBetsOfEightyAndRestartReplay(t *testing.T) {
	pool := storageTestPool(t)
	ctx := context.Background()
	wallet := openWagerWallet(t, ctx, pool, "100")
	independent := openWagerWallet(t, ctx, pool, "100")
	batches := [3][]processJob{{job(wallet, "a", "80")}, {job(wallet, "b", "80")}, {job(independent, "c", "25")}}
	first := runThreeProcesses(t, pool, batches, nil)
	processed, rejected := 0, 0
	byID := map[uuid.UUID]processResult{}
	for _, result := range first {
		if result.Replay {
			t.Fatal("unexpected initial replay")
		}
		byID[result.ID] = result
		switch {
		case result.Status == domain.WagerStatusProcessed && result.Balance == "20.00":
			processed++
		case result.Status == domain.WagerStatusRejected && result.Failure == application.FailureInsufficientBalance:
			rejected++
		case result.Status == domain.WagerStatusProcessed && result.Balance == "75.00":
		default:
			t.Fatal(result)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatal(processed, rejected)
	}
	for _, result := range runThreeProcesses(t, pool, batches, nil) {
		original := byID[result.ID]
		if !result.Replay || result.Status != original.Status || result.Balance != original.Balance || result.Failure != original.Failure {
			t.Fatal("restart changed result", result)
		}
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 2000)
	assertWalletLedger(t, ctx, pool, independent.ID(), 7500)
	var debits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND direction='DEBIT'`, wallet.ID()).Scan(&debits); err != nil || debits != 1 {
		t.Fatal(debits, err)
	}
	if countRows(t, pool, "wager_transactions") != 5 || countRows(t, pool, "outbox") != 9 {
		t.Fatal("replays produced extra effects")
	}
}

func TestThreeProcessesFiftyDuplicatesAndRestartReplay(t *testing.T) {
	pool := storageTestPool(t)
	ctx := context.Background()
	wallet := openWagerWallet(t, ctx, pool, "100")
	var batches [3][]processJob
	for i := 0; i < 50; i++ {
		batches[i%3] = append(batches[i%3], job(wallet, "duplicate", "25"))
	}
	var transactionID uuid.UUID
	for round := 0; round < 2; round++ {
		replays := 0
		for _, result := range runThreeProcesses(t, pool, batches, nil) {
			if result.Status != domain.WagerStatusProcessed || result.Balance != "75.00" {
				t.Fatal(result)
			}
			if transactionID != uuid.Nil && transactionID != result.ID {
				t.Fatal("duplicate transaction")
			}
			transactionID = result.ID
			if result.Replay {
				replays++
			}
		}
		if replays != 49+round {
			t.Fatal("incorrect replay count", replays)
		}
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
	if countRows(t, pool, "wallet_ledger_entries") != 2 || countRows(t, pool, "outbox") != 4 {
		t.Fatal("duplicate financial effects")
	}
}

func TestThreeProcessesIndependentWalletsProgressWhileOneLocked(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wallets := []*domain.Wallet{openWagerWallet(t, ctx, pool, "100"), openWagerWallet(t, ctx, pool, "100"), openWagerWallet(t, ctx, pool, "100")}
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(ctx, `SELECT id FROM wallets WHERE id=$1 FOR UPDATE`, wallets[0].ID()); err != nil {
		t.Fatal(err)
	}
	var batches [3][]processJob
	for i, wallet := range wallets {
		batches[i] = []processJob{job(wallet, fmt.Sprintf("wallet-%d", i), "25")}
	}
	runThreeProcesses(t, pool, batches, func(runCtx context.Context) {
		deadline, cancel := context.WithTimeout(runCtx, 10*time.Second)
		defer cancel()
		for {
			var completed int
			if err := pool.QueryRow(deadline, `SELECT count(*) FROM wallets WHERE id=ANY($1) AND balance_cents=7500`, []uuid.UUID{wallets[1].ID(), wallets[2].ID()}).Scan(&completed); err != nil {
				t.Fatal("independent wallets stalled", err)
			}
			if completed == 2 {
				break
			}
			select {
			case <-deadline.Done():
				t.Fatal("independent wallets stalled behind locked wallet")
			case <-time.After(10 * time.Millisecond):
			}
		}
		assertWalletLedger(t, runCtx, pool, wallets[0].ID(), 10000)
		if err := lock.Rollback(runCtx); err != nil {
			t.Fatal(err)
		}
	})
	for _, wallet := range wallets {
		assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
	}
}
