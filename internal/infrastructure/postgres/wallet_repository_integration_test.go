//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Every run owns a separate schema; existing application data is never modified.
func repositoryTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("integration requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + uuid.New().String()
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		_ = admin.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Error(err)
		}
		_ = admin.Close(cleanup)
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = identifier
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	migration, err := os.ReadFile("../../../migrations/000001_initial_schema.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestWalletRepositoryIntegration(t *testing.T) {
	pool := repositoryTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r := NewWalletRepository(pool)
	money, err := domain.NewMoney("100.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	first, _ := domain.NewWallet(uuid.New(), money)
	second, _ := domain.NewWallet(uuid.New(), money)
	for _, w := range []*domain.Wallet{first, second} {
		if err := r.Create(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	duplicate, _ := domain.NewWallet(first.PlayerID(), money)
	if err := r.Create(ctx, duplicate); !errors.Is(err, ports.ErrWalletConflict) {
		t.Fatal(err)
	}
	if _, err := r.FindByID(ctx, uuid.New()); !errors.Is(err, ports.ErrWalletNotFound) {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	locked, err := r.WithTx(tx).FindByIDForUpdate(ctx, first.ID())
	if err != nil {
		t.Fatal(err)
	}
	if locked.Version() != 1 || locked.Balance() != money || locked.PlayerID() != first.PlayerID() {
		t.Fatal("incorrect rehydration")
	}

	// Another connection can lock a different wallet immediately.
	other, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Rollback(ctx)
	parallelCtx, parallelCancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = r.WithTx(other).FindByIDForUpdate(parallelCtx, second.ID())
	parallelCancel()
	if err != nil {
		t.Fatalf("independent wallet blocked: %v", err)
	}
	if err := other.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// A short server-side lock timeout proves that the first lock is still held.
	contender, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Rollback(ctx)
	if _, err := contender.Exec(ctx, "SET LOCAL lock_timeout = '150ms'"); err != nil {
		t.Fatal(err)
	}
	_, err = r.WithTx(contender).FindByIDForUpdate(ctx, first.ID())
	if err == nil {
		t.Fatal("same wallet lock was not retained")
	}
	var state interface{ SQLState() string }
	if !errors.As(err, &state) || state.SQLState() != "55P03" {
		t.Fatalf("expected lock timeout, got %v", err)
	}
	if err := contender.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	debit, _ := domain.NewMoney("80.00", "BRL")
	if err := locked.Debit(debit); err != nil {
		t.Fatal(err)
	}
	if err := r.WithTx(tx).Update(ctx, locked); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	next, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Rollback(ctx)
	current, err := r.WithTx(next).FindByIDForUpdate(ctx, first.ID())
	if err != nil {
		t.Fatal(err)
	}
	if current.Balance().String() != "20.00" || current.Version() != 2 {
		t.Fatal("committed change was lost")
	}
	if err := current.Debit(debit); !errors.Is(err, domain.ErrInsufficientBalance) {
		t.Fatal(err)
	}
	if err := next.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// Optimistic version check also prevents overwriting an already committed writer.
	if err := first.Debit(debit); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(ctx, first); !errors.Is(err, ports.ErrWalletConcurrentUpdate) {
		t.Fatal(err)
	}

	rollback, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback.Rollback(ctx)
	rolled, err := r.WithTx(rollback).FindByIDForUpdate(ctx, first.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := rolled.Credit(debit); err != nil {
		t.Fatal(err)
	}
	if err := r.WithTx(rollback).Update(ctx, rolled); err != nil {
		t.Fatal(err)
	}
	if err := rollback.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	final, err := r.FindByID(ctx, first.ID())
	if err != nil {
		t.Fatal(err)
	}
	if final.Balance().String() != "20.00" || final.Version() != 2 {
		t.Fatal("rollback persisted changes")
	}
}
