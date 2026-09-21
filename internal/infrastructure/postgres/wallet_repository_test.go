package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/google/uuid"
)

func TestWalletLockRequiresTransaction(t *testing.T) {
	r := NewWalletRepository(nil)
	if _, err := r.FindByIDForUpdate(context.Background(), uuid.New()); !errors.Is(err, ports.ErrTransactionRequired) {
		t.Fatalf("expected explicit transaction requirement, got %v", err)
	}
}
