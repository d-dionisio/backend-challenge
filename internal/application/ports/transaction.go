package ports

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

var (
	ErrWagerNotFound   = errors.New("wager transaction not found")
	ErrWagerConflict   = errors.New("wager transaction conflicts with an existing operation")
	ErrWagerNotPending = errors.New("wager transaction is not pending")
	ErrInboxConflict   = errors.New("message identity reused with a different payload")
	ErrInboxIncomplete = errors.New("inbox message is not completed")
	ErrOutboxConflict  = errors.New("event identity reused with a different payload")
)

type WagerRepository interface {
	Create(context.Context, *domain.WagerTransaction) error
	FindByID(context.Context, string, uuid.UUID) (*domain.WagerTransaction, error)
	FindByExternalID(context.Context, string, string) (*domain.WagerTransaction, error)
	FindByIdempotencyKey(context.Context, string, string) (*domain.WagerTransaction, error)
	HasSuccessfulReversal(context.Context, uuid.UUID) (bool, error)
	Update(context.Context, *domain.WagerTransaction) error
	ClaimPendingReference(context.Context) (*PendingReference, error)
	ScheduleReferenceRetry(context.Context, uuid.UUID, int, time.Duration) error
}

type PendingReference struct {
	Transaction   *domain.WagerTransaction
	Attempts      int
	CorrelationID string
	CausationID   string
}

type LedgerRepository interface {
	Create(context.Context, *domain.WalletLedgerEntry) error
}

type InboxRepository interface {
	// Retorna true somente quando a mensagem equivalente já foi concluída.
	Register(ctx context.Context, consumerName, messageID, payloadHash string) (bool, error)
	Complete(ctx context.Context, consumerName, messageID string) error
}

type IntegrationEvent interface {
	json.Marshaler
	EventID() uuid.UUID
}

type OutboxRepository interface {
	Create(context.Context, IntegrationEvent) error
}

// Todos estes repositórios compartilham a mesma transação SQL.
type Repositories struct {
	Wallets WalletRepository
	Wagers  WagerRepository
	Ledger  LedgerRepository
	Inbox   InboxRepository
	Outbox  OutboxRepository
}

type UnitOfWork interface {
	WithinTransaction(context.Context, func(Repositories) error) error
}
