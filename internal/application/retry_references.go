package application

import (
	"context"
	"errors"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

var ErrInvalidReferenceRetryPolicy = errors.New("invalid reference retry policy")

type ReferenceRetryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

func (policy ReferenceRetryPolicy) Validate() error {
	if policy.MaxAttempts < 1 || policy.MaxAttempts > 1000 || policy.InitialDelay < time.Microsecond || policy.MaxDelay < policy.InitialDelay {
		return ErrInvalidReferenceRetryPolicy
	}
	return nil
}

func (policy ReferenceRetryPolicy) delay(attempt int) time.Duration {
	delay := policy.InitialDelay
	for i := 1; i < attempt; i++ {
		if delay > policy.MaxDelay/2 {
			return policy.MaxDelay
		}
		delay *= 2
	}
	return delay
}

type ReferenceRetryResult struct {
	TransactionID uuid.UUID
	WalletID      uuid.UUID
	ProviderID    string
	CorrelationID string
	Status        domain.WagerStatus
	Attempts      int
}

type RetryReferences struct{ unitOfWork ports.UnitOfWork }

func NewRetryReferences(unitOfWork ports.UnitOfWork) *RetryReferences {
	return &RetryReferences{unitOfWork: unitOfWork}
}

// Cada chamada trata no máximo uma pendência, mantendo a transação curta.
func (useCase *RetryReferences) Execute(ctx context.Context, policy ReferenceRetryPolicy) (*ReferenceRetryResult, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	var result *ReferenceRetryResult
	err := useCase.unitOfWork.WithinTransaction(ctx, func(r ports.Repositories) error {
		pending, err := r.Wagers.ClaimPendingReference(ctx)
		if err != nil {
			return err
		}
		if pending == nil {
			return nil
		}
		transaction := pending.Transaction
		wallet, err := r.Wallets.FindByIDForUpdate(ctx, transaction.WalletID())
		if err != nil {
			return err
		}
		attempt := pending.Attempts + 1
		input := ProcessWagerInput{CorrelationID: pending.CorrelationID, CausationID: pending.CausationID}
		if pending.Attempts >= policy.MaxAttempts {
			// Também respeita um limite reduzido após reiniciar a aplicação.
			attempt = pending.Attempts
			if err := rejectWager(ctx, r, transaction, input, FailureReferenceNotFound); err != nil {
				return err
			}
		} else {
			if err := r.Wagers.ScheduleReferenceRetry(ctx, transaction.ID(), attempt, policy.delay(attempt)); err != nil {
				return err
			}
			if err := processWager(ctx, r, wallet, transaction, input); err != nil {
				return err
			}
			if transaction.Status() == domain.WagerStatusPendingReference && attempt >= policy.MaxAttempts {
				if err := rejectWager(ctx, r, transaction, input, FailureReferenceNotFound); err != nil {
					return err
				}
			}
		}
		result = &ReferenceRetryResult{TransactionID: transaction.ID(), WalletID: transaction.WalletID(), ProviderID: transaction.ProviderID(),
			CorrelationID: pending.CorrelationID, Status: transaction.Status(), Attempts: attempt}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
