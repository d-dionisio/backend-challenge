package application

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

var (
	ErrProviderNotAuthorized  = errors.New("provider is not authorized")
	ErrIdempotencyConflict    = errors.New("idempotency key or external identity conflicts with stored operation")
	ErrWalletIdentityMismatch = errors.New("player or currency does not match wallet")
)

const (
	FailureInsufficientBalance         = "INSUFFICIENT_BALANCE"
	FailureReversalInsufficientBalance = "REVERSAL_INSUFFICIENT_BALANCE"
	FailureInvalidReference            = "INVALID_REFERENCE"
	FailureReferenceUnsuccessful       = "REFERENCE_UNSUCCESSFUL"
	FailureAlreadyReversed             = "ALREADY_REVERSED"
	FailureMoneyOverflow               = "MONEY_OVERFLOW"
	FailureReferenceNotFound           = "REFERENCE_NOT_FOUND"
)

type WagerResult struct {
	TransactionID    uuid.UUID          `json:"transactionId"`
	Status           domain.WagerStatus `json:"status"`
	Balance          *domain.Money      `json:"balance,omitempty"`
	FailureCode      *string            `json:"failureCode,omitempty"`
	IdempotentReplay bool               `json:"idempotentReplay"`
}

type ProcessWager struct{ unitOfWork ports.UnitOfWork }

func NewProcessWager(unitOfWork ports.UnitOfWork) *ProcessWager {
	return &ProcessWager{unitOfWork: unitOfWork}
}

// authorizedProviderID deve vir da identidade verificada pelo adaptador de entrada.
func (useCase *ProcessWager) Execute(ctx context.Context, authorizedProviderID string, input ProcessWagerInput) (*WagerResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(authorizedProviderID) == "" || authorizedProviderID != input.ProviderID {
		return nil, ErrProviderNotAuthorized
	}
	if !utf8.ValidString(input.ProviderID) || utf8.RuneCountInString(input.ProviderID) > 100 {
		return nil, domain.ErrInvalidWagerTransaction
	}
	for _, value := range []string{input.ExternalTransactionID, input.IdempotencyKey, input.RoundID, input.GameID} {
		if !utf8.ValidString(value) || utf8.RuneCountInString(value) > 255 {
			return nil, domain.ErrInvalidWagerTransaction
		}
	}
	if input.ReferenceExternalTransactionID != nil {
		reference := *input.ReferenceExternalTransactionID
		if !utf8.ValidString(reference) || utf8.RuneCountInString(reference) > 255 {
			return nil, domain.ErrInvalidWagerReference
		}
		input.ReferenceExternalTransactionID = &reference
	}
	hash, err := input.payloadHash()
	if err != nil {
		return nil, err
	}
	transaction, err := domain.NewWagerTransaction(input.ProviderID, input.ExternalTransactionID, input.IdempotencyKey, hash,
		input.WalletID, input.PlayerID, input.RoundID, input.GameID, input.Kind, input.Money, input.ReferenceExternalTransactionID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.CorrelationID) == "" {
		input.CorrelationID = uuid.NewString()
	}
	var result *WagerResult
	err = useCase.unitOfWork.WithinTransaction(ctx, func(r ports.Repositories) error {
		replay, err := findWagerReplay(ctx, r.Wagers, transaction)
		if err != nil {
			return err
		}
		if replay != nil {
			result = replay
			return nil
		}

		wallet, err := r.Wallets.FindByIDForUpdate(ctx, input.WalletID)
		if err != nil {
			return err
		}
		// A outra instância pode ter confirmado enquanto esperávamos pelo lock.
		replay, err = findWagerReplay(ctx, r.Wagers, transaction)
		if err != nil {
			return err
		}
		if replay != nil {
			result = replay
			return nil
		}
		if wallet.PlayerID() != input.PlayerID || wallet.Balance().Currency() != input.Money.Currency() {
			return ErrWalletIdentityMismatch
		}
		if err := r.Wagers.Create(ctx, transaction); err != nil {
			if !errors.Is(err, ports.ErrWagerConflict) {
				return err
			}
			// Também cobre a disputa pela mesma chave em carteiras diferentes.
			replay, err := findWagerReplay(ctx, r.Wagers, transaction)
			if err != nil {
				return err
			}
			if replay == nil {
				return ErrIdempotencyConflict
			}
			result = replay
			return nil
		}
		if err := processWager(ctx, r, wallet, transaction, input); err != nil {
			return err
		}
		result = wagerResult(transaction, false)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func findWagerReplay(ctx context.Context, repository ports.WagerRepository, requested *domain.WagerTransaction) (*WagerResult, error) {
	existing, err := repository.FindByIdempotencyKey(ctx, requested.ProviderID(), requested.IdempotencyKey())
	if errors.Is(err, ports.ErrWagerNotFound) {
		existing, err = repository.FindByExternalID(ctx, requested.ProviderID(), requested.ExternalTransactionID())
	}
	if errors.Is(err, ports.ErrWagerNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if existing.PayloadHash() != requested.PayloadHash() || existing.IdempotencyKey() != requested.IdempotencyKey() {
		return nil, ErrIdempotencyConflict
	}
	return wagerResult(existing, true), nil
}

func wagerResult(transaction *domain.WagerTransaction, replay bool) *WagerResult {
	return &WagerResult{TransactionID: transaction.ID(), Status: transaction.Status(), Balance: transaction.ResultBalance(),
		FailureCode: transaction.FailureCode(), IdempotentReplay: replay}
}

func processWager(ctx context.Context, r ports.Repositories, wallet *domain.Wallet, transaction *domain.WagerTransaction, input ProcessWagerInput) error {
	var reference *domain.WagerTransaction
	needsReference := transaction.Kind() == domain.WagerKindRefund || transaction.Kind() == domain.WagerKindRollback ||
		(transaction.Kind() == domain.WagerKindWin && transaction.ReferenceExternalTransactionID() != nil)
	if needsReference {
		var err error
		reference, err = r.Wagers.FindByExternalID(ctx, transaction.ProviderID(), *transaction.ReferenceExternalTransactionID())
		if errors.Is(err, ports.ErrWagerNotFound) {
			return waitForReference(ctx, r, transaction, input)
		}
		if err != nil {
			return err
		}
		err = transaction.ResolveReference(reference)
		if errors.Is(err, domain.ErrReferenceNotProcessed) {
			if !reference.IsTerminal() {
				return waitForReference(ctx, r, transaction, input)
			}
			return rejectWager(ctx, r, transaction, input, FailureReferenceUnsuccessful)
		}
		if errors.Is(err, domain.ErrInvalidWagerReference) {
			return rejectWager(ctx, r, transaction, input, FailureInvalidReference)
		}
		if err != nil {
			return err
		}
		if transaction.Kind() == domain.WagerKindRefund || transaction.Kind() == domain.WagerKindRollback {
			reversed, err := r.Wagers.HasSuccessfulReversal(ctx, reference.ID())
			if err != nil {
				return err
			}
			if reversed {
				return rejectWager(ctx, r, transaction, input, FailureAlreadyReversed)
			}
		}
	}

	direction := domain.LedgerDirectionCredit
	if transaction.Kind() == domain.WagerKindBet {
		direction = domain.LedgerDirectionDebit
	}
	if transaction.Kind() == domain.WagerKindRollback && reference.Kind() != domain.WagerKindBet {
		direction = domain.LedgerDirectionDebit
	}
	before := wallet.Balance()
	if transaction.Kind() != domain.WagerKindLoss {
		var err error
		if direction == domain.LedgerDirectionDebit {
			err = wallet.Debit(transaction.Money())
		} else {
			err = wallet.Credit(transaction.Money())
		}
		if errors.Is(err, domain.ErrInsufficientBalance) {
			code := FailureInsufficientBalance
			if transaction.Kind() == domain.WagerKindRollback {
				code = FailureReversalInsufficientBalance
			}
			return rejectWager(ctx, r, transaction, input, code)
		}
		if errors.Is(err, domain.ErrMoneyOverflow) {
			return rejectWager(ctx, r, transaction, input, FailureMoneyOverflow)
		}
		if err != nil {
			return err
		}
		if err := r.Wallets.Update(ctx, wallet); err != nil {
			return err
		}
	}
	if err := transaction.MarkProcessed(wallet.Balance()); err != nil {
		return err
	}
	if err := r.Wagers.Update(ctx, transaction); err != nil {
		return err
	}
	processed, err := domain.NewWagerTransactionProcessed(transaction, input.CorrelationID, input.CausationID)
	if err != nil {
		return err
	}
	if err := r.Outbox.Create(ctx, processed); err != nil {
		return err
	}
	if transaction.Kind() == domain.WagerKindLoss {
		return nil
	}
	entry, err := domain.NewWalletLedgerEntry(wallet.ID(), transaction.ID(), direction, transaction.Money(), before, wallet.Balance())
	if err != nil {
		return err
	}
	if err := r.Ledger.Create(ctx, entry); err != nil {
		return err
	}
	changed, err := domain.NewWalletBalanceChanged(transaction, entry, wallet.Version(), input.CorrelationID, input.CausationID)
	if err != nil {
		return err
	}
	return r.Outbox.Create(ctx, changed)
}

func rejectWager(ctx context.Context, r ports.Repositories, transaction *domain.WagerTransaction, input ProcessWagerInput, code string) error {
	if err := transaction.Reject(code); err != nil {
		return err
	}
	if err := r.Wagers.Update(ctx, transaction); err != nil {
		return err
	}
	event, err := domain.NewWagerTransactionRejected(transaction, input.CorrelationID, input.CausationID)
	if err != nil {
		return err
	}
	return r.Outbox.Create(ctx, event)
}

func waitForReference(ctx context.Context, r ports.Repositories, transaction *domain.WagerTransaction, input ProcessWagerInput) error {
	// A retomada não repete a transição nem o evento de entrada em espera.
	if transaction.Status() == domain.WagerStatusPendingReference {
		return nil
	}
	if err := transaction.MarkPendingReference(); err != nil {
		return err
	}
	if err := r.Wagers.Update(ctx, transaction); err != nil {
		return err
	}
	event, err := domain.NewWagerTransactionPendingReference(transaction, input.CorrelationID, input.CausationID)
	if err != nil {
		return err
	}
	return r.Outbox.Create(ctx, event)
}
