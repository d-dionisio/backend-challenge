package application

import (
	"context"
	"errors"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
)

type OutboxPolicy struct {
	LeaseDuration time.Duration
	InitialDelay  time.Duration
	MaxDelay      time.Duration
}

func (p OutboxPolicy) Validate() error {
	if p.LeaseDuration < time.Microsecond || p.InitialDelay < time.Microsecond || p.MaxDelay < p.InitialDelay {
		return errors.New("invalid outbox policy")
	}
	return nil
}

func (p OutboxPolicy) delay(attempt int) time.Duration {
	delay := p.InitialDelay
	for i := 1; i < attempt && delay < p.MaxDelay; i++ {
		if delay > p.MaxDelay/2 {
			return p.MaxDelay
		}
		delay *= 2
	}
	return delay
}

type PublishOutbox struct {
	repository ports.OutboxPublisherRepository
	publisher  ports.EventPublisher
}

func NewPublishOutbox(repository ports.OutboxPublisherRepository, publisher ports.EventPublisher) *PublishOutbox {
	return &PublishOutbox{repository: repository, publisher: publisher}
}

// Execute publica no máximo um evento. Nenhuma transação financeira fica
// aberta durante a chamada ao SQS.
func (u *PublishOutbox) Execute(ctx context.Context, policy OutboxPolicy) (*ports.PendingEvent, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	event, err := u.repository.Claim(ctx, policy.LeaseDuration)
	if err != nil || event == nil {
		return nil, err
	}
	if err := u.publisher.Publish(ctx, event); err != nil {
		// Com cancelamento, a reserva expira e outra instância pode retomar.
		if ctx.Err() != nil {
			return event, err
		}
		retryErr := u.repository.Retry(ctx, event, policy.delay(event.Attempts))
		return event, errors.Join(err, retryErr)
	}
	// Se o envio funcionou mas esta gravação falhar, o evento será reenviado
	// com o mesmo eventId. Não existe commit conjunto entre PostgreSQL e SQS.
	return event, u.repository.MarkPublished(ctx, event)
}
