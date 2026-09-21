package ports

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrOutboxLeaseLost = errors.New("outbox reservation is no longer owned")

// O payload já foi serializado e confirmado pela operação financeira.
type PendingEvent struct {
	EventID       uuid.UUID
	AggregateID   uuid.UUID
	EventType     string
	CorrelationID string
	TransactionID string
	ProviderID    string
	Payload       []byte
	Attempts      int
	LockToken     uuid.UUID
}

type OutboxPublisherRepository interface {
	Claim(context.Context, time.Duration) (*PendingEvent, error)
	MarkPublished(context.Context, *PendingEvent) error
	Retry(context.Context, *PendingEvent, time.Duration) error
}

type EventPublisher interface {
	Publish(context.Context, *PendingEvent) error
}
