package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"go.uber.org/fx"
)

type OutboxConfig struct {
	PollInterval   time.Duration
	AttemptTimeout time.Duration
	Policy         application.OutboxPolicy
}

func NewOutboxConfig() (OutboxConfig, error) {
	c := OutboxConfig{PollInterval: time.Second, AttemptTimeout: 5 * time.Second,
		Policy: application.OutboxPolicy{LeaseDuration: 30 * time.Second, InitialDelay: time.Second, MaxDelay: time.Minute}}
	for _, setting := range []struct {
		name   string
		target *time.Duration
	}{
		{"OUTBOX_POLL_INTERVAL", &c.PollInterval},
		{"OUTBOX_ATTEMPT_TIMEOUT", &c.AttemptTimeout},
		{"OUTBOX_LEASE_DURATION", &c.Policy.LeaseDuration},
		{"OUTBOX_INITIAL_DELAY", &c.Policy.InitialDelay},
		{"OUTBOX_MAX_DELAY", &c.Policy.MaxDelay},
	} {
		if value, exists := os.LookupEnv(setting.name); exists {
			duration, err := time.ParseDuration(value)
			if err != nil || duration < time.Microsecond {
				return OutboxConfig{}, fmt.Errorf("invalid %s", setting.name)
			}
			*setting.target = duration
		}
	}
	return c, c.validate()
}

func (c OutboxConfig) validate() error {
	if err := c.Policy.Validate(); err != nil {
		return err
	}
	if c.PollInterval <= 0 || c.AttemptTimeout <= 0 || c.Policy.LeaseDuration <= c.AttemptTimeout {
		return errors.New("outbox requires positive intervals and lease longer than attempt timeout")
	}
	return nil
}

func RegisterOutboxWorker(lifecycle fx.Lifecycle, config OutboxConfig, useCase *application.PublishOutbox) error {
	if err := config.validate(); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var cancel context.CancelFunc
	done := make(chan struct{})
	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			var ctx context.Context
			ctx, cancel = context.WithCancel(context.Background())
			go func() {
				defer close(done)
				logger.Info("outbox worker started")
				defer logger.Info("outbox worker stopped")
				for ctx.Err() == nil {
					attemptCtx, attemptCancel := context.WithTimeout(ctx, config.AttemptTimeout)
					event, err := useCase.Execute(attemptCtx, config.Policy)
					attemptCancel()
					if ctx.Err() != nil {
						return
					}
					if err != nil {
						if event != nil {
							logger.Error("outbox attempt failed", "eventId", event.EventID, "walletId", event.AggregateID,
								"transactionId", event.TransactionID, "providerId", event.ProviderID,
								"correlationId", event.CorrelationID, "attempts", event.Attempts)
						} else {
							logger.Error("outbox claim failed")
						}
					} else if event != nil {
						logger.Info("outbox published", "eventId", event.EventID, "walletId", event.AggregateID,
							"transactionId", event.TransactionID, "providerId", event.ProviderID,
							"correlationId", event.CorrelationID, "attempts", event.Attempts)
						continue
					}
					timer := time.NewTimer(config.PollInterval)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return nil
}
