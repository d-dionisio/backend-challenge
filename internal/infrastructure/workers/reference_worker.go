package workers

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"go.uber.org/fx"
)

type ReferenceConfig struct {
	PollInterval   time.Duration
	AttemptTimeout time.Duration
	Policy         application.ReferenceRetryPolicy
}

func NewReferenceConfig() (ReferenceConfig, error) {
	config := ReferenceConfig{PollInterval: time.Second, AttemptTimeout: 5 * time.Second,
		Policy: application.ReferenceRetryPolicy{MaxAttempts: 10, InitialDelay: time.Second, MaxDelay: time.Minute}}
	for _, setting := range []struct {
		name   string
		target *time.Duration
	}{
		{"REFERENCE_POLL_INTERVAL", &config.PollInterval},
		{"REFERENCE_ATTEMPT_TIMEOUT", &config.AttemptTimeout},
		{"REFERENCE_INITIAL_DELAY", &config.Policy.InitialDelay},
		{"REFERENCE_MAX_DELAY", &config.Policy.MaxDelay},
	} {
		if value, exists := os.LookupEnv(setting.name); exists {
			duration, err := time.ParseDuration(value)
			if err != nil || duration < time.Microsecond {
				return ReferenceConfig{}, fmt.Errorf("invalid %s", setting.name)
			}
			*setting.target = duration
		}
	}
	if value, exists := os.LookupEnv("REFERENCE_MAX_ATTEMPTS"); exists {
		attempts, err := strconv.Atoi(value)
		if err != nil {
			return ReferenceConfig{}, fmt.Errorf("invalid REFERENCE_MAX_ATTEMPTS: %w", err)
		}
		config.Policy.MaxAttempts = attempts
	}
	if err := config.Policy.Validate(); err != nil {
		return ReferenceConfig{}, err
	}
	return config, nil
}

func RegisterReferenceWorker(lifecycle fx.Lifecycle, config ReferenceConfig, useCase *application.RetryReferences) error {
	if err := config.Policy.Validate(); err != nil {
		return err
	}
	if config.PollInterval <= 0 || config.AttemptTimeout <= 0 {
		return fmt.Errorf("reference worker intervals must be positive")
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
				logger.Info("reference worker started")
				defer logger.Info("reference worker stopped")
				for ctx.Err() == nil {
					attemptCtx, attemptCancel := context.WithTimeout(ctx, config.AttemptTimeout)
					result, err := useCase.Execute(attemptCtx, config.Policy)
					attemptCancel()
					if ctx.Err() != nil {
						return
					}
					if err != nil {
						// Não registra erro SQL bruto, que pode conter dados do payload.
						logger.Error("reference worker attempt failed", "retryAfter", config.PollInterval.String())
					} else if result != nil {
						logger.Info("reference attempt committed", "transactionId", result.TransactionID, "walletId", result.WalletID,
							"providerId", result.ProviderID, "correlationId", result.CorrelationID, "status", result.Status, "attempts", result.Attempts)
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

var Module = fx.Module("workers", fx.Provide(NewReferenceConfig), fx.Invoke(RegisterReferenceWorker))
