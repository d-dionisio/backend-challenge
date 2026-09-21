package workers

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/infrastructure/messaging"
	"go.uber.org/fx"
)

func RegisterWagerWorker(lifecycle fx.Lifecycle, consumer *messaging.SQSConsumer) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var cancel context.CancelFunc
	done := make(chan struct{})
	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			var ctx context.Context
			ctx, cancel = context.WithCancel(context.Background())
			go func() {
				defer close(done)
				logger.Info("SQS consumer started")
				defer logger.Info("SQS consumer stopped")
				for ctx.Err() == nil {
					message, err := consumer.Receive(ctx)
					if message != nil {
						err = consumer.Handle(ctx, message)
					}
					if ctx.Err() != nil {
						return
					}
					if err == nil {
						continue
					}
					logger.Error("SQS consumer iteration failed")
					timer := time.NewTimer(time.Second)
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
}

var WagerModule = fx.Module("wager-worker", fx.Invoke(RegisterWagerWorker))
