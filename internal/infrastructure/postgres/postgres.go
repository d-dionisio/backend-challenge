package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func NewPool(lifecycle fx.Lifecycle, config Config) (*pgxpool.Pool, error) {

	poolConfig, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	poolConfig.MaxConns = 20
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(
		context.Background(),
		poolConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("cate postgres pool: %w", err)
	}

	lifecycle.Append(
		fx.Hook{
			OnStart: func(ctx context.Context) error {
				if err := pool.Ping(ctx); err != nil {
					return fmt.Errorf("ping postgres: %w", err)
				}

				return nil

			},

			OnStop: func(ctx context.Context) error {
				pool.Close()
				return nil
			},
		},
	)

	return pool, nil
}
