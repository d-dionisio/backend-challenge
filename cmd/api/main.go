package main

import (
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func main() {
	app := fx.New(
		postgres.Module,

		fx.Invoke(
			func(pool *pgxpool.Pool) {},
		),
	)

	app.Run()
}
