package main

import (
	"go.uber.org/fx"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/postgres"
)

func main() {
	app := fx.New(
		postgres.Module,

		fx.Invoke(
			func(
				repository ports.WalletRepository,
			) {
			},
		),
	)

	app.Run()
}
