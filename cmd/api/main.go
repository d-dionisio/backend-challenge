package main

import (
	"go.uber.org/fx"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/postgres"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/workers"
)

func main() {
	app := fx.New(
		postgres.Module,
		application.Module,
		workers.Module,

		fx.Invoke(
			func(
				openWallet *application.OpenWallet,
			) {
			},
		),
	)

	app.Run()
}
