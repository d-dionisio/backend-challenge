package main

import (
	"go.uber.org/fx"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/postgres"
)

func main() {
	app := fx.New(
		postgres.Module,
		application.Module,

		fx.Invoke(
			func(
				openWallet *application.OpenWallet,
			) {
			},
		),
	)

	app.Run()
}
