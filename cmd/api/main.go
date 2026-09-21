package main

import (
	"go.uber.org/fx"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/auth"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/messaging"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/postgres"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/workers"
)

func main() {
	app := fx.New(
		auth.Module,
		postgres.Module,
		application.Module,
		messaging.Module,
		messaging.ConsumerModule,
		workers.Module,
		workers.WagerModule,

		fx.Invoke(
			func(
				openWallet *application.OpenWallet,
			) {
			},
		),
	)

	app.Run()
}
