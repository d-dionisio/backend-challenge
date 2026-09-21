package postgres

import (
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"postgres",

	fx.Provide(
		NewConfig,
		NewPool,
		fx.Annotate(NewUnitOfWork, fx.As(new(ports.UnitOfWork))),

		fx.Annotate(
			NewWalletRepository,
			fx.As(
				new(ports.WalletRepository),
			),
		),
	),
)
