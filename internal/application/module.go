package application

import "go.uber.org/fx"

var Module = fx.Module(
	"application",
	fx.Provide(NewOpenWallet, NewProcessWager, NewRetryReferences, NewPublishOutbox, NewListLedger, NewReconcileWallet),
)
