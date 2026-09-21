package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/auth"
	"github.com/google/uuid"
)

type API struct {
	openWallet      *application.OpenWallet
	processWager    *application.ProcessWager
	unit            ports.UnitOfWork
	auth            *auth.Authenticator
	readiness       *Readiness
	logger          *slog.Logger
	listLedger      *application.ListLedger
	reconcileWallet *application.ReconcileWallet
	metrics         *Metrics
}

func NewAPI(open *application.OpenWallet, process *application.ProcessWager, unit ports.UnitOfWork, authenticator *auth.Authenticator, readiness *Readiness, listLedger *application.ListLedger, reconcileWallet *application.ReconcileWallet, metrics *Metrics) *API {
	return &API{openWallet: open, processWager: process, unit: unit, auth: authenticator, readiness: readiness,
		listLedger: listLedger, reconcileWallet: reconcileWallet, metrics: metrics, logger: slog.New(slog.NewJSONHandler(os.Stdout, nil))}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("POST /wallets", a.auth.RequireInternal(http.HandlerFunc(a.createWallet)))
	mux.Handle("GET /wallets/{walletId}", a.auth.RequireInternal(http.HandlerFunc(a.getWallet)))
	mux.Handle("GET /wallets/{walletId}/ledger", a.auth.RequireInternal(http.HandlerFunc(a.listWalletLedger)))
	mux.Handle("POST /wallets/{walletId}/reconciliation", a.auth.RequireInternal(http.HandlerFunc(a.reconcileWalletBalance)))
	mux.Handle("GET /metrics", a.metrics)
	mux.Handle("POST /wagering/transactions", a.auth.RequireProvider(http.HandlerFunc(a.createWager)))
	mux.Handle("GET /wagering/transactions/{transactionId}", a.auth.RequireProvider(http.HandlerFunc(a.getTransaction)))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", a.auth.RequireProvider(http.HandlerFunc(a.getTransaction)))

	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /health/ready", a.ready)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlation := r.Header.Get("X-Correlation-Id")

		if strings.TrimSpace(correlation) == "" {
			correlation = uuid.NewString()
		}

		w.Header().Set("X-Correlation-Id", correlation)

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		ctx = context.WithValue(ctx, correlationKey{}, correlation)

		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

type correlationKey struct{}

func correlationID(r *http.Request) string {
	value, _ := r.Context().Value(correlationKey{}).(string)
	return value
}

func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code := errorResponse(err)

	a.logger.Error("HTTP operation failed", "correlationId", correlationID(r), "code", code, "status", status)

	writeError(w, status, code)
}

func (a *API) createWallet(w http.ResponseWriter, r *http.Request) {
	var request *openWalletRequest

	if !readJSON(w, r, &request) {
		return
	}

	if request == nil {
		writeError(w, 400, "INVALID_REQUEST")
		return
	}

	money, err := domain.NewMoney(request.InitialBalance.Amount, request.InitialBalance.Currency)

	if err != nil {
		a.fail(w, r, err)
		return
	}

	wallet, err := a.openWallet.Execute(r.Context(), application.OpenWalletInput{PlayerID: request.PlayerID, InitialBalance: money, CorrelationID: correlationID(r)})

	if err != nil {
		a.fail(w, r, err)
		return
	}

	a.logger.Info("wallet opened", "correlationId", correlationID(r), "walletId", wallet.ID())

	w.Header().Set("Location", "/wallets/"+wallet.ID().String())
	writeJSON(w, http.StatusCreated, walletBody(wallet))
}

func (a *API) getWallet(w http.ResponseWriter, r *http.Request) {

	id, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil || id == uuid.Nil {
		writeError(w, 400, "INVALID_REQUEST")
		return
	}

	var wallet *domain.Wallet

	err = a.unit.WithinTransaction(r.Context(), func(repositories ports.Repositories) error {
		var err error
		wallet, err = repositories.Wallets.FindByID(r.Context(), id)
		return err
	})

	if err != nil {
		a.fail(w, r, err)
		return
	}

	writeJSON(w, 200, walletBody(wallet))
}

func (a *API) createWager(w http.ResponseWriter, r *http.Request) {
	started := time.Now()

	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || strings.TrimSpace(keys[0]) == "" {
		writeError(w, 400, "IDEMPOTENCY_KEY_REQUIRED")
		return
	}

	var request *wagerRequest

	if !readJSON(w, r, &request) {
		return
	}

	if request == nil {
		writeError(w, 400, "INVALID_REQUEST")
		return
	}

	money, err := domain.NewMoney(request.Money.Amount, request.Money.Currency)
	if err != nil {
		a.fail(w, r, err)
		return
	}

	identity, ok := auth.IdentityFromContext(r.Context())

	if !ok {
		writeError(w, 401, "UNAUTHENTICATED")
		return
	}

	input := application.ProcessWagerInput{ProviderID: request.ProviderID, ExternalTransactionID: request.ExternalTransactionID, IdempotencyKey: keys[0],
		PlayerID: request.PlayerID, WalletID: request.WalletID, RoundID: request.RoundID, GameID: request.GameID, Kind: request.Kind, Money: money,
		ReferenceExternalTransactionID: request.ReferenceExternalTransactionID, CorrelationID: correlationID(r)}

	result, err := a.processWager.Execute(r.Context(), identity.ProviderID, input)
	if err != nil {
		a.metrics.ObserveProcessingError(err, time.Since(started))
		a.fail(w, r, err)
		return
	}
	a.metrics.ObserveWagerResult(string(result.Status), result.IdempotentReplay, time.Since(started))

	status := http.StatusOK

	switch result.Status {
	case domain.WagerStatusPending, domain.WagerStatusPendingReference:
		status = http.StatusAccepted
	case domain.WagerStatusRejected:
		status = http.StatusUnprocessableEntity
	case domain.WagerStatusFailed:
		status = http.StatusInternalServerError
	}

	a.logger.Info("HTTP wager result", "correlationId", correlationID(r), "providerId", identity.ProviderID, "walletId", input.WalletID,
		"transactionId", result.TransactionID, "status", result.Status, "replay", result.IdempotentReplay)

	writeJSON(w, status, result)
}

func (a *API) getTransaction(w http.ResponseWriter, r *http.Request) {
	identity, ok := auth.IdentityFromContext(r.Context())

	if !ok {
		writeError(w, 401, "UNAUTHENTICATED")
		return
	}

	externalID := r.PathValue("externalTransactionId")

	if provider := r.PathValue("providerId"); provider != "" && provider != identity.ProviderID {
		writeError(w, 403, "FORBIDDEN")
		return
	}

	var id uuid.UUID

	if externalID == "" {
		var err error
		id, err = uuid.Parse(r.PathValue("transactionId"))
		if err != nil || id == uuid.Nil {
			writeError(w, 400, "INVALID_REQUEST")
			return
		}
	}

	var transaction *domain.WagerTransaction

	err := a.unit.WithinTransaction(r.Context(), func(repositories ports.Repositories) error {
		var err error
		if externalID != "" {
			transaction, err = repositories.Wagers.FindByExternalID(r.Context(), identity.ProviderID, externalID)
		} else {
			transaction, err = repositories.Wagers.FindByID(r.Context(), identity.ProviderID, id)
		}
		return err
	})

	if err != nil {
		a.fail(w, r, err)
		return
	}

	writeJSON(w, 200, transactionBody(transaction))
}
