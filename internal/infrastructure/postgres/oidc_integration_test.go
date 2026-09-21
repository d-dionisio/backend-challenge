//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/auth"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

func oidcTestConfig(t *testing.T) auth.Config {
	t.Helper()
	issuer := os.Getenv("TEST_OIDC_ISSUER_URL")
	if issuer == "" {
		t.Fatal("integration requires TEST_OIDC_ISSUER_URL")
	}
	return auth.Config{IssuerURL: issuer, Audience: "wagering-api", ClientID: "wagering-api", ClientSecret: "local-api-secret"}
}

func realAuthenticator(t *testing.T) *auth.Authenticator {
	t.Helper()
	var authenticator *auth.Authenticator
	app := fx.New(auth.Module, fx.Replace(oidcTestConfig(t)), fx.Populate(&authenticator), fx.NopLogger)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return authenticator
}

func keycloakToken(t *testing.T, clientID string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {"local-" + clientID + "-secret"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, oidcTestConfig(t).IssuerURL+"/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("could not contact token endpoint")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("token endpoint status %d for %s", response.StatusCode, clientID)
	}
	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil || result.AccessToken == "" {
		t.Fatal("missing access token")
	}
	return result.AccessToken
}

func authenticatedRequest(t *testing.T, method, address, token string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, address, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	// Um header controlado pelo chamador não pode trocar sua identidade.
	request.Header.Set("X-Provider-Id", "provider-b")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("protected test endpoint failed")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode == http.StatusUnauthorized && response.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("missing authentication challenge")
	}
	return response.StatusCode, string(body)
}

func TestOIDCRealTokensAndPermissions(t *testing.T) {
	a := realAuthenticator(t)
	mux := http.NewServeMux()
	mux.Handle("/provider", a.RequireProvider(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, ok := auth.IdentityFromContext(r.Context())
		if !ok {
			t.Error("missing verified identity")
			w.WriteHeader(500)
			return
		}
		_, _ = io.WriteString(w, identity.ProviderID)
	})))
	mux.Handle("/internal", a.RequireInternal(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })))
	server := httptest.NewServer(mux)
	defer server.Close()
	providerA := keycloakToken(t, "provider-a")
	providerB := keycloakToken(t, "provider-b")
	internal := keycloakToken(t, "wallet-service")
	wrongAudience := keycloakToken(t, "wrong-audience")
	parts := strings.Split(providerA, ".")
	if len(parts) != 3 {
		t.Fatal("expected Keycloak signed JWT")
	}
	if strings.HasPrefix(parts[2], "A") {
		parts[2] = "B" + parts[2][1:]
	} else {
		parts[2] = "A" + parts[2][1:]
	}
	tampered := strings.Join(parts, ".")
	for _, test := range []struct {
		name, path, token string
		want              int
		body              string
	}{
		{"provider-a", "/provider", providerA, 200, "provider-a"},
		{"provider-b", "/provider", providerB, 200, "provider-b"},
		{"internal", "/internal", internal, 204, ""},
		{"missing", "/provider", "", 401, ""},
		{"invalid", "/provider", "invalid-token", 401, ""},
		{"tampered", "/provider", tampered, 401, ""},
		{"wrong-audience", "/provider", wrongAudience, 401, ""},
		{"provider-cannot-open-wallet", "/internal", providerA, 403, ""},
		{"internal-cannot-act-as-provider", "/provider", internal, 403, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			status, body := authenticatedRequest(t, http.MethodGet, server.URL+test.path, test.token)
			if status != test.want {
				t.Fatal(status, test.want)
			}
			if test.body != "" && body != test.body {
				t.Fatal("identity was overwritten")
			}
		})
	}
	t.Run("expired-real-token", func(t *testing.T) {
		token := keycloakToken(t, "provider-expiring")
		time.Sleep(3 * time.Second)
		status, _ := authenticatedRequest(t, http.MethodGet, server.URL+"/provider", token)
		if status != http.StatusUnauthorized {
			t.Fatal("expired token accepted", status)
		}
	})
}

func TestOIDCAuthorizationPreventsFinancialEffectsAndCrossProviderReads(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := realAuthenticator(t)
	wallet := openWagerWallet(t, ctx, pool, "100")
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1")
	useCase := application.NewProcessWager(NewUnitOfWork(pool))
	// Handlers exclusivos do teste exercitam a fronteira de autorização
	// com os casos de uso reais. Os endpoints da API serão feitos na etapa 10.
	mux := http.NewServeMux()
	mux.Handle("/wager", a.RequireProvider(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, _ := auth.IdentityFromContext(r.Context())
		result, err := useCase.Execute(r.Context(), identity.ProviderID, input)
		if errors.Is(err, application.ErrProviderNotAuthorized) {
			w.WriteHeader(403)
			return
		}
		if err != nil {
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	})))
	mux.Handle("/transaction", a.RequireProvider(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, _ := auth.IdentityFromContext(r.Context())
		var transaction *domain.WagerTransaction
		err := NewUnitOfWork(pool).WithinTransaction(r.Context(), func(repositories ports.Repositories) error {
			var err error
			transaction, err = repositories.Wagers.FindByExternalID(r.Context(), identity.ProviderID, input.ExternalTransactionID)
			return err
		})
		if errors.Is(err, ports.ErrWagerNotFound) {
			w.WriteHeader(404)
			return
		}
		if err != nil {
			w.WriteHeader(500)
			return
		}
		_, _ = io.WriteString(w, transaction.ID().String())
	})))
	mux.Handle("/wallets", a.RequireInternal(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		zero, err := domain.ZeroMoney("BRL")
		if err != nil {
			w.WriteHeader(500)
			return
		}
		_, err = application.NewOpenWallet(NewUnitOfWork(pool)).Execute(r.Context(), application.OpenWalletInput{PlayerID: uuid.New(), InitialBalance: zero})
		if err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})))
	server := httptest.NewServer(mux)
	defer server.Close()
	providerA := keycloakToken(t, "provider-a")
	providerB := keycloakToken(t, "provider-b")
	internal := keycloakToken(t, "wallet-service")
	for _, test := range []struct {
		path, token string
		want        int
	}{
		{"/wager", "", 401}, {"/wager", "invalid", 401}, {"/wager", providerB, 403},
		{"/wallets", providerA, 403}, {"/wallets", providerB, 403}, {"/wager", internal, 403},
	} {
		status, _ := authenticatedRequest(t, http.MethodPost, server.URL+test.path, test.token)
		if status != test.want {
			t.Fatal(status, test.want)
		}
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
	if countRows(t, pool, "wallets") != 1 || countRows(t, pool, "wager_transactions") != 1 || countRows(t, pool, "outbox") != 2 {
		t.Fatal("denied requests caused financial effects")
	}
	if status, _ := authenticatedRequest(t, http.MethodPost, server.URL+"/wager", providerA); status != 200 {
		t.Fatal(status)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
	if status, body := authenticatedRequest(t, http.MethodPost, server.URL+"/wager", providerB); status != 403 || body != "" {
		t.Fatal("cross-provider replay exposed data", status)
	}
	if status, body := authenticatedRequest(t, http.MethodGet, server.URL+"/transaction", providerB); status != 404 || body != "" {
		t.Fatal("cross-provider read exposed data", status)
	}
	if status, body := authenticatedRequest(t, http.MethodGet, server.URL+"/transaction", providerA); status != 200 || body == "" {
		t.Fatal("owner cannot read transaction", status)
	}
	if _, err := useCase.Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindWin, "10", "win-1")); err != nil {
		t.Fatal(err)
	}
	status, body := authenticatedRequest(t, http.MethodPost, server.URL+"/wager", providerA)
	var replay struct {
		IdempotentReplay bool `json:"idempotentReplay"`
		Balance          struct {
			Amount string `json:"amount"`
		} `json:"balance"`
	}
	if status != 200 || json.Unmarshal([]byte(body), &replay) != nil || !replay.IdempotentReplay || replay.Balance.Amount != "75.00" {
		t.Fatal("replay did not preserve original result")
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 8500)
	if status, _ := authenticatedRequest(t, http.MethodPost, server.URL+"/wallets", internal); status != 201 {
		t.Fatal("internal wallet operation denied", status)
	}
}

func TestOIDCStartupRejectsInvalidClientCredentials(t *testing.T) {
	settings := oidcTestConfig(t)
	settings.ClientSecret = "wrong-secret"
	app := fx.New(auth.Module, fx.Replace(settings), fx.NopLogger)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Start(ctx); !errors.Is(err, auth.ErrIdentityProviderUnavailable) {
		t.Fatal("invalid API credentials accepted", err)
	}
}
