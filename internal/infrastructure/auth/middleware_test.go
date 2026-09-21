package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/fx"
)

func authTestServer(t *testing.T, change func(map[string]any), status int) *Authenticator {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": server.URL, "introspection_endpoint": server.URL + "/introspect"})
			return
		}
		user, password, ok := r.BasicAuth()
		if !ok || user != "api" || password != "test-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Form.Get("token") == "startup-check" {
			_ = json.NewEncoder(w).Encode(map[string]bool{"active": false})
			return
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		claims := map[string]any{"active": true, "iss": server.URL, "aud": []string{"api"}, "sub": "service-account",
			"exp": time.Now().Add(time.Minute).Unix(), "token_type": "Bearer", "providerId": "provider-a",
			"resource_access": map[string]any{"api": map[string]any{"roles": []string{"provider"}}}}
		if change != nil {
			change(claims)
		}
		_ = json.NewEncoder(w).Encode(claims)
	}))
	t.Cleanup(server.Close)
	var authenticator *Authenticator
	app := fx.New(fx.Supply(Config{IssuerURL: server.URL, Audience: "api", ClientID: "api", ClientSecret: "test-secret"}),
		fx.Provide(NewAuthenticator), fx.Populate(&authenticator), fx.NopLogger)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func TestAuthenticationAndAuthorizationBoundaries(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(map[string]any)
		want   int
	}{
		{"valid", nil, 200},
		{"inactive", func(c map[string]any) { c["active"] = false }, 401},
		{"wrong-issuer", func(c map[string]any) { c["iss"] = "https://other.example" }, 401},
		{"wrong-audience", func(c map[string]any) { c["aud"] = "other-api" }, 401},
		{"expired", func(c map[string]any) { c["exp"] = time.Now().Add(-time.Second).Unix() }, 401},
		{"not-yet-valid", func(c map[string]any) { c["nbf"] = time.Now().Add(time.Minute).Unix() }, 401},
		{"refresh-token", func(c map[string]any) { c["token_type"] = "Refresh" }, 401},
		{"missing-subject", func(c map[string]any) { delete(c, "sub") }, 401},
		{"missing-provider", func(c map[string]any) { delete(c, "providerId") }, 403},
		{"missing-role", func(c map[string]any) { delete(c, "resource_access") }, 403},
		{"ambiguous-roles", func(c map[string]any) {
			c["resource_access"] = map[string]any{"api": map[string]any{"roles": []string{"provider", "internal"}}}
		}, 403},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := authTestServer(t, test.change, http.StatusOK)
			called := false
			handler := a.RequireProvider(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				identity, ok := IdentityFromContext(r.Context())
				if !ok || identity.ProviderID != "provider-a" {
					t.Error("identity missing or overwritten")
				}
				w.WriteHeader(http.StatusOK)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("Authorization", "Bearer test-token")
			request.Header.Set("X-Provider-Id", "provider-b")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want || called != (test.want == 200) {
				t.Fatal(response.Code, called)
			}
		})
	}
}

func TestMissingCredentialsAndIdPFailureDoNotReachHandler(t *testing.T) {
	a := authTestServer(t, nil, http.StatusServiceUnavailable)
	handler := a.RequireInternal(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unauthorized request reached handler") }))
	for _, header := range []string{"", "Basic dGVzdA==", "Bearer", "Bearer one two", "Bearer token"} {
		request := httptest.NewRequest(http.MethodPost, "/wallets", nil)
		if header != "" {
			request.Header.Set("Authorization", header)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		want := http.StatusUnauthorized
		if header == "Bearer token" {
			want = http.StatusServiceUnavailable
		}
		if response.Code != want {
			t.Fatal(response.Code, want)
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Add("Authorization", "Bearer first")
	request.Header.Add("Authorization", "Bearer second")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatal("multiple credentials accepted")
	}
}

func TestInternalRoleDoesNotGrantProviderAccess(t *testing.T) {
	a := authTestServer(t, func(c map[string]any) {
		delete(c, "providerId")
		c["resource_access"] = map[string]any{"api": map[string]any{"roles": []string{"internal"}}}
	}, http.StatusOK)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	for _, test := range []struct {
		handler http.Handler
		want    int
	}{{a.RequireInternal(next), 204}, {a.RequireProvider(next), 403}} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", "Bearer token")
		w := httptest.NewRecorder()
		test.handler.ServeHTTP(w, r)
		if w.Code != test.want {
			t.Fatal(w.Code, test.want)
		}
	}
}
