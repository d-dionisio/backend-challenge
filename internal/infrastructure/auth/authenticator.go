package auth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"go.uber.org/fx"
)

var (
	ErrUnauthenticated             = errors.New("invalid or missing access token")
	ErrIdentityProviderUnavailable = errors.New("identity provider unavailable")
)

type Identity struct {
	Subject    string
	ProviderID string
	provider   bool
	internal   bool
}

type tokenClaims struct {
	Active         bool            `json:"active"`
	Issuer         string          `json:"iss"`
	Audience       json.RawMessage `json:"aud"`
	Subject        string          `json:"sub"`
	ExpiresAt      int64           `json:"exp"`
	NotBefore      int64           `json:"nbf"`
	TokenType      string          `json:"token_type"`
	ProviderID     string          `json:"providerId"`
	ResourceAccess map[string]struct {
		Roles []string `json:"roles"`
	} `json:"resource_access"`
}

type Authenticator struct {
	settings         Config
	client           *http.Client
	introspectionURL string
}

func NewAuthenticator(lifecycle fx.Lifecycle, settings Config) (*Authenticator, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	a := &Authenticator{settings: settings, client: &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ready := false
			defer func() {
				if !ready {
					transport.CloseIdleConnections()
				}
			}()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, settings.IssuerURL+"/.well-known/openid-configuration", nil)
			if err != nil {
				return ErrIdentityProviderUnavailable
			}
			response, err := a.client.Do(request)
			if err != nil {
				return ErrIdentityProviderUnavailable
			}
			defer response.Body.Close()
			var discovery struct {
				Issuer                string `json:"issuer"`
				IntrospectionEndpoint string `json:"introspection_endpoint"`
			}
			if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&discovery) != nil || discovery.Issuer != settings.IssuerURL {
				return ErrIdentityProviderUnavailable
			}
			issuer, _ := url.Parse(settings.IssuerURL)
			endpoint, err := url.Parse(discovery.IntrospectionEndpoint)
			if err != nil || endpoint.Scheme != issuer.Scheme || endpoint.Host != issuer.Host || endpoint.User != nil || endpoint.Fragment != "" {
				return ErrIdentityProviderUnavailable
			}
			a.introspectionURL = endpoint.String()
			// Confere as credenciais do cliente da API. Um token fictício deve
			// ser consultável e retornar inativo, sem emitir token próprio.
			_, err = a.introspect(ctx, "startup-check")
			ready = err == nil
			return err
		},
		OnStop: func(context.Context) error { transport.CloseIdleConnections(); return nil },
	})
	return a, nil
}

func (a *Authenticator) introspect(ctx context.Context, token string) (tokenClaims, error) {
	var claims tokenClaims
	form := url.Values{"token": {token}, "token_type_hint": {"access_token"}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.introspectionURL, strings.NewReader(form.Encode()))
	if err != nil {
		return claims, ErrIdentityProviderUnavailable
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	request.SetBasicAuth(a.settings.ClientID, a.settings.ClientSecret)
	response, err := a.client.Do(request)
	if err != nil {
		return claims, ErrIdentityProviderUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&claims) != nil {
		return claims, ErrIdentityProviderUnavailable
	}
	return claims, nil
}

func (a *Authenticator) Authenticate(ctx context.Context, token string) (Identity, error) {
	if strings.TrimSpace(token) == "" {
		return Identity{}, ErrUnauthenticated
	}
	claims, err := a.introspect(ctx, token)
	if err != nil {
		return Identity{}, err
	}
	now := time.Now().Unix()
	if !claims.Active || claims.Issuer != a.settings.IssuerURL || claims.Subject == "" || claims.ExpiresAt <= now || claims.NotBefore > now ||
		!strings.EqualFold(claims.TokenType, "Bearer") || !hasAudience(claims.Audience, a.settings.Audience) {
		return Identity{}, ErrUnauthenticated
	}
	roles := claims.ResourceAccess[a.settings.Audience].Roles
	identity := Identity{Subject: claims.Subject, ProviderID: claims.ProviderID,
		provider: slices.Contains(roles, "provider"), internal: slices.Contains(roles, "internal")}
	if strings.TrimSpace(identity.ProviderID) == "" || !utf8.ValidString(identity.ProviderID) || utf8.RuneCountInString(identity.ProviderID) > 100 {
		identity.provider = false
	}
	return identity, nil
}

func hasAudience(raw json.RawMessage, expected string) bool {
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single == expected
	}
	var multiple []string
	return json.Unmarshal(raw, &multiple) == nil && slices.Contains(multiple, expected)
}

var Module = fx.Module("auth", fx.Provide(NewConfig, NewAuthenticator), fx.Invoke(func(*Authenticator) {}))
