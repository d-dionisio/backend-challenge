package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type identityKey struct{}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	return identity, ok
}

func (a *Authenticator) RequireProvider(next http.Handler) http.Handler {
	return a.authorize(next, false)
}

func (a *Authenticator) RequireInternal(next http.Handler) http.Handler {
	return a.authorize(next, true)
}

func (a *Authenticator) authorize(next http.Handler, internal bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		if len(values) != 1 {
			writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED")
			return
		}
		parts := strings.Fields(values[0])
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED")
			return
		}
		identity, err := a.Authenticate(r.Context(), parts[1])
		if err != nil {
			if errors.Is(err, ErrIdentityProviderUnavailable) {
				writeAuthError(w, http.StatusServiceUnavailable, "IDENTITY_PROVIDER_UNAVAILABLE")
			} else {
				writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED")
			}
			return
		}
		allowed := identity.provider && !identity.internal
		if internal {
			allowed = identity.internal && !identity.provider && identity.ProviderID == ""
		}
		if !allowed {
			writeAuthError(w, http.StatusForbidden, "FORBIDDEN")
			return
		}
		ctx := context.WithValue(r.Context(), identityKey{}, identity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func writeAuthError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="wagering-api"`)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Code string `json:"code"`
	}{Code: code})
}
