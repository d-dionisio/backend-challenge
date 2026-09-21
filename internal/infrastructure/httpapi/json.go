package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
)

func readJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		writeError(w, 415, "UNSUPPORTED_MEDIA_TYPE")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeDecodeError(w, err)
		return false
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			writeError(w, 400, "INVALID_REQUEST")
		} else {
			writeDecodeError(w, err)
		}
		return false
	}
	return true
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, 413, "REQUEST_TOO_LARGE")
		return
	}
	writeError(w, 400, "INVALID_REQUEST")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	// Serializa antes do status: um erro de serialização não vira um falso 200.
	body, err := json.Marshal(value)
	if err != nil {
		writeError(w, 500, "INTERNAL_ERROR")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, struct {
		Code string `json:"code"`
	}{Code: code})
}

func errorResponse(err error) (int, string) {
	switch {
	case errors.Is(err, application.ErrProviderNotAuthorized):
		return 403, "FORBIDDEN"
	case errors.Is(err, application.ErrIdempotencyConflict):
		return 409, "IDEMPOTENCY_CONFLICT"
	case errors.Is(err, ports.ErrWalletConflict):
		return 409, "WALLET_ALREADY_EXISTS"
	case errors.Is(err, ports.ErrWalletNotFound), errors.Is(err, ports.ErrWagerNotFound):
		return 404, "NOT_FOUND"
	case errors.Is(err, application.ErrWalletIdentityMismatch):
		return 400, "WALLET_IDENTITY_MISMATCH"
	}
	for _, invalid := range []error{domain.ErrInvalidMoney, domain.ErrMoneyOverflow, domain.ErrInvalidWallet,
		domain.ErrInvalidWagerTransaction, domain.ErrInvalidWagerKind, domain.ErrInvalidWagerMoney, domain.ErrReferenceRequired,
		domain.ErrInvalidWagerReference, domain.ErrExternalOpening} {
		if errors.Is(err, invalid) {
			return 400, "INVALID_REQUEST"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, ports.ErrWalletConcurrentUpdate) || errors.Is(err, io.EOF) {
		return 503, "DEPENDENCY_UNAVAILABLE"
	}
	var networkError net.Error
	var databaseError *pgconn.PgError
	if errors.As(err, &networkError) {
		return 503, "DEPENDENCY_UNAVAILABLE"
	}
	if errors.As(err, &databaseError) && (strings.HasPrefix(databaseError.Code, "08") || strings.HasPrefix(databaseError.Code, "53") ||
		strings.HasPrefix(databaseError.Code, "57P") || databaseError.Code == "40001" || databaseError.Code == "40P01") {
		return 503, "DEPENDENCY_UNAVAILABLE"
	}
	return 500, "INTERNAL_ERROR"
}
