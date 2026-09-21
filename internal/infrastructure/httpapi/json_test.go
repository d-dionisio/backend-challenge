package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestReadJSON(t *testing.T) {
	for _, test := range []struct {
		name, body, contentType string
		status                  int
	}{
		{"valid", `{"amount":"25.00","currency":"BRL"}`, "application/json; charset=utf-8", 200},
		{"number is not money", `{"amount":25.00,"currency":"BRL"}`, "application/json", 400},
		{"unknown field", `{"amount":"25","extra":true}`, "application/json", 400},
		{"two documents", `{} {}`, "application/json", 400},
		{"empty", ``, "application/json", 400},
		{"incomplete", `{"amount":`, "application/json", 400},
		{"wrong content type", `{}`, "text/plain", 415},
		{"too large", `{"amount":"` + strings.Repeat("0", 1<<20) + `"}`, "application/json", 413},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", strings.NewReader(test.body))
			r.Header.Set("Content-Type", test.contentType)
			w := httptest.NewRecorder()
			var request moneyRequest
			ok := readJSON(w, r, &request)
			if w.Code != test.status || ok != (test.status == 200) {
				t.Fatalf("status=%d, accepted=%t, body=%s", w.Code, ok, w.Body)
			}
		})
	}
}

func TestErrorResponses(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
		code   string
	}{
		{application.ErrIdempotencyConflict, 409, "IDEMPOTENCY_CONFLICT"},
		{application.ErrInvalidLedgerPage, 400, "INVALID_LEDGER_PAGE"},
		{errors.Join(ports.ErrInvalidFinancialState, domain.ErrMoneyOverflow), 500, "INVALID_FINANCIAL_STATE"},
		{ports.ErrWalletConflict, 409, "WALLET_ALREADY_EXISTS"},
		{ports.ErrWalletNotFound, 404, "NOT_FOUND"},
		{ports.ErrWagerNotFound, 404, "NOT_FOUND"},
		{application.ErrProviderNotAuthorized, 403, "FORBIDDEN"},
		{domain.ErrInvalidMoney, 400, "INVALID_REQUEST"},
		{domain.ErrExternalOpening, 400, "INVALID_REQUEST"},
		{context.DeadlineExceeded, 503, "DEPENDENCY_UNAVAILABLE"},
		{&pgconn.PgError{Code: "40001", Message: "private database details"}, 503, "DEPENDENCY_UNAVAILABLE"},
		{errors.New("private database details"), 500, "INTERNAL_ERROR"},
	} {
		status, code := errorResponse(fmt.Errorf("wrapped: %w", test.err))
		if status != test.status || code != test.code {
			t.Errorf("%v: got %d %s", test.err, status, code)
		}
	}
}
