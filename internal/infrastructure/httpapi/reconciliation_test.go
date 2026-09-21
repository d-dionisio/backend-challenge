package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

type divergentWalletQuery struct{ snapshot *ports.ReconciliationSnapshot }

func (q divergentWalletQuery) ListLedger(context.Context, uuid.UUID, *ports.LedgerPosition, int) ([]ports.LedgerRecord, error) {
	return nil, nil
}
func (q divergentWalletQuery) ReconciliationSnapshot(context.Context, uuid.UUID) (*ports.ReconciliationSnapshot, error) {
	return q.snapshot, nil
}

func TestReconciliationLogsAndConcurrentCounter(t *testing.T) {
	stored, _ := domain.NewMoney("99.99", "BRL")
	calculated, _ := domain.NewMoney("100", "BRL")
	id := uuid.New()
	query := divergentWalletQuery{snapshot: &ports.ReconciliationSnapshot{WalletID: id, StoredBalance: stored, CalculatedBalance: calculated, CheckedEntries: 1}}
	var logs bytes.Buffer
	api := &API{reconcileWallet: application.NewReconcileWallet(query), metrics: NewMetrics(), logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	var requests sync.WaitGroup
	for i := 0; i < 20; i++ {
		requests.Add(1)
		go func() {
			defer requests.Done()
			r := httptest.NewRequest("POST", "/wallets/"+id.String()+"/reconciliation", nil)
			r.SetPathValue("walletId", id.String())
			r = r.WithContext(context.WithValue(r.Context(), correlationKey{}, "reconcile-request"))
			w := httptest.NewRecorder()
			api.reconcileWalletBalance(w, r)
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"amount":"-0.01"`) {
				t.Errorf("unexpected reconciliation response: %d %s", w.Code, w.Body)
			}
		}()
	}
	requests.Wait()
	w := httptest.NewRecorder()
	api.metrics.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), "wallet_reconciliation_divergences_total 20\n") || w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatal(w.Body.String())
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 20 {
		t.Fatal("lost divergence logs")
	}
	for _, line := range lines {
		var record struct {
			Level, CorrelationID, WalletID string
			Consistent                     bool
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record.Level != "WARN" || record.CorrelationID != "reconcile-request" || record.WalletID != id.String() || record.Consistent {
			t.Fatal(record)
		}
		if strings.Contains(line, "99.99") || strings.Contains(line, "100.00") {
			t.Fatal("financial amounts exposed in log")
		}
	}
}
