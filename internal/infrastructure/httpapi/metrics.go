package httpapi

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct{ reconciliationDivergences atomic.Uint64 }

func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = fmt.Fprintf(w, "# HELP wallet_reconciliation_divergences_total Reconciliation checks that found a balance divergence.\n# TYPE wallet_reconciliation_divergences_total counter\nwallet_reconciliation_divergences_total %d\n", m.reconciliationDivergences.Load())
}
