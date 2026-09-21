package httpapi

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"
)

func (a *API) listWalletLedger(w http.ResponseWriter, r *http.Request) {
	walletID, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil || walletID == uuid.Nil {
		writeError(w, 400, "INVALID_REQUEST")
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, 400, "INVALID_LEDGER_PAGE")
		return
	}
	limit := 50
	if values, ok := query["limit"]; ok {
		if len(values) != 1 {
			writeError(w, 400, "INVALID_LEDGER_PAGE")
			return
		}
		limit, err = strconv.Atoi(values[0])
		if err != nil {
			writeError(w, 400, "INVALID_LEDGER_PAGE")
			return
		}
	}
	if len(query["cursor"]) > 1 {
		writeError(w, 400, "INVALID_LEDGER_PAGE")
		return
	}
	page, err := a.listLedger.Execute(r.Context(), walletID, query.Get("cursor"), limit)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, page)
}

func (a *API) reconcileWalletBalance(w http.ResponseWriter, r *http.Request) {
	walletID, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil || walletID == uuid.Nil {
		writeError(w, 400, "INVALID_REQUEST")
		return
	}
	result, err := a.reconcileWallet.Execute(r.Context(), walletID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if !result.Consistent {
		a.metrics.AddReconciliationDivergence()
		a.logger.Warn("wallet reconciliation divergence", "correlationId", correlationID(r), "walletId", walletID, "consistent", false, "checkedEntries", result.CheckedEntries)
	}
	writeJSON(w, 200, result)
}
