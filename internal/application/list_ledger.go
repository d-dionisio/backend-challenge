package application

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

var ErrInvalidLedgerPage = errors.New("invalid ledger cursor or limit")

type LedgerPage struct {
	Items      []ports.LedgerRecord `json:"items"`
	NextCursor string               `json:"nextCursor,omitempty"`
}

type ListLedger struct{ queries ports.WalletQueries }

func NewListLedger(queries ports.WalletQueries) *ListLedger { return &ListLedger{queries: queries} }

func (useCase *ListLedger) Execute(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (*LedgerPage, error) {
	if walletID == uuid.Nil {
		return nil, domain.ErrInvalidWallet
	}
	if limit < 1 || limit > 100 {
		return nil, ErrInvalidLedgerPage
	}
	position, err := decodeLedgerCursor(walletID, cursor)
	if err != nil {
		return nil, err
	}
	// Uma linha extra informa se existe outra página, sem contar todo o ledger.
	items, err := useCase.queries.ListLedger(ctx, walletID, position, limit+1)
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []ports.LedgerRecord{}
	}
	page := &LedgerPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		last := page.Items[limit-1]
		encoded, err := json.Marshal(ports.LedgerPosition{WalletID: walletID, CreatedAt: last.CreatedAt, ID: last.ID})
		if err != nil {
			return nil, err
		}
		page.NextCursor = base64.RawURLEncoding.EncodeToString(encoded)
	}
	return page, nil
}

func decodeLedgerCursor(walletID uuid.UUID, cursor string) (*ports.LedgerPosition, error) {
	if cursor == "" {
		return nil, nil
	}
	if len(cursor) > 512 {
		return nil, ErrInvalidLedgerPage
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(cursor)
	if err != nil {
		return nil, ErrInvalidLedgerPage
	}
	var position ports.LedgerPosition
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&position); err != nil {
		return nil, ErrInvalidLedgerPage
	}
	var extra json.RawMessage
	if decoder.Decode(&extra) != io.EOF || position.WalletID != walletID || position.ID == uuid.Nil || position.CreatedAt.IsZero() {
		return nil, ErrInvalidLedgerPage
	}
	return &position, nil
}
