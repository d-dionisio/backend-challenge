package postgres

import (
	"context"
	"fmt"

	"github.com/d-dionisio/backend-challenge/internal/application/ports"
)

type InboxRepository struct{ db DBTX }

var _ ports.InboxRepository = (*InboxRepository)(nil)

func (r *InboxRepository) Register(ctx context.Context, consumerName, messageID, payloadHash string) (bool, error) {
	result, err := r.db.Exec(ctx, `INSERT INTO inbox (consumer_name, message_id, payload_hash)
		VALUES ($1,$2,$3) ON CONFLICT (consumer_name, message_id) DO NOTHING`, consumerName, messageID, payloadHash)
	if err != nil {
		return false, fmt.Errorf("register inbox message: %w", err)
	}
	if result.RowsAffected() == 1 {
		return false, nil
	}
	var existingHash string
	var completed bool
	err = r.db.QueryRow(ctx, `SELECT payload_hash, completed_at IS NOT NULL FROM inbox
		WHERE consumer_name=$1 AND message_id=$2`, consumerName, messageID).Scan(&existingHash, &completed)
	if err != nil {
		return false, fmt.Errorf("read inbox message: %w", err)
	}
	if existingHash != payloadHash {
		return false, ports.ErrInboxConflict
	}
	if !completed {
		return false, ports.ErrInboxIncomplete
	}
	return true, nil
}

func (r *InboxRepository) Complete(ctx context.Context, consumerName, messageID string) error {
	result, err := r.db.Exec(ctx, `UPDATE inbox SET completed_at=COALESCE(completed_at, now())
		WHERE consumer_name=$1 AND message_id=$2`, consumerName, messageID)
	if err != nil {
		return fmt.Errorf("complete inbox message: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ports.ErrInboxIncomplete
	}
	return nil
}
