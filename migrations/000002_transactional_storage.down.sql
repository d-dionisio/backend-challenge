DROP TRIGGER check_wallet_financial_state ON wallets;
DROP TRIGGER check_wager_financial_state ON wager_transactions;
DROP TRIGGER check_ledger_financial_state ON wallet_ledger_entries;
DROP FUNCTION check_wallet_financial_state();
DROP TABLE outbox;
DROP FUNCTION protect_outbox_payload();
DROP TABLE inbox;
DROP FUNCTION require_inbox_completion();
DROP TRIGGER validate_ledger_insert ON wallet_ledger_entries;
DROP FUNCTION validate_ledger_insert();
DROP TRIGGER validate_wager_write ON wager_transactions;
DROP FUNCTION validate_wager_write();
DROP TRIGGER prevent_ledger_truncate ON wallet_ledger_entries;
DROP INDEX idx_ledger_wallet_order;
ALTER TABLE wallet_ledger_entries DROP CONSTRAINT fk_ledger_exact_transaction;
DROP INDEX uq_wager_successful_reversal;
DROP INDEX idx_wager_reference_retry;
ALTER TABLE wager_transactions
    DROP CONSTRAINT uq_wager_ledger_identity,
    DROP CONSTRAINT chk_wager_origin,
    DROP CONSTRAINT chk_wager_result,
    DROP CONSTRAINT chk_wager_reference,
    DROP CONSTRAINT chk_wager_dates,
    DROP COLUMN reference_attempts,
    DROP COLUMN next_reference_attempt_at;
ALTER TABLE wallets DROP CONSTRAINT chk_wallet_currency;
