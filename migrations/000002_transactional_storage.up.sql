ALTER TABLE wallets ADD CONSTRAINT chk_wallet_currency CHECK (currency IN ('BRL', 'USD'));

ALTER TABLE wager_transactions
    ADD COLUMN reference_attempts INTEGER NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
    ADD COLUMN next_reference_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD CONSTRAINT chk_wager_origin CHECK (
        (kind = 'OPENING' AND amount_cents > 0 AND status = 'PROCESSED'
            AND provider_id IS NULL AND external_transaction_id IS NULL
            AND idempotency_key IS NULL AND payload_hash IS NULL
            AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL
            AND result_balance_cents = amount_cents)
        OR
        (kind <> 'OPENING'
            AND provider_id IS NOT NULL AND btrim(provider_id) <> ''
            AND external_transaction_id IS NOT NULL AND btrim(external_transaction_id) <> ''
            AND idempotency_key IS NOT NULL AND btrim(idempotency_key) <> ''
            AND payload_hash IS NOT NULL AND btrim(payload_hash) <> ''
            AND round_id IS NOT NULL AND btrim(round_id) <> ''
            AND game_id IS NOT NULL AND btrim(game_id) <> '')
    ),
    ADD CONSTRAINT chk_wager_result CHECK (
        (status = 'PROCESSED' AND result_balance_cents IS NOT NULL AND failure_code IS NULL)
        OR (status IN ('PENDING', 'PENDING_REFERENCE') AND result_balance_cents IS NULL AND failure_code IS NULL)
        OR (status IN ('REJECTED', 'FAILED') AND result_balance_cents IS NULL
            AND failure_code IS NOT NULL AND btrim(failure_code) <> '')
    ),
    ADD CONSTRAINT chk_wager_reference CHECK (
        (reference_external_transaction_id IS NULL OR btrim(reference_external_transaction_id) <> '')
        AND (kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL)
        AND (status <> 'PENDING_REFERENCE' OR
            (kind IN ('WIN', 'REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL))
        AND (status <> 'PROCESSED' OR reference_transaction_id IS NOT NULL OR
            (kind NOT IN ('REFUND', 'ROLLBACK') AND NOT (kind = 'WIN' AND reference_external_transaction_id IS NOT NULL)))
        AND (reference_transaction_id IS NULL OR reference_transaction_id <> id)
    ),
    ADD CONSTRAINT chk_wager_dates CHECK (updated_at >= created_at),
    ADD CONSTRAINT uq_wager_ledger_identity UNIQUE (id, wallet_id, amount_cents, currency);

-- Uma BET não pode ser devolvida por REFUND e também por ROLLBACK.
CREATE UNIQUE INDEX uq_wager_successful_reversal
ON wager_transactions (reference_transaction_id)
WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

CREATE INDEX idx_wager_reference_retry
ON wager_transactions (next_reference_attempt_at, id)
WHERE status = 'PENDING_REFERENCE';

ALTER TABLE wallet_ledger_entries
    ADD CONSTRAINT fk_ledger_exact_transaction
    FOREIGN KEY (transaction_id, wallet_id, amount_cents, currency)
    REFERENCES wager_transactions (id, wallet_id, amount_cents, currency);

CREATE INDEX idx_ledger_wallet_order ON wallet_ledger_entries (wallet_id, created_at, id);

CREATE TRIGGER prevent_ledger_truncate
BEFORE TRUNCATE ON wallet_ledger_entries
FOR EACH STATEMENT EXECUTE FUNCTION prevent_ledger_mutation();

CREATE FUNCTION validate_wager_write() RETURNS TRIGGER AS $$
DECLARE
    original wager_transactions%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
            RAISE EXCEPTION 'terminal transaction is immutable' USING ERRCODE = '23514';
        END IF;
        IF ROW(NEW.id, NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key,
               NEW.payload_hash, NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id,
               NEW.kind, NEW.amount_cents, NEW.currency, NEW.reference_external_transaction_id, NEW.created_at)
           IS DISTINCT FROM
           ROW(OLD.id, OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key,
               OLD.payload_hash, OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id,
               OLD.kind, OLD.amount_cents, OLD.currency, OLD.reference_external_transaction_id, OLD.created_at) THEN
            RAISE EXCEPTION 'transaction business fields are immutable' USING ERRCODE = '23514';
        END IF;
        IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
            RAISE EXCEPTION 'invalid state transition' USING ERRCODE = '23514';
        END IF;
        IF OLD.reference_transaction_id IS NOT NULL AND NEW.reference_transaction_id IS DISTINCT FROM OLD.reference_transaction_id THEN
            RAISE EXCEPTION 'resolved reference is immutable' USING ERRCODE = '23514';
        END IF;
    END IF;
    IF NEW.reference_transaction_id IS NOT NULL THEN
        SELECT * INTO original FROM wager_transactions WHERE id = NEW.reference_transaction_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'reference not found' USING ERRCODE = '23503';
        END IF;
        IF original.status <> 'PROCESSED'
           OR ROW(original.provider_id, original.external_transaction_id, original.wallet_id,
                  original.player_id, original.currency, original.round_id)
              IS DISTINCT FROM ROW(NEW.provider_id, NEW.reference_external_transaction_id, NEW.wallet_id,
                                   NEW.player_id, NEW.currency, NEW.round_id) THEN
            RAISE EXCEPTION 'reference does not match operation' USING ERRCODE = '23514';
        END IF;
        IF (NEW.kind IN ('WIN', 'REFUND') AND original.kind <> 'BET')
           OR (NEW.kind = 'ROLLBACK' AND original.kind NOT IN ('BET', 'WIN', 'REFUND'))
           OR NEW.kind NOT IN ('WIN', 'REFUND', 'ROLLBACK')
           OR (NEW.kind IN ('REFUND', 'ROLLBACK') AND NEW.amount_cents <> original.amount_cents) THEN
            RAISE EXCEPTION 'invalid reference kind or amount' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_wager_write BEFORE INSERT OR UPDATE ON wager_transactions
FOR EACH ROW EXECUTE FUNCTION validate_wager_write();

CREATE FUNCTION validate_ledger_insert() RETURNS TRIGGER AS $$
DECLARE
    operation wager_transactions%ROWTYPE;
    original_kind TEXT;
    expected_direction TEXT;
BEGIN
    SELECT * INTO operation FROM wager_transactions WHERE id = NEW.transaction_id;
    IF NOT FOUND OR operation.status <> 'PROCESSED' OR operation.kind = 'LOSS' THEN
        RAISE EXCEPTION 'ledger requires a processed financial operation' USING ERRCODE = '23514';
    END IF;
    expected_direction := 'CREDIT';
    IF operation.kind = 'BET' THEN expected_direction := 'DEBIT'; END IF;
    IF operation.kind = 'ROLLBACK' THEN
        SELECT kind INTO original_kind FROM wager_transactions WHERE id = operation.reference_transaction_id;
        IF original_kind IN ('WIN', 'REFUND') THEN expected_direction := 'DEBIT'; END IF;
    END IF;
    IF NEW.direction <> expected_direction OR NEW.balance_after_cents <> operation.result_balance_cents
       OR (operation.kind = 'OPENING' AND NEW.balance_before_cents <> 0) THEN
        RAISE EXCEPTION 'ledger does not match operation result' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_ledger_insert BEFORE INSERT ON wallet_ledger_entries
FOR EACH ROW EXECUTE FUNCTION validate_ledger_insert();

CREATE TABLE inbox (
    consumer_name TEXT NOT NULL CHECK (btrim(consumer_name) <> ''),
    message_id TEXT NOT NULL CHECK (btrim(message_id) <> ''),
    payload_hash TEXT NOT NULL CHECK (btrim(payload_hash) <> ''),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, message_id),
    CHECK (completed_at IS NULL OR completed_at >= received_at)
);

CREATE TABLE outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES wallets(id),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'WagerTransactionProcessed', 'WagerTransactionRejected',
        'WalletBalanceChanged', 'WagerTransactionPendingReference')),
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    lock_token UUID,
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK ((payload->>'eventId') IS NOT NULL AND (payload->>'eventId')::UUID = event_id),
    CHECK ((payload->>'aggregateId') IS NOT NULL AND (payload->>'aggregateId')::UUID = aggregate_id),
    CHECK ((payload->>'eventType') IS NOT NULL AND payload->>'eventType' = event_type),
    CHECK ((payload->>'version') IS NOT NULL AND payload->>'version' = '1'),
    CHECK ((payload->>'correlationId') IS NOT NULL AND btrim(payload->>'correlationId') <> ''),
    CHECK ((payload->>'occurredAt') IS NOT NULL AND (payload->>'occurredAt')::TIMESTAMPTZ = occurred_at),
    CHECK (payload->'data' IS NOT NULL AND jsonb_typeof(payload->'data') = 'object'),
    CHECK ((locked_until IS NULL) = (lock_token IS NULL))
);

CREATE INDEX idx_outbox_pending ON outbox (next_attempt_at, event_id) WHERE published_at IS NULL;

CREATE FUNCTION protect_outbox_payload() RETURNS TRIGGER AS $$
BEGIN
    IF ROW(NEW.event_id, NEW.aggregate_id, NEW.event_type, NEW.payload, NEW.occurred_at)
       IS DISTINCT FROM ROW(OLD.event_id, OLD.aggregate_id, OLD.event_type, OLD.payload, OLD.occurred_at) THEN
        RAISE EXCEPTION 'outbox event snapshot is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER protect_outbox_payload BEFORE UPDATE ON outbox
FOR EACH ROW EXECUTE FUNCTION protect_outbox_payload();

-- O consumidor deve concluir a inbox no mesmo commit das alterações financeiras.
CREATE FUNCTION require_inbox_completion() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM inbox WHERE consumer_name = NEW.consumer_name
               AND message_id = NEW.message_id AND completed_at IS NULL) THEN
        RAISE EXCEPTION 'inbox treatment must complete before commit' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER require_inbox_completion
AFTER INSERT OR UPDATE ON inbox DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION require_inbox_completion();

-- A checagem ocorre no commit: durante o trabalho o saldo e o ledger podem
-- ser gravados em comandos separados, mas precisam terminar consistentes.
CREATE FUNCTION check_wallet_financial_state() RETURNS TRIGGER AS $$
DECLARE
    target_wallet UUID;
    stored_balance BIGINT;
    stored_version BIGINT;
    calculated_balance NUMERIC;
    calculated_version BIGINT;
BEGIN
    IF TG_TABLE_NAME = 'wallets' THEN target_wallet := NEW.id;
    ELSE target_wallet := NEW.wallet_id;
    END IF;
    SELECT balance_cents, version INTO stored_balance, stored_version
    FROM wallets WHERE id = target_wallet FOR UPDATE;

    SELECT COALESCE(sum(CASE WHEN ledger.direction = 'CREDIT'
                            THEN ledger.amount_cents::NUMERIC ELSE -ledger.amount_cents::NUMERIC END), 0),
           1 + count(*) FILTER (WHERE operation.kind <> 'OPENING')
    INTO calculated_balance, calculated_version
    FROM wallet_ledger_entries ledger
    JOIN wager_transactions operation ON operation.id = ledger.transaction_id
    WHERE ledger.wallet_id = target_wallet;

    IF stored_balance <> calculated_balance OR stored_version <> calculated_version THEN
        RAISE EXCEPTION 'wallet balance or version does not match ledger' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM wager_transactions operation
        WHERE operation.wallet_id = target_wallet AND operation.status = 'PROCESSED' AND operation.kind <> 'LOSS'
        AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries ledger WHERE ledger.transaction_id = operation.id)
    ) THEN
        RAISE EXCEPTION 'processed financial operation requires a ledger entry' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER check_wallet_financial_state
AFTER INSERT OR UPDATE ON wallets DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_wallet_financial_state();

CREATE CONSTRAINT TRIGGER check_wager_financial_state
AFTER INSERT OR UPDATE ON wager_transactions DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_wallet_financial_state();

CREATE CONSTRAINT TRIGGER check_ledger_financial_state
AFTER INSERT ON wallet_ledger_entries DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_wallet_financial_state();
