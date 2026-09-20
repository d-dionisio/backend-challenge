CREATE TABLE wallets (
    id UUID PRIMARY KEY,

    player_id UUID NOT NULL,

    balance_cents BIGINT NOT NULL
        CHECK (balance_cents >= 0),

    currency CHAR(3) NOT NULL,

    version BIGINT NOT NULL
        CHECK (version >= 1),

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT uq_wallet_player_currency
        UNIQUE (player_id, currency),

    CONSTRAINT uq_wallet_identity
        UNIQUE (id, player_id, currency)
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,

    provider_id VARCHAR(100),

    external_transaction_id VARCHAR(255),

    idempotency_key VARCHAR(255),

    payload_hash VARCHAR(255),

    wallet_id UUID NOT NULL,

    player_id UUID NOT NULL,

    round_id VARCHAR(255),

    game_id VARCHAR(255),

    kind VARCHAR(20) NOT NULL,

    amount_cents BIGINT NOT NULL,

    currency CHAR(3) NOT NULL,

    reference_external_transaction_id VARCHAR(255),

    reference_transaction_id UUID,

    status VARCHAR(30) NOT NULL,

    failure_code VARCHAR(100),

    result_balance_cents BIGINT,

    created_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_wager_wallet
        FOREIGN KEY (wallet_id, player_id, currency)
        REFERENCES wallets (id, player_id, currency),

    CONSTRAINT fk_wager_reference
        FOREIGN KEY (reference_transaction_id)
        REFERENCES wager_transactions (id),

    CONSTRAINT chk_wager_kind
        CHECK (
            kind IN (
                'OPENING',
                'BET',
                'WIN',
                'LOSS',
                'REFUND',
                'ROLLBACK'
            )
        ),

    CONSTRAINT chk_wager_status
        CHECK (
            status IN (
                'PENDING',
                'PENDING_REFERENCE',
                'PROCESSED',
                'REJECTED',
                'FAILED'
            )
        ),

    CONSTRAINT chk_wager_amount
        CHECK (
            (
                kind = 'LOSS'
                AND amount_cents = 0
            )
            OR
            (
                kind = 'OPENING'
                AND amount_cents >= 0
            )
            OR
            (
                kind IN (
                    'BET',
                    'WIN',
                    'REFUND',
                    'ROLLBACK'
                )
                AND amount_cents > 0
            )
        ),

    CONSTRAINT chk_result_balance_non_negative
        CHECK (
            result_balance_cents IS NULL
            OR result_balance_cents >= 0
        )
);

CREATE UNIQUE INDEX uq_wager_provider_external_transaction
ON wager_transactions (
    provider_id,
    external_transaction_id
)
WHERE provider_id IS NOT NULL
AND external_transaction_id IS NOT NULL;

CREATE UNIQUE INDEX uq_wager_provider_idempotency
ON wager_transactions (
    provider_id,
    idempotency_key
)
WHERE provider_id IS NOT NULL
AND idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX uq_wager_opening_wallet
ON wager_transactions (wallet_id)
WHERE kind = 'OPENING';

CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY,

    wallet_id UUID NOT NULL,

    transaction_id UUID NOT NULL,

    direction VARCHAR(10) NOT NULL,

    amount_cents BIGINT NOT NULL,

    currency CHAR(3) NOT NULL,

    balance_before_cents BIGINT NOT NULL,

    balance_after_cents BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_ledger_wallet
        FOREIGN KEY (wallet_id)
        REFERENCES wallets (id),

    CONSTRAINT fk_ledger_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES wager_transactions (id),

    CONSTRAINT uq_ledger_wallet_transaction
        UNIQUE (wallet_id, transaction_id),

    CONSTRAINT chk_ledger_direction
        CHECK (
            direction IN ('DEBIT', 'CREDIT')
        ),

    CONSTRAINT chk_ledger_amount_positive
        CHECK (
            amount_cents > 0
        ),

    CONSTRAINT chk_ledger_balance_before
        CHECK (
            balance_before_cents >= 0
        ),

    CONSTRAINT chk_ledger_balance_after
        CHECK (
            balance_after_cents >= 0
        ),

    CONSTRAINT chk_ledger_math
        CHECK (
            (
                direction = 'DEBIT'
                AND balance_after_cents =
                    balance_before_cents - amount_cents
            )
            OR
            (
                direction = 'CREDIT'
                AND balance_after_cents =
                    balance_before_cents + amount_cents
            )
        )
);

CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'wallet ledger entries are immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prevent_ledger_update
BEFORE UPDATE ON wallet_ledger_entries
FOR EACH ROW
EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TRIGGER prevent_ledger_delete
BEFORE DELETE ON wallet_ledger_entries
FOR EACH ROW
EXECUTE FUNCTION prevent_ledger_mutation();