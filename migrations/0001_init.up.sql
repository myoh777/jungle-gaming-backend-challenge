-- Wallets: one per (player_id, currency). Balance is int64 minor units (cents).
CREATE TABLE wallets (
    id             uuid        PRIMARY KEY,
    player_id      text        NOT NULL CHECK (player_id <> ''),
    currency       char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_amount bigint      NOT NULL CHECK (balance_amount >= 0),
    version        bigint      NOT NULL CHECK (version >= 1),
    created_at     timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency),
    -- Target for the ledger composite FK (wallet currency consistency).
    CONSTRAINT wallets_id_currency_key UNIQUE (id, currency)
);

-- Wager transactions. The row itself is the persistent idempotency record:
-- (provider_id, idempotency_key) and (provider_id, external_transaction_id)
-- are unique, and payload_hash detects key reuse with another payload.
CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL CHECK (origin IN ('EXTERNAL', 'INTERNAL')),
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text,
    wallet_id                         uuid        NOT NULL REFERENCES wallets (id),
    player_id                         text        NOT NULL,
    round_id                          text,
    game_id                           text,
    kind                              text        NOT NULL CHECK (kind IN ('BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK', 'OPENING')),
    amount                            bigint      NOT NULL CHECK (amount >= 0),
    currency                          char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    reference_external_transaction_id text,
    reference_transaction_id          uuid        REFERENCES wager_transactions (id),
    status                            text        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code                      text,
    result_balance_amount             bigint      CHECK (result_balance_amount >= 0),
    result_wallet_version             bigint,
    reference_attempts                integer     NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
    next_reference_attempt_at         timestamptz,
    correlation_id                    text,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,

    CONSTRAINT wager_transactions_provider_external_key UNIQUE (provider_id, external_transaction_id),
    CONSTRAINT wager_transactions_provider_idempotency_key UNIQUE (provider_id, idempotency_key),
    -- Target for the ledger composite FK (ledger wallet = transaction wallet).
    CONSTRAINT wager_transactions_id_wallet_key UNIQUE (id, wallet_id),

    -- INTERNAL rows are exactly the OPENING credit and carry no external metadata;
    -- EXTERNAL rows must carry all of it and can never be OPENING.
    CONSTRAINT wager_transactions_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL
            AND status = 'PROCESSED' AND amount > 0)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_zero_policy CHECK ((kind = 'LOSS') = (amount = 0)),
    CONSTRAINT wager_transactions_reversal_has_reference CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_transactions_failure_code CHECK (
        (status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_processed_result CHECK (
        status <> 'PROCESSED' OR (result_balance_amount IS NOT NULL AND result_wallet_version IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_pending_reference_schedule CHECK (
        (status = 'PENDING_REFERENCE') = (next_reference_attempt_at IS NOT NULL)
    )
);

-- At most one OPENING per wallet.
CREATE UNIQUE INDEX wager_transactions_one_opening_per_wallet
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- At most one successful reversal (REFUND or ROLLBACK, combined) per referenced transaction.
CREATE UNIQUE INDEX wager_transactions_one_reversal_per_reference
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

CREATE INDEX wager_transactions_pending_reference_due
    ON wager_transactions (next_reference_attempt_at) WHERE status = 'PENDING_REFERENCE';

CREATE INDEX wager_transactions_wallet ON wager_transactions (wallet_id);

-- Append-only ledger. seq gives a stable order for cursor pagination.
CREATE TABLE wallet_ledger_entries (
    id             uuid        PRIMARY KEY,
    seq            bigserial   NOT NULL UNIQUE,
    wallet_id      uuid        NOT NULL,
    transaction_id uuid        NOT NULL,
    direction      text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount         bigint      NOT NULL CHECK (amount > 0),
    currency       char(3)     NOT NULL,
    balance_before bigint      NOT NULL CHECK (balance_before >= 0),
    balance_after  bigint      NOT NULL CHECK (balance_after >= 0),
    created_at     timestamptz NOT NULL,
    CONSTRAINT wallet_ledger_entries_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_wallet_currency_fkey FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
    CONSTRAINT wallet_ledger_entries_transaction_wallet_fkey FOREIGN KEY (transaction_id, wallet_id) REFERENCES wager_transactions (id, wallet_id),
    CONSTRAINT wallet_ledger_entries_balance_math CHECK (
        (direction = 'CREDIT' AND balance_after = balance_before + amount)
        OR (direction = 'DEBIT' AND balance_after = balance_before - amount)
    )
);

CREATE INDEX wallet_ledger_entries_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

CREATE FUNCTION wallet_ledger_entries_immutable() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER wallet_ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_immutable();

CREATE TRIGGER wallet_ledger_entries_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION wallet_ledger_entries_immutable();

-- Inbox for SQS deduplication by the application (not by FIFO dedup).
CREATE TABLE inbox_messages (
    consumer_name  text        NOT NULL,
    message_id     text        NOT NULL,
    payload_hash   text        NOT NULL,
    transaction_id uuid        REFERENCES wager_transactions (id),
    received_at    timestamptz NOT NULL,
    processed_at   timestamptz NOT NULL,
    PRIMARY KEY (consumer_name, message_id)
);

-- Transactional outbox. payload is the immutable serialized event envelope.
CREATE TABLE outbox_events (
    id                uuid        PRIMARY KEY, -- = eventId
    seq               bigserial   NOT NULL UNIQUE,
    event_type        text        NOT NULL,
    aggregate_id      text        NOT NULL,
    payload           jsonb       NOT NULL,
    created_at        timestamptz NOT NULL,
    attempts          integer     NOT NULL DEFAULT 0,
    next_attempt_at   timestamptz NOT NULL,
    lease_owner       text,
    lease_expires_at  timestamptz,
    last_error        text,
    published_at      timestamptz
);

CREATE INDEX outbox_events_unpublished
    ON outbox_events (next_attempt_at, seq) WHERE published_at IS NULL;

-- The payload is a snapshot: it may never change after insert.
CREATE FUNCTION outbox_events_payload_immutable() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    IF NEW.payload IS DISTINCT FROM OLD.payload OR NEW.id <> OLD.id OR NEW.event_type <> OLD.event_type THEN
        RAISE EXCEPTION 'outbox_events payload is immutable' USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_events_payload_immutable
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_payload_immutable();
