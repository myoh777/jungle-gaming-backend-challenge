package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"wagering/internal/app"
	"wagering/internal/domain/wagering"
	"wagering/internal/domain/wallet"
)

// txRepo implements app.Tx on top of one pgx transaction.
type txRepo struct {
	tx pgx.Tx
}

var _ app.Tx = (*txRepo)(nil)

func (r *txRepo) InsertWallet(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.tx.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_amount, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().MinorUnits(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err, "wallets_player_currency_key") {
		return app.ErrWalletAlreadyExists
	}
	return err
}

// LockWallet takes FOR NO KEY UPDATE: it serializes writers of the same wallet
// but does not conflict with the FOR KEY SHARE locks that foreign-key inserts
// (transactions, ledger) take on the wallet row.
func (r *txRepo) LockWallet(ctx context.Context, id string) (*wallet.Wallet, error) {
	return scanWallet(r.tx.QueryRow(ctx, selectWallet+` WHERE id = $1 FOR NO KEY UPDATE`, id))
}

func (r *txRepo) UpdateWalletBalance(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.tx.Exec(ctx, `
		UPDATE wallets SET balance_amount = $2, version = $3, updated_at = $4
		WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().MinorUnits(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: wallet %s version %d", app.ErrConcurrentModification, w.ID(), expectedVersion)
	}
	return nil
}

func (r *txRepo) InsertTransaction(ctx context.Context, t wagering.Snapshot) (bool, error) {
	var resultBalance *int64
	if t.ResultBalance != nil {
		v := t.ResultBalance.MinorUnits()
		resultBalance = &v
	}
	tag, err := r.tx.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount, currency,
			reference_external_transaction_id, reference_transaction_id, status, failure_code,
			result_balance_amount, result_wallet_version, reference_attempts, next_reference_attempt_at,
			correlation_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
		ON CONFLICT DO NOTHING`,
		t.ID, string(t.Origin), nullable(t.ProviderID), nullable(t.ExternalTransactionID), nullable(t.IdempotencyKey),
		nullable(t.PayloadHash), t.WalletID, t.PlayerID, nullable(t.RoundID), nullable(t.GameID), string(t.Kind),
		t.Money.MinorUnits(), string(t.Money.Currency()), nullable(t.ReferenceExternalTransactionID),
		nullable(t.ReferenceTransactionID), string(t.Status), nullable(string(t.FailureCode)),
		resultBalance, t.ResultWalletVersion, t.ReferenceAttempts, t.NextReferenceAttemptAt,
		nullable(t.CorrelationID), t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// UpdatePendingReference persists a transition out of (or within)
// PENDING_REFERENCE. The WHERE clause makes it a no-op-with-error if the row
// already left that state.
func (r *txRepo) UpdatePendingReference(ctx context.Context, t wagering.Snapshot) error {
	var resultBalance *int64
	if t.ResultBalance != nil {
		v := t.ResultBalance.MinorUnits()
		resultBalance = &v
	}
	tag, err := r.tx.Exec(ctx, `
		UPDATE wager_transactions
		SET status = $2, failure_code = $3, reference_transaction_id = $4,
		    result_balance_amount = $5, result_wallet_version = $6,
		    reference_attempts = $7, next_reference_attempt_at = $8, updated_at = $9
		WHERE id = $1 AND status = 'PENDING_REFERENCE'`,
		t.ID, string(t.Status), nullable(string(t.FailureCode)), nullable(t.ReferenceTransactionID),
		resultBalance, t.ResultWalletVersion, t.ReferenceAttempts, t.NextReferenceAttemptAt, t.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: transaction %s is no longer PENDING_REFERENCE", app.ErrConcurrentModification, t.ID)
	}
	return nil
}

func (r *txRepo) MarkTransactionFailed(ctx context.Context, t wagering.Snapshot) error {
	_, err := r.tx.Exec(ctx, `
		UPDATE wager_transactions
		SET status = 'FAILED', failure_code = $2, next_reference_attempt_at = NULL, updated_at = $3
		WHERE id = $1 AND status IN ('PENDING', 'PENDING_REFERENCE')`,
		t.ID, string(t.FailureCode), t.UpdatedAt)
	return err
}

func (r *txRepo) GetTransaction(ctx context.Context, id string) (*wagering.Transaction, error) {
	t, err := scanTransaction(r.tx.QueryRow(ctx, selectTransaction+` WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, err
}

func (r *txRepo) LockTransaction(ctx context.Context, id string) (*wagering.Transaction, error) {
	t, err := scanTransaction(r.tx.QueryRow(ctx, selectTransaction+` WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, err
}

func (r *txRepo) FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wagering.Transaction, error) {
	return optional(scanTransaction(r.tx.QueryRow(ctx, selectTransaction+` WHERE provider_id = $1 AND idempotency_key = $2`, providerID, key)))
}

func (r *txRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error) {
	return optional(scanTransaction(r.tx.QueryRow(ctx, selectTransaction+` WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID)))
}

func optional(t *wagering.Transaction, err error) (*wagering.Transaction, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

func (r *txRepo) HasSuccessfulReversal(ctx context.Context, referenceTransactionID string) (bool, error) {
	var exists bool
	err := r.tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM wager_transactions
			WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`,
		referenceTransactionID).Scan(&exists)
	return exists, err
}

func (r *txRepo) InsertLedgerEntry(ctx context.Context, e wallet.LedgerEntry) error {
	_, err := r.tx.Exec(ctx, `
		INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID, e.WalletID, e.TransactionID, string(e.Direction), e.Amount.MinorUnits(), string(e.Amount.Currency()),
		e.BalanceBefore.MinorUnits(), e.BalanceAfter.MinorUnits(), e.CreatedAt)
	return err
}

// InsertOutboxEvents stores each event envelope as an immutable JSON snapshot.
func (r *txRepo) InsertOutboxEvents(ctx context.Context, events []wagering.Event) error {
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		occurred, err := time.Parse(time.RFC3339, e.OccurredAt)
		if err != nil {
			return err
		}
		_, err = r.tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_id, payload, created_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $5)`,
			e.EventID, e.EventType, e.AggregateID, payload, occurred)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *txRepo) InsertInbox(ctx context.Context, rec app.InboxRecord) (bool, error) {
	tag, err := r.tx.Exec(ctx, `
		INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		rec.ConsumerName, rec.MessageID, rec.PayloadHash, rec.ReceivedAt, rec.ProcessedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (r *txRepo) GetInbox(ctx context.Context, consumer, messageID string) (app.InboxRecord, error) {
	var rec app.InboxRecord
	var txID *string
	err := r.tx.QueryRow(ctx, `
		SELECT consumer_name, message_id, payload_hash, transaction_id, received_at, processed_at
		FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).
		Scan(&rec.ConsumerName, &rec.MessageID, &rec.PayloadHash, &txID, &rec.ReceivedAt, &rec.ProcessedAt)
	rec.TransactionID = deref(txID)
	return rec, err
}

func (r *txRepo) SetInboxTransaction(ctx context.Context, consumer, messageID, transactionID string) error {
	_, err := r.tx.Exec(ctx, `
		UPDATE inbox_messages SET transaction_id = $3 WHERE consumer_name = $1 AND message_id = $2`,
		consumer, messageID, transactionID)
	return err
}
