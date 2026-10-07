// Package postgres implements the app.Store port with pgx and explicit SQL.
//
// Transaction boundaries: every use case runs in one READ COMMITTED
// transaction opened by Store.InTx. Writers serialize per wallet with
// SELECT ... FOR NO KEY UPDATE on the wallet row (no global lock), and the
// balance UPDATE also checks the expected version as a second guard against
// lost updates. Uniqueness, non-negativity and ledger immutability are
// enforced by constraints and triggers (see migrations/).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"wagering/internal/app"
	"wagering/internal/domain/wagering"
	"wagering/internal/domain/wallet"
	"wagering/internal/money"
	"wagering/internal/observability"
)

// maxTxAttempts bounds retries of a whole SQL transaction after a retryable conflict.
const maxTxAttempts = 3

// lockTimeout bounds how long one statement waits for a row lock, so a stuck
// holder surfaces as a transient error instead of a hung request.
const lockTimeout = "5s"

// Store implements app.Store.
type Store struct {
	pool    *pgxpool.Pool
	metrics *observability.Metrics
	log     *slog.Logger
}

var _ app.Store = (*Store)(nil)

func NewStore(pool *pgxpool.Pool, metrics *observability.Metrics, log *slog.Logger) *Store {
	return &Store{pool: pool, metrics: metrics, log: log}
}

// InTx runs fn in a READ COMMITTED transaction and retries it, up to
// maxTxAttempts, after deadlocks, serialization failures, lock timeouts or
// optimistic version conflicts.
func (s *Store) InTx(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	var err error
	for attempt := 1; attempt <= maxTxAttempts; attempt++ {
		err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(ptx pgx.Tx) error {
			if _, err := ptx.Exec(ctx, "SET LOCAL lock_timeout = '"+lockTimeout+"'"); err != nil {
				return err
			}
			return fn(ctx, &txRepo{tx: ptx})
		})
		if err == nil || !isRetryable(err) || ctx.Err() != nil {
			break
		}
		s.metrics.ConcurrencyRetries.Inc()
		s.log.Debug("retrying sql transaction", "attempt", attempt, "error", err.Error())
	}
	return classify(err)
}

func isRetryable(err error) bool {
	if errors.Is(err, app.ErrConcurrentModification) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01", "55P03": // serialization_failure, deadlock_detected, lock_not_available
			return true
		}
	}
	return false
}

// classify wraps transient infrastructure errors with app.ErrUnavailable and
// leaves domain/app errors untouched.
func classify(err error) error {
	if err == nil || errors.Is(err, app.ErrUnavailable) {
		return err
	}
	if isRetryable(err) {
		return fmt.Errorf("%w: %v", app.ErrUnavailable, err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code[:2] {
		case "08", "53", "57": // connection exception, insufficient resources, operator intervention
			return fmt.Errorf("%w: %v", app.ErrUnavailable, err)
		}
		return err
	}
	var netErr net.Error
	if errors.As(err, &netErr) || pgconn.SafeToRetry(err) || pgconn.Timeout(err) ||
		errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", app.ErrUnavailable, err)
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return fmt.Errorf("%w: %v", app.ErrUnavailable, err)
	}
	return err
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && (constraint == "" || pgErr.ConstraintName == constraint)
}

// ---------- read-only queries (outside use-case transactions) ----------

func (s *Store) GetWallet(ctx context.Context, id string) (*wallet.Wallet, error) {
	w, err := scanWallet(s.pool.QueryRow(ctx, selectWallet+` WHERE id = $1`, id))
	return w, classify(err)
}

func (s *Store) ListLedger(ctx context.Context, walletID string, afterSeq int64, limit int) ([]app.LedgerItem, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq, id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at
		FROM wallet_ledger_entries
		WHERE wallet_id = $1 AND seq > $2
		ORDER BY seq
		LIMIT $3`, walletID, afterSeq, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []app.LedgerItem
	for rows.Next() {
		var (
			seq                   int64
			e                     wallet.LedgerEntry
			dir, currency         string
			amount, before, after int64
		)
		if err := rows.Scan(&seq, &e.ID, &e.WalletID, &e.TransactionID, &dir, &amount, &currency, &before, &after, &e.CreatedAt); err != nil {
			return nil, classify(err)
		}
		c := money.Currency(currency)
		if e.Amount, err = money.New(amount, c); err != nil {
			return nil, err
		}
		if e.BalanceBefore, err = money.New(before, c); err != nil {
			return nil, err
		}
		if e.BalanceAfter, err = money.New(after, c); err != nil {
			return nil, err
		}
		e.Direction = wallet.Direction(dir)
		e.CreatedAt = e.CreatedAt.UTC()
		out = append(out, app.LedgerItem{Seq: seq, Entry: e})
	}
	return out, classify(rows.Err())
}

func (s *Store) GetTransaction(ctx context.Context, id string) (*wagering.Transaction, error) {
	t, err := scanTransaction(s.pool.QueryRow(ctx, selectTransaction+` WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, classify(err)
}

func (s *Store) GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error) {
	t, err := scanTransaction(s.pool.QueryRow(ctx, selectTransaction+` WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, classify(err)
}

func (s *Store) ListDueReferencePending(ctx context.Context, now time.Time, limit int) ([]app.PendingRef, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, wallet_id FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_reference_attempt_at <= $1
		ORDER BY next_reference_attempt_at
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, classify(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (app.PendingRef, error) {
		var p app.PendingRef
		err := r.Scan(&p.TransactionID, &p.WalletID)
		return p, err
	})
	return out, classify(err)
}

// ReconciliationSnapshot reads the wallet and the ledger aggregate in one
// REPEATABLE READ, read-only transaction so both see the same snapshot.
// SUM(...)::bigint raises an error instead of silently overflowing.
func (s *Store) ReconciliationSnapshot(ctx context.Context, walletID string) (app.ReconciliationData, error) {
	var out app.ReconciliationData
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		w, err := scanWallet(tx.QueryRow(ctx, selectWallet+` WHERE id = $1`, walletID))
		if err != nil {
			return err
		}
		out.StoredBalance = w.Balance()
		return tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)::bigint, COUNT(*)
			FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&out.LedgerNet, &out.CheckedEntries)
	})
	return out, classify(err)
}

// ---------- scanning helpers ----------

const selectWallet = `SELECT id, player_id, currency, balance_amount, version, created_at, updated_at FROM wallets`

func scanWallet(row pgx.Row) (*wallet.Wallet, error) {
	var (
		id, playerID, currency string
		balance, version       int64
		createdAt, updatedAt   time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balance, &version, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrWalletNotFound
		}
		return nil, err
	}
	b, err := money.New(balance, money.Currency(currency))
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(id, playerID, b, version, createdAt.UTC(), updatedAt.UTC())
}

const selectTransaction = `
	SELECT id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
	       wallet_id, player_id, round_id, game_id, kind, amount, currency,
	       reference_external_transaction_id, reference_transaction_id, status, failure_code,
	       result_balance_amount, result_wallet_version, reference_attempts, next_reference_attempt_at,
	       correlation_id, created_at, updated_at
	FROM wager_transactions`

func scanTransaction(row pgx.Row) (*wagering.Transaction, error) {
	var (
		s                                                      wagering.Snapshot
		origin, kind, status, currency                         string
		providerID, externalID, idemKey, hash, roundID, gameID *string
		refExt, refID, failureCode, correlationID              *string
		amount                                                 int64
		resultBalance, resultVersion                           *int64
		nextAttempt                                            *time.Time
	)
	err := row.Scan(&s.ID, &origin, &providerID, &externalID, &idemKey, &hash,
		&s.WalletID, &s.PlayerID, &roundID, &gameID, &kind, &amount, &currency,
		&refExt, &refID, &status, &failureCode,
		&resultBalance, &resultVersion, &s.ReferenceAttempts, &nextAttempt,
		&correlationID, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return nil, err
	}
	s.Origin, s.Kind, s.Status = wagering.Origin(origin), wagering.Kind(kind), wagering.Status(status)
	s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey = deref(providerID), deref(externalID), deref(idemKey)
	s.PayloadHash, s.RoundID, s.GameID = deref(hash), deref(roundID), deref(gameID)
	s.ReferenceExternalTransactionID, s.ReferenceTransactionID = deref(refExt), deref(refID)
	s.FailureCode, s.CorrelationID = wagering.FailureCode(deref(failureCode)), deref(correlationID)
	c := money.Currency(currency)
	if s.Money, err = money.New(amount, c); err != nil {
		return nil, err
	}
	if resultBalance != nil {
		b, err := money.New(*resultBalance, c)
		if err != nil {
			return nil, err
		}
		s.ResultBalance = &b
	}
	s.ResultWalletVersion = resultVersion
	if nextAttempt != nil {
		n := nextAttempt.UTC()
		s.NextReferenceAttemptAt = &n
	}
	s.CreatedAt, s.UpdatedAt = s.CreatedAt.UTC(), s.UpdatedAt.UTC()
	return wagering.Rehydrate(s)
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
