//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"wagering/internal/postgres"
	"wagering/migrations"
)

// TestMigrationsUpDownUp applies, reverts and re-applies the migrations on a
// throwaway database.
func TestMigrationsUpDownUp(t *testing.T) {
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, env.AdminDatabaseURL)
	must(t, err)
	defer admin.Close()
	name := "wagering_mig_" + strings.ReplaceAll(newID()[:8], "-", "")
	_, err = admin.Exec(ctx, "CREATE DATABASE "+name)
	must(t, err)
	defer admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") //nolint:errcheck

	url := strings.Replace(env.AdminDatabaseURL, "/postgres?", "/"+name+"?", 1)
	pool, err := pgxpool.New(ctx, url)
	must(t, err)
	defer pool.Close()

	tables := func() int {
		return countOn(t, pool, `SELECT count(*) FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name IN ('wallets','wager_transactions','wallet_ledger_entries','inbox_messages','outbox_events')`)
	}
	applied, err := postgres.MigrateUp(ctx, pool, migrations.FS)
	must(t, err)
	if len(applied) == 0 || tables() != 5 {
		t.Fatalf("up: applied=%v tables=%d", applied, tables())
	}
	again, err := postgres.MigrateUp(ctx, pool, migrations.FS)
	must(t, err)
	if len(again) != 0 {
		t.Fatalf("second up must be a no-op, applied %v", again)
	}
	reverted, err := postgres.MigrateDown(ctx, pool, migrations.FS, 1)
	must(t, err)
	if len(reverted) != 1 || tables() != 0 {
		t.Fatalf("down: reverted=%v tables=%d", reverted, tables())
	}
	if _, err := postgres.MigrateUp(ctx, pool, migrations.FS); err != nil || tables() != 5 {
		t.Fatalf("re-up: %v tables=%d", err, tables())
	}
}

func countOn(t *testing.T, pool *pgxpool.Pool, q string) int {
	t.Helper()
	var n int
	must(t, pool.QueryRow(context.Background(), q).Scan(&n))
	return n
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// TestDatabaseConstraints proves the invariants hold even for writes that
// bypass the application.
func TestDatabaseConstraints(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	walletID, player := newID(), "player-"+newID()
	_, err := db.Exec(ctx, `INSERT INTO wallets VALUES ($1, $2, 'BRL', 10000, 1, $3, $3)`, walletID, player, now)
	must(t, err)
	openingID := newID()
	_, err = db.Exec(ctx, `INSERT INTO wager_transactions (id, origin, wallet_id, player_id, kind, amount, currency, status,
		result_balance_amount, result_wallet_version, created_at, updated_at)
		VALUES ($1, 'INTERNAL', $2, $3, 'OPENING', 10000, 'BRL', 'PROCESSED', 10000, 1, $4, $4)`, openingID, walletID, player, now)
	must(t, err)
	_, err = db.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
		VALUES ($1, $2, $3, 'CREDIT', 10000, 'BRL', 0, 10000, $4)`, newID(), walletID, openingID, now)
	must(t, err)

	cases := []struct {
		name, sql string
		args      []any
		code      string
	}{
		{"negative balance", `UPDATE wallets SET balance_amount = -1 WHERE id = $1`, []any{walletID}, "23514"},
		{"duplicate wallet per player/currency", `INSERT INTO wallets VALUES ($1, $2, 'BRL', 0, 1, now(), now())`, []any{newID(), player}, "23505"},
		{"version below 1", `UPDATE wallets SET version = 0 WHERE id = $1`, []any{walletID}, "23514"},
		{"ledger update", `UPDATE wallet_ledger_entries SET amount = 1 WHERE wallet_id = $1`, []any{walletID}, "42501"},
		{"ledger delete", `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, []any{walletID}, "42501"},
		{"ledger truncate", `TRUNCATE wallet_ledger_entries`, nil, "42501"},
		{"duplicate ledger entry", `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
			VALUES ($1, $2, $3, 'CREDIT', 1, 'BRL', 10000, 10001, now())`, []any{newID(), walletID, openingID}, "23505"},
		{"second opening", `INSERT INTO wager_transactions (id, origin, wallet_id, player_id, kind, amount, currency, status,
			result_balance_amount, result_wallet_version, created_at, updated_at)
			VALUES ($1, 'INTERNAL', $2, $3, 'OPENING', 5, 'BRL', 'PROCESSED', 5, 1, now(), now())`, []any{newID(), walletID, player}, "23505"},
		{"external opening", `INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount, currency, status, failure_code, created_at, updated_at)
			VALUES ($1::uuid, 'EXTERNAL', 'alpha', $1::uuid::text, $1::uuid::text, 'h', $2, $3, 'r', 'g', 'OPENING', 5, 'BRL', 'REJECTED', 'X', now(), now())`, []any{newID(), walletID, player}, "23514"},
		{"loss with amount", `INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount, currency, status, failure_code, created_at, updated_at)
			VALUES ($1::uuid, 'EXTERNAL', 'alpha', $1::uuid::text, $1::uuid::text, 'h', $2, $3, 'r', 'g', 'LOSS', 5, 'BRL', 'REJECTED', 'X', now(), now())`, []any{newID(), walletID, player}, "23514"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tc.sql, tc.args...)
			if got := pgCode(err); got != tc.code {
				t.Fatalf("expected SQLSTATE %s, got %q (%v)", tc.code, got, err)
			}
		})
	}

	t.Run("ledger balance math", func(t *testing.T) {
		betID := newID()
		_, err := db.Exec(ctx, `INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount, currency, status, result_balance_amount, result_wallet_version, created_at, updated_at)
			VALUES ($1::uuid, 'EXTERNAL', 'alpha', $1::uuid::text, $1::uuid::text, 'h', $2, $3, 'r', 'g', 'BET', 100, 'BRL', 'PROCESSED', 9900, 2, now(), now())`, betID, walletID, player)
		must(t, err)
		_, err = db.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
			VALUES ($1, $2, $3, 'DEBIT', 100, 'BRL', 10000, 9999, now())`, newID(), walletID, betID)
		if pgCode(err) != "23514" {
			t.Fatalf("wrong balance_after must violate check, got %v", err)
		}

		// Two successful reversals of the same reference are impossible.
		insertRefund := func(status string) error {
			id := newID()
			_, err := db.Exec(ctx, `INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
				wallet_id, player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id, reference_transaction_id,
				status, result_balance_amount, result_wallet_version, created_at, updated_at)
				VALUES ($1::uuid, 'EXTERNAL', 'alpha', $1::uuid::text, $1::uuid::text, 'h', $2, $3, 'r', 'g', 'REFUND', 100, 'BRL', $4::uuid::text, $4::uuid, $5, 10000, 3, now(), now())`,
				id, walletID, player, betID, status)
			return err
		}
		must(t, insertRefund("PROCESSED"))
		if err := insertRefund("PROCESSED"); pgCode(err) != "23505" {
			t.Fatalf("second successful reversal must violate unique index, got %v", err)
		}
	})
}
