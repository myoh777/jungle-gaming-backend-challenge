package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"wagering/internal/outbox"
)

// OutboxRepo implements outbox.Repository.
//
// Concurrency: Claim uses FOR UPDATE SKIP LOCKED inside a single UPDATE, so
// concurrent publishers never claim the same row at the same time. A claim is
// a lease (lease_owner, lease_expires_at); if a publisher dies after claiming,
// the row becomes claimable again when the lease expires. Confirmation is
// conditional on still owning the lease.
type OutboxRepo struct {
	pool *pgxpool.Pool
}

var _ outbox.Repository = (*OutboxRepo)(nil)

func NewOutboxRepo(pool *pgxpool.Pool) *OutboxRepo { return &OutboxRepo{pool: pool} }

func (r *OutboxRepo) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]outbox.Record, error) {
	rows, err := r.pool.Query(ctx, `
		UPDATE outbox_events
		SET lease_owner = $1,
		    lease_expires_at = now() + $2 * interval '1 millisecond',
		    attempts = attempts + 1
		WHERE id IN (
			SELECT id FROM outbox_events
			WHERE published_at IS NULL
			  AND next_attempt_at <= now()
			  AND (lease_expires_at IS NULL OR lease_expires_at < now())
			ORDER BY seq
			LIMIT $3
			FOR UPDATE SKIP LOCKED)
		RETURNING id, event_type, aggregate_id, payload::text, attempts, seq`,
		owner, lease.Milliseconds(), limit)
	if err != nil {
		return nil, classify(err)
	}
	recs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outbox.Record, error) {
		var rec outbox.Record
		var payload string
		err := row.Scan(&rec.EventID, &rec.EventType, &rec.AggregateID, &payload, &rec.Attempts, &rec.Seq)
		rec.Payload = []byte(payload)
		return rec, err
	})
	return recs, classify(err)
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, eventID, owner string) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = now(), lease_owner = NULL, lease_expires_at = NULL, last_error = NULL
		WHERE id = $1 AND lease_owner = $2 AND published_at IS NULL`, eventID, owner)
	if err != nil {
		return false, classify(err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *OutboxRepo) MarkFailed(ctx context.Context, eventID, owner, lastError string, retryAfter time.Duration) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET lease_owner = NULL, lease_expires_at = NULL, last_error = $3,
		    next_attempt_at = now() + $4 * interval '1 millisecond'
		WHERE id = $1 AND lease_owner = $2 AND published_at IS NULL`,
		eventID, owner, lastError, retryAfter.Milliseconds())
	return classify(err)
}

func (r *OutboxRepo) OldestPendingAge(ctx context.Context) (time.Duration, error) {
	var seconds float64
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM now() - MIN(created_at)), 0)::float8
		FROM outbox_events WHERE published_at IS NULL`).Scan(&seconds)
	return time.Duration(seconds * float64(time.Second)), classify(err)
}
