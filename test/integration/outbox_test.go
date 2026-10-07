//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"wagering/internal/observability"
	"wagering/internal/outbox"
	"wagering/internal/postgres"
	"wagering/internal/sqsx"
)

func newPublisher(q testQueues, owner string, lease time.Duration) *outbox.Publisher {
	sender := sqsx.NewEventSender(sqsClient, func() string { return q.EventsURL })
	return outbox.NewPublisher(postgres.NewOutboxRepo(db), sender, outbox.Config{
		Owner: owner, PollInterval: 50 * time.Millisecond, BatchSize: 20, Lease: lease,
		RetryBase: 100 * time.Millisecond, RetryMax: time.Second,
	}, observability.NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// drainEvents reads every message from the events queue, keyed by eventId.
func drainEvents(t *testing.T, q testQueues, until func(map[string]int) bool) map[string]int {
	t.Helper()
	seen := map[string]int{}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && !until(seen) {
		out, err := sqsClient.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(q.EventsURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
		})
		must(t, err)
		for _, m := range out.Messages {
			var env struct {
				EventID string `json:"eventId"`
			}
			must(t, json.Unmarshal([]byte(aws.ToString(m.Body)), &env))
			seen[env.EventID]++
			_, _ = sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(q.EventsURL), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return seen
}

func walletEventIDs(t *testing.T, walletIDs []string) []string {
	t.Helper()
	rows, err := db.Query(context.Background(), `
		SELECT id::text FROM outbox_events
		WHERE aggregate_id = ANY($1) OR payload->'data'->>'walletId' = ANY($1)`, walletIDs)
	must(t, err)
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		must(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	return ids
}

func unpublished(t *testing.T, ids []string) int {
	return countRows(t, `SELECT count(*) FROM outbox_events WHERE id::text = ANY($1) AND published_at IS NULL`, ids)
}

// TestOutboxCommittedBeforePublishAndTwoPublishers: events committed while no
// publisher runs (crash between commit and publish) are published later; two
// publishers draining concurrently publish every event exactly once.
func TestOutboxCommittedBeforePublishAndTwoPublishers(t *testing.T) {
	q := createQueues(t, 5)
	a := startApp(t, baseConfig(t, q)) // publisher disabled
	var walletIDs []string
	for i := 0; i < 15; i++ {
		walletIDs = append(walletIDs, createWallet(t, a.base, "player-"+newID(), "10.00"))
	}
	ids := walletEventIDs(t, walletIDs)
	if len(ids) != 30 || unpublished(t, ids) != 30 {
		t.Fatalf("expected 30 committed, unpublished events; got %d / %d", len(ids), unpublished(t, ids))
	}

	p1, p2 := newPublisher(q, "pub-1", 30*time.Second), newPublisher(q, "pub-2", 30*time.Second)
	p1.Start()
	p2.Start()
	defer p1.Stop(context.Background()) //nolint:errcheck
	defer p2.Stop(context.Background()) //nolint:errcheck

	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	seen := drainEvents(t, q, func(s map[string]int) bool {
		for id := range want {
			if s[id] == 0 {
				return false
			}
		}
		return true
	})
	for id := range want {
		if seen[id] != 1 {
			t.Fatalf("event %s published %d times", id, seen[id])
		}
	}
	eventually(t, 10*time.Second, func() bool { return unpublished(t, ids) == 0 }, "all confirmed")
	owners := countRows(t, `SELECT count(DISTINCT lease_owner) FROM outbox_events WHERE id::text = ANY($1)`, ids)
	if owners != 0 {
		t.Fatalf("leases must be released after confirmation, %d owners remain", owners)
	}
}

// TestOutboxCrashBetweenPublishAndConfirm: a publisher claims and sends but dies
// before confirming. After the lease expires another publisher republishes the
// same eventId; consumers deduplicate by eventId.
func TestOutboxCrashBetweenPublishAndConfirm(t *testing.T) {
	q := createQueues(t, 5)
	a := startApp(t, baseConfig(t, q))
	walletID := createWallet(t, a.base, "player-"+newID(), "10.00")
	ids := walletEventIDs(t, []string{walletID})
	if len(ids) != 2 {
		t.Fatalf("expected 2 events, got %d", len(ids))
	}

	// The "crashed" publisher: claim with a 1s lease and send, never confirm.
	// Other tests' leftovers may be claimed too; they are published as well.
	repo := postgres.NewOutboxRepo(db)
	sender := sqsx.NewEventSender(sqsClient, func() string { return q.EventsURL })
	sentByCrashed := map[string]bool{}
	for i := 0; i < 50 && len(sentByCrashed) < 2; i++ {
		recs, err := repo.Claim(context.Background(), "crashed", time.Second, 100)
		must(t, err)
		for _, r := range recs {
			must(t, sender.Send(context.Background(), r.EventID, r.EventType, r.Payload))
			for _, id := range ids {
				if id == r.EventID {
					sentByCrashed[id] = true
				}
			}
		}
		if len(recs) == 0 {
			break
		}
	}
	if len(sentByCrashed) != 2 {
		t.Fatalf("crashed publisher sent %d of our events", len(sentByCrashed))
	}
	if unpublished(t, ids) != 2 {
		t.Fatal("events must remain unconfirmed")
	}

	time.Sleep(1500 * time.Millisecond) // lease expires
	p := newPublisher(q, "recovery", 30*time.Second)
	p.Start()
	defer p.Stop(context.Background()) //nolint:errcheck

	seen := drainEvents(t, q, func(s map[string]int) bool { return s[ids[0]] >= 2 && s[ids[1]] >= 2 })
	for _, id := range ids {
		if seen[id] != 2 {
			t.Fatalf("event %s delivered %d times, expected 2 (original + republication with same eventId)", id, seen[id])
		}
	}
	eventually(t, 10*time.Second, func() bool { return unpublished(t, ids) == 0 }, "confirmed after recovery")
}

// TestOutboxRetriesWhenBrokerFails: sends to a missing queue fail, are
// rescheduled with backoff and succeed once the queue exists.
func TestOutboxRetriesWhenBrokerFails(t *testing.T) {
	q := createQueues(t, 5)
	a := startApp(t, baseConfig(t, q))
	walletID := createWallet(t, a.base, "player-"+newID(), "10.00")
	ids := walletEventIDs(t, []string{walletID})

	var mu sync.Mutex
	target := q.EventsURL + "-missing"
	sender := sqsx.NewEventSender(sqsClient, func() string { mu.Lock(); defer mu.Unlock(); return target })
	p := outbox.NewPublisher(postgres.NewOutboxRepo(db), sender, outbox.Config{
		Owner: "flaky", PollInterval: 50 * time.Millisecond, BatchSize: 50, Lease: 30 * time.Second,
		RetryBase: 100 * time.Millisecond, RetryMax: 300 * time.Millisecond,
	}, observability.NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.Start()
	defer p.Stop(context.Background()) //nolint:errcheck

	eventually(t, 10*time.Second, func() bool {
		return countRows(t, `SELECT count(*) FROM outbox_events WHERE id::text = ANY($1) AND attempts >= 2 AND last_error IS NOT NULL`, ids) == 2
	}, "failed attempts recorded")
	mu.Lock()
	target = q.EventsURL
	mu.Unlock()
	eventually(t, 10*time.Second, func() bool { return unpublished(t, ids) == 0 }, "published after broker recovers")
}
