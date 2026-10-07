//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/fx"

	"wagering/internal/app"
	"wagering/internal/observability"
	"wagering/internal/sqsconsumer"
)

func envelope(messageID, key string, w wager) string {
	data := w.body()
	data["idempotencyKey"] = key
	raw, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": sqsconsumer.MessageType,
		"occurredAt": time.Now().UTC().Format(time.RFC3339), "data": data,
	})
	return string(raw)
}

// sendRaw publishes with an explicit deduplication id; tests use a fresh one
// per send so that SQS FIFO deduplication cannot hide duplicates and the
// application inbox is what deduplicates.
func sendRaw(t *testing.T, queueURL, body, group string) {
	t.Helper()
	_, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(newID()),
	})
	must(t, err)
}

func queueDepth(t *testing.T, url string) int {
	t.Helper()
	out, err := sqsClient.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	must(t, err)
	a, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	b, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return a + b
}

func txStatusByExternal(t *testing.T, ext string) string {
	t.Helper()
	var status string
	err := db.QueryRow(context.Background(), `SELECT status FROM wager_transactions WHERE provider_id = 'alpha' AND external_transaction_id = $1`, ext).Scan(&status)
	if err != nil {
		return ""
	}
	return status
}

func TestSQSConsumerProcessesAndDeduplicatesByInbox(t *testing.T) {
	q := createQueues(t, 5)
	cfg := baseConfig(t, q)
	cfg.ConsumerEnabled = true
	a := startApp(t, cfg)
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")

	ext, msgID := newID(), newID()
	body := envelope(msgID, newID(), wager{ProviderID: "alpha", ExternalID: ext, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "15.00"})
	// The same message delivered three times (distinct SQS dedup ids).
	for i := 0; i < 3; i++ {
		sendRaw(t, q.WagerURL, body, walletID)
	}
	eventually(t, 30*time.Second, func() bool { return txStatusByExternal(t, ext) == "PROCESSED" && queueDepth(t, q.WagerURL) == 0 }, "message processed and deleted")

	if n := countRows(t, `SELECT count(*) FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2 AND transaction_id IS NOT NULL`, cfg.ConsumerName, msgID); n != 1 {
		t.Fatalf("inbox rows = %d", n)
	}
	if bal, _ := walletBalance(t, walletID); bal != 8500 {
		t.Fatalf("balance %d: duplicates were applied", bal)
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 1 {
		t.Fatalf("debits = %d", n)
	}

	// Same messageId with a different payload is a poison message -> DLQ.
	tampered := envelope(msgID, newID(), wager{ProviderID: "alpha", ExternalID: ext, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "99.00"})
	sendRaw(t, q.WagerURL, tampered, walletID)
	eventually(t, 30*time.Second, func() bool { return queueDepth(t, q.DLQURL) == 1 && queueDepth(t, q.WagerURL) == 0 }, "tampered redelivery in DLQ")
	if bal, _ := walletBalance(t, walletID); bal != 8500 {
		t.Fatal("tampered message changed the balance")
	}
}

// TestSQSCrashAfterCommitBeforeDelete: one consumer commits but "dies" before
// deleting; another instance gets the redelivery and only deletes it.
func TestSQSCrashAfterCommitBeforeDelete(t *testing.T) {
	q := createQueues(t, 5)
	var consumer *sqsconsumer.Consumer
	first := startApp(t, baseConfig(t, q), fx.Populate(&consumer))
	player := "player-" + newID()
	walletID := createWallet(t, first.base, player, "100.00")
	ext := newID()
	sendRaw(t, q.WagerURL, envelope(newID(), newID(), wager{ProviderID: "alpha", ExternalID: ext, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "40.00"}), walletID)

	var msgs []types.Message
	eventually(t, 15*time.Second, func() bool {
		var err error
		msgs, err = consumer.Receive(context.Background())
		return err == nil && len(msgs) == 1
	}, "receive message")
	if action := consumer.Handle(context.Background(), msgs[0]); action != sqsconsumer.ActionDelete {
		t.Fatalf("expected commit, got %s", action)
	}
	// Crash: no Apply/delete. The effect is committed, the message is still in flight.
	first.stop(t)
	if txStatusByExternal(t, ext) != "PROCESSED" || queueDepth(t, q.WagerURL) != 1 {
		t.Fatal("expected committed transaction and undeleted message")
	}

	cfg := baseConfig(t, q)
	cfg.ConsumerEnabled = true
	startApp(t, cfg)
	eventually(t, 30*time.Second, func() bool { return queueDepth(t, q.WagerURL) == 0 }, "redelivered message deleted")
	if bal, _ := walletBalance(t, walletID); bal != 6000 {
		t.Fatalf("redelivery reapplied the bet: balance %d", bal)
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
}

func TestSQSPermanentFailuresGoToDLQ(t *testing.T) {
	q := createQueues(t, 5)
	cfg := baseConfig(t, q)
	cfg.ConsumerEnabled = true
	a := startApp(t, cfg)
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")

	sendRaw(t, q.WagerURL, `{"not":"an envelope"}`, "g1")
	sendRaw(t, q.WagerURL, envelope(newID(), newID(), wager{ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "OPENING", Amount: "5.00"}), walletID)
	sendRaw(t, q.WagerURL, envelope(newID(), newID(), wager{ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: newID(), Kind: "BET", Amount: "5.00"}), "g2")
	// A business rejection is a committed result: it is deleted, not dead-lettered.
	rejected := newID()
	sendRaw(t, q.WagerURL, envelope(newID(), newID(), wager{ProviderID: "alpha", ExternalID: rejected, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "500.00"}), walletID)

	eventually(t, 30*time.Second, func() bool { return queueDepth(t, q.DLQURL) == 3 && queueDepth(t, q.WagerURL) == 0 }, "3 messages in DLQ")
	if txStatusByExternal(t, rejected) != "REJECTED" {
		t.Fatal("business rejection must be persisted")
	}
	if bal, _ := walletBalance(t, walletID); bal != 10000 {
		t.Fatalf("balance changed: %d", bal)
	}
}

// flakyProcessor simulates a transient infrastructure failure for the first
// `failures` calls and then delegates to the real use case (real PostgreSQL).
type flakyProcessor struct {
	failures int32
	calls    atomic.Int32
	next     sqsconsumer.Processor
}

func (f *flakyProcessor) Process(ctx context.Context, in app.WagerInput, inbox *app.InboxMessage) (app.WagerResult, error) {
	if f.calls.Add(1) <= f.failures {
		return app.WagerResult{}, app.ErrUnavailable
	}
	return f.next.Process(ctx, in, inbox)
}

func manualConsumer(q testQueues, proc sqsconsumer.Processor) *sqsconsumer.Consumer {
	return sqsconsumer.New(sqsClient, proc, sqsconsumer.Config{
		ConsumerName: "it-consumer", QueueURL: func() string { return q.WagerURL }, DLQURL: func() string { return q.DLQURL },
		WaitSeconds: 1, VisibilityTimeout: 5, MaxMessages: 10, ProcessTimeout: 4 * time.Second,
		RetryBase: time.Second, RetryMax: time.Second,
	}, observability.NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestSQSTransientRetriesThenSuccess(t *testing.T) {
	q := createQueues(t, 5)
	var svc *app.WagerService
	a := startApp(t, baseConfig(t, q), fx.Populate(&svc))
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")
	ext := newID()
	sendRaw(t, q.WagerURL, envelope(newID(), newID(), wager{ProviderID: "alpha", ExternalID: ext, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "1.00"}), walletID)

	proc := &flakyProcessor{failures: 2, next: svc}
	c := manualConsumer(q, proc)
	c.Start()
	defer c.Stop(context.Background()) //nolint:errcheck
	eventually(t, 30*time.Second, func() bool { return txStatusByExternal(t, ext) == "PROCESSED" && queueDepth(t, q.WagerURL) == 0 }, "processed after retries")
	if proc.calls.Load() != 3 {
		t.Fatalf("expected 2 failures + 1 success, got %d calls", proc.calls.Load())
	}
	if bal, _ := walletBalance(t, walletID); bal != 9900 {
		t.Fatalf("balance %d", bal)
	}
}

func TestSQSTransientRetriesExhaustToDLQ(t *testing.T) {
	q := createQueues(t, 2) // redrive after 2 receives
	sendRaw(t, q.WagerURL, envelope(newID(), newID(), wager{ProviderID: "alpha", ExternalID: newID(), PlayerID: "p", WalletID: newID(), Kind: "BET", Amount: "1.00"}), "g")
	proc := &flakyProcessor{failures: 1 << 30}
	c := manualConsumer(q, proc)
	c.Start()
	defer c.Stop(context.Background()) //nolint:errcheck
	eventually(t, 40*time.Second, func() bool { return queueDepth(t, q.DLQURL) == 1 && queueDepth(t, q.WagerURL) == 0 }, "redrive to DLQ")
	if proc.calls.Load() < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", proc.calls.Load())
	}
}

// TestHTTPAndSQSSameOperation sends one operation through both entry points at
// the same time with the same Idempotency-Key: one effect, one transaction.
func TestHTTPAndSQSSameOperation(t *testing.T) {
	q := createQueues(t, 5)
	cfg := baseConfig(t, q)
	cfg.ConsumerEnabled = true
	a := startApp(t, cfg)
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")

	for i := 0; i < 5; i++ {
		key, ext := newID(), newID()
		w := wager{ProviderID: "alpha", ExternalID: ext, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "2.00"}
		var wg sync.WaitGroup
		var httpResp response
		wg.Add(2)
		go func() { defer wg.Done(); sendRaw(t, q.WagerURL, envelope(newID(), key, w), walletID) }()
		go func() { defer wg.Done(); httpResp = postWager(t, a.base, "provider-alpha", key, w) }()
		wg.Wait()
		if httpResp.Status != 200 || httpResp.Body["status"] != "PROCESSED" {
			t.Fatalf("http: %d %s", httpResp.Status, httpResp.Raw)
		}
		eventually(t, 30*time.Second, func() bool { return queueDepth(t, q.WagerURL) == 0 }, "sqs message handled")
		if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE provider_id = 'alpha' AND external_transaction_id = $1`, ext); n != 1 {
			t.Fatalf("transactions for %s = %d", ext, n)
		}
	}
	if bal, _ := walletBalance(t, walletID); bal != 9000 {
		t.Fatalf("balance %d, expected 5 debits of 2.00", bal)
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 5 {
		t.Fatalf("debits = %d", n)
	}
}
