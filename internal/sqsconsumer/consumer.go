// Package sqsconsumer consumes wager operations from the FIFO queue and calls
// the same use case as HTTP. Every message must carry an HMAC signature made
// by the trusted gateway with the key of the declared providerId (see
// package sqsauth); the signature is verified before the use case runs. A
// message is deleted only after the SQL transaction that recorded it in the
// inbox has committed.
package sqsconsumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"wagering/internal/app"
	"wagering/internal/observability"
	"wagering/internal/sqsauth"
)

// MessageType is the only accepted envelope type.
const MessageType = "WagerTransactionRequested"

// Envelope is the message body contract.
type Envelope struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt string    `json:"occurredAt"`
	Data       WagerData `json:"data"`
	// Signature is the lowercase hex HMAC-SHA-256 of the canonical business
	// fields (sqsauth.Canonical), made by the trusted gateway.
	Signature string `json:"signature,omitempty"`
}

// WagerData mirrors the HTTP body plus the idempotency key (HTTP sends it as a header).
type WagerData struct {
	IdempotencyKey                 string    `json:"idempotencyKey"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          MoneyData `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
	CorrelationID                  string    `json:"correlationId,omitempty"`
}

// MoneyData requires the amount as a JSON string; a JSON number fails decoding.
type MoneyData struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// Action is what to do with a message after handling it.
type Action int

const (
	// ActionDelete: committed (processed, rejected, pending reference or replay).
	ActionDelete Action = iota
	// ActionRetry: transient failure; make the message visible again after a backoff.
	ActionRetry
	// ActionDeadLetter: permanent failure; move to the DLQ and delete.
	ActionDeadLetter
)

func (a Action) String() string {
	return [...]string{"delete", "retry", "dead_letter"}[a]
}

// Processor is the use case the consumer calls (app.WagerService).
type Processor interface {
	Process(ctx context.Context, in app.WagerInput, inbox *app.InboxMessage) (app.WagerResult, error)
}

// Config of the consumer.
type Config struct {
	ConsumerName      string
	QueueURL          func() string
	DLQURL            func() string
	WaitSeconds       int32
	VisibilityTimeout int32
	MaxMessages       int32
	ProcessTimeout    time.Duration
	RetryBase         time.Duration
	RetryMax          time.Duration
	// Signatures verifies the gateway signature. Nil rejects every message.
	Signatures *sqsauth.Verifier
}

// Consumer polls one queue.
type Consumer struct {
	client  *sqs.Client
	proc    Processor
	cfg     Config
	metrics *observability.Metrics
	log     *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func New(client *sqs.Client, proc Processor, cfg Config, metrics *observability.Metrics, log *slog.Logger) *Consumer {
	return &Consumer{client: client, proc: proc, cfg: cfg, metrics: metrics, log: log.With("component", "sqs-consumer", "consumer", cfg.ConsumerName)}
}

// Start begins polling in a goroutine.
func (c *Consumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.done = make(chan struct{})
	go c.run(ctx)
	c.log.Info("sqs consumer started")
}

// Stop stops fetching, lets the in-flight message finish and returns when the
// loop exits or ctx expires. A message whose handling did not finish is not
// deleted, so it becomes visible again and is redelivered safely (inbox).
func (c *Consumer) Stop(ctx context.Context) error {
	if c.cancel == nil {
		return nil
	}
	c.once.Do(c.cancel)
	select {
	case <-c.done:
		c.log.Info("sqs consumer stopped")
		return nil
	case <-ctx.Done():
		c.log.Warn("sqs consumer stop timed out; in-flight message will be redelivered")
		return ctx.Err()
	}
}

func (c *Consumer) run(ctx context.Context) {
	defer close(c.done)
	for ctx.Err() == nil {
		msgs, err := c.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Warn("receive failed", "error", err.Error())
			sleep(ctx, time.Second)
			continue
		}
		for i, m := range msgs {
			if ctx.Err() != nil {
				// Shutting down: release messages we will not handle.
				c.release(msgs[i:])
				return
			}
			// Handling uses a context detached from shutdown, bounded by
			// ProcessTimeout (< visibility timeout), so a commit is never cut halfway.
			hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.ProcessTimeout)
			action := c.Handle(hctx, m)
			c.Apply(hctx, m, action)
			cancel()
		}
	}
}

// Receive long-polls one batch.
func (c *Consumer) Receive(ctx context.Context) ([]types.Message, error) {
	out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.cfg.QueueURL()),
		MaxNumberOfMessages: c.cfg.MaxMessages,
		WaitTimeSeconds:     c.cfg.WaitSeconds,
		VisibilityTimeout:   c.cfg.VisibilityTimeout,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameSentTimestamp,
			types.MessageSystemAttributeNameMessageGroupId,
		},
	})
	if err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// Handle decodes and processes one message and decides what to do with it.
// It never deletes; Apply does. Splitting them lets tests simulate a crash
// between commit and delete.
func (c *Consumer) Handle(ctx context.Context, m types.Message) Action {
	env, err := Decode(aws.ToString(m.Body))
	if err != nil {
		c.log.Warn("invalid message", "sqsMessageId", aws.ToString(m.MessageId), "error", err.Error())
		return ActionDeadLetter
	}
	log := c.log.With("messageId", env.MessageID, "providerId", env.Data.ProviderID, "walletId", env.Data.WalletID,
		"correlationId", correlationID(env))

	in := toInput(env)
	// Authenticate before the use case: a rejected message writes nothing
	// (no inbox row, transaction, ledger entry or event).
	req, err := app.BuildRequest(in)
	if err != nil {
		log.Warn("invalid message, sent to DLQ", "error", err.Error())
		return ActionDeadLetter
	}
	if err := c.cfg.Signatures.Verify(req, env.Signature); err != nil {
		c.metrics.SQSUnauthenticated.Inc()
		log.Warn("unauthenticated message, sent to DLQ", "error", err.Error())
		return ActionDeadLetter
	}

	res, err := c.proc.Process(ctx, in, &app.InboxMessage{
		ConsumerName: c.cfg.ConsumerName, MessageID: env.MessageID, ReceivedAt: time.Now().UTC(),
	})
	action := classify(err)
	switch action {
	case ActionDelete:
		log.Info("message committed", "transactionId", res.Transaction.ID, "status", res.Transaction.Status, "replay", res.Replay)
	case ActionRetry:
		log.Warn("transient failure, message will be retried", "error", err.Error(), "receiveCount", receiveCount(m))
	case ActionDeadLetter:
		log.Warn("permanent failure, message goes to DLQ", "error", err.Error())
	}
	return action
}

// classify maps use-case errors to message actions. Business rejections are
// committed results (err == nil) and are deleted. Unknown errors are retried;
// once maxReceiveCount is exceeded the SQS redrive policy moves them to the DLQ.
func classify(err error) Action {
	var invalid *app.InvalidInputError
	switch {
	case err == nil:
		return ActionDelete
	case errors.As(err, &invalid),
		errors.Is(err, app.ErrIdempotencyConflict),
		errors.Is(err, app.ErrDuplicateExternalTransaction),
		errors.Is(err, app.ErrInboxHashMismatch),
		errors.Is(err, app.ErrWalletNotFound):
		return ActionDeadLetter
	default:
		return ActionRetry
	}
}

// Apply performs the action against SQS.
func (c *Consumer) Apply(ctx context.Context, m types.Message, action Action) {
	c.metrics.SQSMessages.WithLabelValues(action.String()).Inc()
	switch action {
	case ActionDelete:
		c.delete(ctx, m)
	case ActionRetry:
		c.metrics.SQSRetries.Inc()
		delay := c.backoff(receiveCount(m))
		_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(c.cfg.QueueURL()), ReceiptHandle: m.ReceiptHandle,
			VisibilityTimeout: int32(delay / time.Second),
		})
		if err != nil {
			c.log.Warn("could not change visibility; default visibility timeout applies", "error", err.Error())
		}
	case ActionDeadLetter:
		if err := c.deadLetter(ctx, m); err != nil {
			// Leave the message; it will be redelivered and eventually redriven.
			c.log.Error("could not send to DLQ", "error", err.Error())
			return
		}
		c.metrics.SQSDeadLettered.Inc()
		c.delete(ctx, m)
	}
}

func (c *Consumer) delete(ctx context.Context, m types.Message) {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(c.cfg.QueueURL()), ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		// Safe: the redelivery hits the inbox and is answered as a replay.
		c.log.Warn("delete failed; message will be redelivered and deduplicated by the inbox", "error", err.Error())
	}
}

func (c *Consumer) deadLetter(ctx context.Context, m types.Message) error {
	group := "invalid"
	dedup := aws.ToString(m.MessageId)
	if g, ok := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]; ok && g != "" {
		group = g
	}
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(c.cfg.DLQURL()), MessageBody: m.Body,
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"sourceConsumer": {DataType: aws.String("String"), StringValue: aws.String(c.cfg.ConsumerName)},
		},
	})
	return err
}

// release makes unhandled messages visible again immediately.
func (c *Consumer) release(msgs []types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, m := range msgs {
		_, _ = c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(c.cfg.QueueURL()), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0,
		})
	}
}

func (c *Consumer) backoff(receiveCount int) time.Duration {
	d := c.cfg.RetryBase
	for i := 1; i < receiveCount && d < c.cfg.RetryMax; i++ {
		d *= 2
	}
	if d > c.cfg.RetryMax {
		d = c.cfg.RetryMax
	}
	if d < time.Second {
		d = time.Second
	}
	return d
}

// Decode strictly parses an envelope: unknown fields, numeric amounts, wrong
// type and missing messageId are rejected.
func Decode(body string) (Envelope, error) {
	var env Envelope
	dec := json.NewDecoder(bytes.NewReader([]byte(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	if dec.More() {
		return Envelope{}, errors.New("decode envelope: trailing data")
	}
	if env.MessageID == "" || len(env.MessageID) > 200 {
		return Envelope{}, errors.New("messageId is required (max 200 chars)")
	}
	if env.Type != MessageType {
		return Envelope{}, fmt.Errorf("unsupported type %q", env.Type)
	}
	if _, err := time.Parse(time.RFC3339, env.OccurredAt); err != nil {
		return Envelope{}, fmt.Errorf("occurredAt must be RFC3339: %w", err)
	}
	return env, nil
}

func toInput(env Envelope) app.WagerInput {
	return app.WagerInput{
		ProviderID: env.Data.ProviderID, ExternalTransactionID: env.Data.ExternalTransactionID,
		IdempotencyKey: env.Data.IdempotencyKey, PlayerID: env.Data.PlayerID, WalletID: env.Data.WalletID,
		RoundID: env.Data.RoundID, GameID: env.Data.GameID, Kind: env.Data.Kind,
		Amount: env.Data.Money.Amount, Currency: env.Data.Money.Currency,
		ReferenceExternalTransactionID: env.Data.ReferenceExternalTransactionID,
		CorrelationID:                  correlationID(env), CausationID: env.MessageID, Source: "sqs",
	}
}

// Sign sets env.Signature with key, exactly as the consumer verifies it. It is
// the reference implementation for the gateway (and is used by cmd/sqssign and
// the tests). It fails if the operation is not a valid request.
func Sign(env *Envelope, key []byte) error {
	req, err := app.BuildRequest(toInput(*env))
	if err != nil {
		return err
	}
	env.Signature = sqsauth.Sign(key, req)
	return nil
}

func correlationID(env Envelope) string {
	if env.Data.CorrelationID != "" {
		return env.Data.CorrelationID
	}
	return env.MessageID
}

func receiveCount(m types.Message) int {
	n, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
