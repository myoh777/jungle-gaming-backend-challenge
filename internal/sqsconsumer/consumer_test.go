package sqsconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"wagering/internal/app"
	"wagering/internal/observability"
	"wagering/internal/sqsauth"
)

func TestDecode(t *testing.T) {
	valid := `{"messageId":"m1","type":"WagerTransactionRequested","occurredAt":"2026-01-01T00:00:00Z",
		"data":{"idempotencyKey":"k","providerId":"alpha","externalTransactionId":"e","playerId":"p",
		"walletId":"w","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}}`
	env, err := Decode(valid)
	if err != nil || env.MessageID != "m1" || env.Data.Money.Amount != "1.00" {
		t.Fatalf("valid envelope: %+v %v", env, err)
	}
	bad := map[string]string{
		"not json":       `{`,
		"numeric amount": `{"messageId":"m1","type":"WagerTransactionRequested","occurredAt":"2026-01-01T00:00:00Z","data":{"money":{"amount":1.0,"currency":"BRL"}}}`,
		"unknown field":  `{"messageId":"m1","type":"WagerTransactionRequested","occurredAt":"2026-01-01T00:00:00Z","data":{},"x":1}`,
		"wrong type":     `{"messageId":"m1","type":"Other","occurredAt":"2026-01-01T00:00:00Z","data":{}}`,
		"no message id":  `{"type":"WagerTransactionRequested","occurredAt":"2026-01-01T00:00:00Z","data":{}}`,
		"bad occurredAt": `{"messageId":"m1","type":"WagerTransactionRequested","occurredAt":"yesterday","data":{}}`,
		"trailing data":  `{"messageId":"m1","type":"WagerTransactionRequested","occurredAt":"2026-01-01T00:00:00Z","data":{}} {}`,
	}
	for name, body := range bad {
		if _, err := Decode(body); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestClassify(t *testing.T) {
	cases := map[error]Action{
		nil:                                      ActionDelete,
		&app.InvalidInputError{Field: "x"}:       ActionDeadLetter,
		app.ErrIdempotencyConflict:               ActionDeadLetter,
		app.ErrDuplicateExternalTransaction:      ActionDeadLetter,
		app.ErrInboxHashMismatch:                 ActionDeadLetter,
		app.ErrWalletNotFound:                    ActionDeadLetter,
		fmt.Errorf("%w: db", app.ErrUnavailable): ActionRetry,
		errors.New("unknown"):                    ActionRetry,
	}
	for err, want := range cases {
		if got := classify(err); got != want {
			t.Fatalf("%v: got %s want %s", err, got, want)
		}
	}
}

func TestBackoff(t *testing.T) {
	c := &Consumer{cfg: Config{RetryBase: 2 * time.Second, RetryMax: 10 * time.Second}}
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := c.backoff(i + 1); got != w {
			t.Fatalf("receive %d: got %v want %v", i+1, got, w)
		}
	}
}

// recordingProcessor counts calls; the consumer must not reach it for
// unauthenticated messages.
type recordingProcessor struct{ calls int }

func (p *recordingProcessor) Process(context.Context, app.WagerInput, *app.InboxMessage) (app.WagerResult, error) {
	p.calls++
	return app.WagerResult{}, nil
}

var (
	alphaKey = []byte("LOCAL-ONLY-FAKE-KEY-provider-alpha-do-not-use")
	betaKey  = []byte("LOCAL-ONLY-FAKE-KEY-provider-beta-do-not-use!")
)

func testEnvelope() Envelope {
	return Envelope{
		MessageID: "m1", Type: MessageType, OccurredAt: "2026-01-01T00:00:00Z",
		Data: WagerData{
			IdempotencyKey: "k1", ProviderID: "alpha", ExternalTransactionID: "e1", PlayerID: "p1",
			WalletID: "01a11822-22dc-72cd-be06-541e1384c17e", RoundID: "r1", GameID: "g1", Kind: "BET",
			Money: MoneyData{Amount: "25.5", Currency: "BRL"},
		},
	}
}

func message(t *testing.T, env Envelope) types.Message {
	t.Helper()
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return types.Message{Body: aws.String(string(raw)), MessageId: aws.String("sqs-1")}
}

func signed(t *testing.T, env Envelope, key []byte) Envelope {
	t.Helper()
	if err := Sign(&env, key); err != nil {
		t.Fatal(err)
	}
	return env
}

func TestHandleVerifiesSignatureBeforeProcessing(t *testing.T) {
	verifier, err := sqsauth.NewVerifier(sqsauth.Keys{"alpha": alphaKey, "beta": betaKey})
	if err != nil {
		t.Fatal(err)
	}
	newConsumer := func(v *sqsauth.Verifier) (*Consumer, *recordingProcessor) {
		p := &recordingProcessor{}
		return New(nil, p, Config{ConsumerName: "c", Signatures: v}, observability.NewMetrics(),
			slog.New(slog.NewTextHandler(io.Discard, nil))), p
	}

	t.Run("valid signature is processed", func(t *testing.T) {
		c, p := newConsumer(verifier)
		if a := c.Handle(context.Background(), message(t, signed(t, testEnvelope(), alphaKey))); a != ActionDelete || p.calls != 1 {
			t.Fatalf("action %s, calls %d", a, p.calls)
		}
	})
	t.Run("amount is signed in normalized form", func(t *testing.T) {
		env := signed(t, testEnvelope(), alphaKey) // signed with "25.5"
		env.Data.Money.Amount = "25.50"
		c, p := newConsumer(verifier)
		if a := c.Handle(context.Background(), message(t, env)); a != ActionDelete || p.calls != 1 {
			t.Fatalf("equivalent amount rejected: action %s, calls %d", a, p.calls)
		}
	})

	rejected := map[string]func() Envelope{
		"missing signature": testEnvelope,
		"malformed signature": func() Envelope {
			env := testEnvelope()
			env.Signature = "not-hex"
			return env
		},
		"signed with another provider's key": func() Envelope { return signed(t, testEnvelope(), betaKey) },
		"providerId changed after signing": func() Envelope {
			env := signed(t, testEnvelope(), alphaKey)
			env.Data.ProviderID = "beta"
			return env
		},
		"amount changed after signing": func() Envelope {
			env := signed(t, testEnvelope(), alphaKey)
			env.Data.Money.Amount = "2500.00"
			return env
		},
		"kind changed after signing": func() Envelope {
			env := signed(t, testEnvelope(), alphaKey)
			env.Data.Kind = "WIN"
			return env
		},
		"idempotency key changed after signing": func() Envelope {
			env := signed(t, testEnvelope(), alphaKey)
			env.Data.IdempotencyKey = "k2"
			return env
		},
		"provider without key": func() Envelope {
			env := testEnvelope()
			env.Data.ProviderID = "gamma"
			return signed(t, env, alphaKey)
		},
	}
	for name, build := range rejected {
		t.Run(name, func(t *testing.T) {
			c, p := newConsumer(verifier)
			if a := c.Handle(context.Background(), message(t, build())); a != ActionDeadLetter || p.calls != 0 {
				t.Fatalf("action %s, calls %d: must dead-letter without processing", a, p.calls)
			}
		})
	}
	t.Run("no verifier configured", func(t *testing.T) {
		c, p := newConsumer(nil)
		if a := c.Handle(context.Background(), message(t, signed(t, testEnvelope(), alphaKey))); a != ActionDeadLetter || p.calls != 0 {
			t.Fatalf("action %s, calls %d", a, p.calls)
		}
	})
}
