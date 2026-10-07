package sqsconsumer

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"wagering/internal/app"
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
