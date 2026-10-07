package wagering

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"wagering/internal/domain/wallet"
	"wagering/internal/money"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

var retry = RetryPolicy{BaseDelay: time.Second, MaxDelay: 8 * time.Second, MaxAttempts: 4, TTL: time.Hour}

func brl(minor int64) money.Money { return money.MustNew(minor, money.BRL) }

func idGen() func() string {
	n := 0
	return func() string { n++; return fmt.Sprintf("id-%d", n) }
}

func baseReq(kind Kind, amount int64) ExternalRequest {
	return ExternalRequest{
		ProviderID: "alpha", ExternalTransactionID: "ext-" + string(kind), IdempotencyKey: "key-" + string(kind),
		PlayerID: "p1", WalletID: "w1", RoundID: "r1", GameID: "g1",
		Kind: kind, Money: brl(amount), CorrelationID: "corr",
	}
}

func newTx(t *testing.T, id string, req ExternalRequest) *Transaction {
	t.Helper()
	tx, err := NewExternal(id, req, PayloadHash(req), t0)
	if err != nil {
		t.Fatalf("NewExternal: %v", err)
	}
	return tx
}

func walletWith(t *testing.T, minor int64) *wallet.Wallet {
	t.Helper()
	w, err := wallet.Rehydrate("w1", "p1", brl(minor), 1, t0, t0)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// processedRef builds a PROCESSED reference transaction as it would come from storage.
func processedRef(t *testing.T, id string, kind Kind, amount int64, mutate func(*Snapshot)) *Transaction {
	t.Helper()
	req := baseReq(kind, amount)
	req.ExternalTransactionID = "ref-" + id
	s := Snapshot{
		ID: id, Origin: OriginExternal, ProviderID: req.ProviderID, ExternalTransactionID: req.ExternalTransactionID,
		IdempotencyKey: "k-" + id, PayloadHash: "h", WalletID: "w1", PlayerID: "p1", RoundID: "r1", GameID: "g1",
		Kind: kind, Money: brl(amount), Status: StatusProcessed, CreatedAt: t0, UpdatedAt: t0,
	}
	if mutate != nil {
		mutate(&s)
	}
	r, err := Rehydrate(s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func settle(t *testing.T, tx *Transaction, w *wallet.Wallet, ref *Transaction, reversed bool) Settlement {
	t.Helper()
	s, err := Settle(SettleInput{
		Transaction: tx, Wallet: w, Reference: ref, ReferenceAlreadyReversed: reversed,
		Retry: retry, Now: t0, NewID: idGen(), CausationID: "cause",
	})
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	return s
}

func eventTypes(s Settlement) []string {
	var out []string
	for _, e := range s.Events {
		out = append(out, e.EventType)
	}
	return out
}

func TestValidate_ZeroPolicyAndStructure(t *testing.T) {
	cases := []struct {
		name  string
		mod   func(*ExternalRequest)
		field string
	}{
		{"bet zero", func(r *ExternalRequest) { r.Money = brl(0) }, "money.amount"},
		{"win zero", func(r *ExternalRequest) { r.Kind = KindWin; r.Money = brl(0) }, "money.amount"},
		{"loss non zero", func(r *ExternalRequest) { r.Kind = KindLoss; r.Money = brl(1) }, "money.amount"},
		{"refund zero", func(r *ExternalRequest) {
			r.Kind = KindRefund
			r.ReferenceExternalTransactionID = "x"
			r.Money = brl(0)
		}, "money.amount"},
		{"refund without ref", func(r *ExternalRequest) { r.Kind = KindRefund }, "referenceExternalTransactionId"},
		{"rollback without ref", func(r *ExternalRequest) { r.Kind = KindRollback }, "referenceExternalTransactionId"},
		{"bet with ref", func(r *ExternalRequest) { r.ReferenceExternalTransactionID = "x" }, "referenceExternalTransactionId"},
		{"self ref", func(r *ExternalRequest) {
			r.Kind = KindRefund
			r.ReferenceExternalTransactionID = r.ExternalTransactionID
		}, "referenceExternalTransactionId"},
		{"opening", func(r *ExternalRequest) { r.Kind = KindOpening }, "kind"},
		{"unknown kind", func(r *ExternalRequest) { r.Kind = "bet" }, "kind"},
		{"missing provider", func(r *ExternalRequest) { r.ProviderID = "" }, "providerId"},
		{"missing round", func(r *ExternalRequest) { r.RoundID = "" }, "roundId"},
		{"missing key", func(r *ExternalRequest) { r.IdempotencyKey = "" }, "idempotencyKey"},
		{"too long", func(r *ExternalRequest) { r.GameID = strings.Repeat("g", 201) }, "gameId"},
		{"negative", func(r *ExternalRequest) { r.Money = brl(-1) }, "money.amount"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := baseReq(KindBet, 100)
			tc.mod(&req)
			err := req.Validate()
			var ve *ValidationError
			if !errors.As(err, &ve) || ve.Field != tc.field || !errors.Is(err, ErrInvalidTransaction) {
				t.Fatalf("got %v, want validation error on %s", err, tc.field)
			}
		})
	}
	loss := baseReq(KindLoss, 0)
	if err := loss.Validate(); err != nil {
		t.Fatalf("LOSS 0.00 must be valid: %v", err)
	}
}

func TestParseExternalKindRejectsOpening(t *testing.T) {
	if _, err := ParseExternalKind("OPENING"); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("OPENING must be rejected: %v", err)
	}
	for _, k := range []string{"BET", "WIN", "LOSS", "REFUND", "ROLLBACK"} {
		if _, err := ParseExternalKind(k); err != nil {
			t.Fatalf("%s: %v", k, err)
		}
	}
}

func TestPayloadHash(t *testing.T) {
	a := baseReq(KindBet, 2550)
	b := a
	b.IdempotencyKey = "other-key"
	b.CorrelationID = "other-corr"
	if PayloadHash(a) != PayloadHash(b) {
		t.Fatal("idempotency key and correlation id must not affect the hash")
	}
	parsed, _ := money.ParseNonNegative("25.5", "BRL")
	c := a
	c.Money = parsed
	if PayloadHash(a) != PayloadHash(c) {
		t.Fatal("25.5 and 25.50 must hash equally")
	}
	d := a
	d.Money = brl(2551)
	if PayloadHash(a) == PayloadHash(d) {
		t.Fatal("different amount must change the hash")
	}
	e := a
	e.RoundID = "r2"
	if PayloadHash(a) == PayloadHash(e) {
		t.Fatal("different round must change the hash")
	}
	if len(PayloadHash(a)) != 64 {
		t.Fatal("expected hex sha256")
	}
}

func TestTransitions(t *testing.T) {
	tx := newTx(t, "t1", baseReq(KindBet, 100))
	if tx.Status() != StatusPending {
		t.Fatal("new transaction must be PENDING")
	}
	if err := tx.markProcessed(brl(0), 2, t0); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func() error{
		func() error { return tx.markProcessed(brl(0), 3, t0) },
		func() error { return tx.markRejected(FailureInsufficientFunds, nil, nil, t0) },
		func() error { return tx.waitForReference(1, t0, t0) },
		func() error { return tx.MarkFailed("X", t0) },
	} {
		if err := fn(); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("terminal state must not transition: %v", err)
		}
	}

	p := newTx(t, "t2", baseReq(KindBet, 100))
	if err := p.waitForReference(1, t0, t0); err != nil {
		t.Fatal(err)
	}
	if err := p.waitForReference(2, t0, t0); err != nil {
		t.Fatalf("PENDING_REFERENCE retry: %v", err)
	}
	if err := p.MarkFailed("DB_PERMANENT", t0); err != nil {
		t.Fatal(err)
	}
	if !p.Status().IsTerminal() {
		t.Fatal("FAILED is terminal")
	}
	if err := newTx(t, "t3", baseReq(KindBet, 1)).markRejected("", nil, nil, t0); err == nil {
		t.Fatal("rejection without code must fail")
	}
}

func TestRehydrateValidates(t *testing.T) {
	if _, err := Rehydrate(Snapshot{ID: "x", WalletID: "w", Kind: KindBet, Status: "WEIRD", Origin: OriginExternal}); err == nil {
		t.Fatal("unknown status must fail")
	}
	if _, err := Rehydrate(Snapshot{ID: "x", WalletID: "w", Kind: KindOpening, Status: StatusProcessed, Origin: OriginExternal}); err == nil {
		t.Fatal("external OPENING must fail")
	}
	if _, err := Rehydrate(Snapshot{ID: "x", WalletID: "w", Kind: KindBet, Status: StatusRejected, Origin: OriginExternal}); err == nil {
		t.Fatal("REJECTED without code must fail")
	}
}

func TestBet(t *testing.T) {
	w := walletWith(t, 10000)
	tx := newTx(t, "t1", baseReq(KindBet, 8000))
	s := settle(t, tx, w, nil, false)
	if tx.Status() != StatusProcessed || w.Balance().MinorUnits() != 2000 || w.Version() != 2 {
		t.Fatalf("status=%s balance=%s version=%d", tx.Status(), w.Balance(), w.Version())
	}
	if s.LedgerEntry == nil || s.LedgerEntry.Direction != wallet.Debit {
		t.Fatal("BET must produce a debit")
	}
	if got := eventTypes(s); fmt.Sprint(got) != "[WagerTransactionProcessed WalletBalanceChanged]" {
		t.Fatalf("events %v", got)
	}
	if tx.ResultBalance().MinorUnits() != 2000 || *tx.ResultWalletVersion() != 2 {
		t.Fatal("result balance must be recorded")
	}
}

func TestBetInsufficientFunds(t *testing.T) {
	w := walletWith(t, 2000)
	tx := newTx(t, "t1", baseReq(KindBet, 8000))
	s := settle(t, tx, w, nil, false)
	if tx.Status() != StatusRejected || tx.FailureCode() != FailureInsufficientFunds {
		t.Fatalf("status=%s code=%s", tx.Status(), tx.FailureCode())
	}
	if s.LedgerEntry != nil || w.Balance().MinorUnits() != 2000 || w.Version() != 1 {
		t.Fatal("rejected BET must not move money")
	}
	if got := eventTypes(s); fmt.Sprint(got) != "[WagerTransactionRejected]" {
		t.Fatalf("events %v", got)
	}
}

func TestWinWithAndWithoutReference(t *testing.T) {
	w := walletWith(t, 1000)
	tx := newTx(t, "t1", baseReq(KindWin, 500))
	s := settle(t, tx, w, nil, false)
	if tx.Status() != StatusProcessed || w.Balance().MinorUnits() != 1500 || s.LedgerEntry.Direction != wallet.Credit {
		t.Fatal("WIN must credit")
	}

	req := baseReq(KindWin, 700)
	req.ExternalTransactionID = "win2"
	req.ReferenceExternalTransactionID = "ref-bet"
	tx2 := newTx(t, "t2", req)
	settle(t, tx2, w, processedRef(t, "bet", KindBet, 100, nil), false)
	if tx2.Status() != StatusProcessed || tx2.ReferenceTransactionID() != "bet" {
		t.Fatalf("WIN with BET reference: %s", tx2.Status())
	}

	req.ExternalTransactionID = "win3"
	tx3 := newTx(t, "t3", req)
	settle(t, tx3, w, processedRef(t, "bet2", KindBet, 100, func(s *Snapshot) { s.RoundID = "r-other" }), false)
	if tx3.FailureCode() != FailureReferenceMismatch {
		t.Fatalf("WIN with reference from another round: %s", tx3.FailureCode())
	}
}

func TestLossDoesNotMoveMoney(t *testing.T) {
	w := walletWith(t, 1000)
	tx := newTx(t, "t1", baseReq(KindLoss, 0))
	s := settle(t, tx, w, nil, false)
	if tx.Status() != StatusProcessed || s.LedgerEntry != nil || w.Version() != 1 || w.Balance().MinorUnits() != 1000 {
		t.Fatal("LOSS must not touch balance, version or ledger")
	}
	if got := eventTypes(s); fmt.Sprint(got) != "[WagerTransactionProcessed]" {
		t.Fatalf("LOSS events %v", got)
	}
}

func reversalReq(kind Kind, amount int64) ExternalRequest {
	r := baseReq(kind, amount)
	r.ReferenceExternalTransactionID = "ref-x"
	return r
}

func TestRefund(t *testing.T) {
	w := walletWith(t, 0)
	tx := newTx(t, "t1", reversalReq(KindRefund, 500))
	s := settle(t, tx, w, processedRef(t, "bet", KindBet, 500, nil), false)
	if tx.Status() != StatusProcessed || w.Balance().MinorUnits() != 500 || s.LedgerEntry.Direction != wallet.Credit {
		t.Fatalf("REFUND must credit the full bet: %s %s", tx.Status(), tx.FailureCode())
	}
}

func TestReversalRejections(t *testing.T) {
	cases := []struct {
		name     string
		kind     Kind
		amount   int64
		ref      func(t *testing.T) *Transaction
		reversed bool
		balance  int64
		want     FailureCode
	}{
		{"refund amount mismatch", KindRefund, 400, func(t *testing.T) *Transaction { return processedRef(t, "b", KindBet, 500, nil) }, false, 0, FailureReversalAmountMismatch},
		{"refund of win", KindRefund, 500, func(t *testing.T) *Transaction { return processedRef(t, "w", KindWin, 500, nil) }, false, 0, FailureReferenceKindNotAllowed},
		{"rollback of loss", KindRollback, 500, func(t *testing.T) *Transaction { return processedRef(t, "l", KindLoss, 500, nil) }, false, 0, FailureReferenceKindNotAllowed},
		{"already reversed", KindRefund, 500, func(t *testing.T) *Transaction { return processedRef(t, "b", KindBet, 500, nil) }, true, 0, FailureReferenceAlreadyReversed},
		{"rollback already reversed", KindRollback, 500, func(t *testing.T) *Transaction { return processedRef(t, "b", KindBet, 500, nil) }, true, 0, FailureReferenceAlreadyReversed},
		{"reference rejected", KindRefund, 500, func(t *testing.T) *Transaction {
			return processedRef(t, "b", KindBet, 500, func(s *Snapshot) { s.Status = StatusRejected; s.FailureCode = FailureInsufficientFunds })
		}, false, 0, FailureReferenceNotProcessed},
		{"reference other player", KindRefund, 500, func(t *testing.T) *Transaction {
			return processedRef(t, "b", KindBet, 500, func(s *Snapshot) { s.PlayerID = "p2" })
		}, false, 0, FailureReferenceMismatch},
		{"reference other wallet", KindRefund, 500, func(t *testing.T) *Transaction {
			return processedRef(t, "b", KindBet, 500, func(s *Snapshot) { s.WalletID = "w2" })
		}, false, 0, FailureReferenceMismatch},
		{"rollback of win insufficient", KindRollback, 500, func(t *testing.T) *Transaction { return processedRef(t, "w", KindWin, 500, nil) }, false, 100, FailureReversalInsufficientFunds},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := walletWith(t, tc.balance)
			tx := newTx(t, "t1", reversalReq(tc.kind, tc.amount))
			s := settle(t, tx, w, tc.ref(t), tc.reversed)
			if tx.Status() != StatusRejected || tx.FailureCode() != tc.want {
				t.Fatalf("got %s/%s, want REJECTED/%s", tx.Status(), tx.FailureCode(), tc.want)
			}
			if s.LedgerEntry != nil || w.Balance().MinorUnits() != tc.balance || w.Version() != 1 {
				t.Fatal("rejection must not move money")
			}
		})
	}
}

func TestRollbackDirections(t *testing.T) {
	cases := []struct {
		refKind Kind
		dir     wallet.Direction
		after   int64
	}{
		{KindBet, wallet.Credit, 1500},
		{KindWin, wallet.Debit, 500},
		{KindRefund, wallet.Debit, 500},
	}
	for _, tc := range cases {
		w := walletWith(t, 1000)
		tx := newTx(t, "t1", reversalReq(KindRollback, 500))
		s := settle(t, tx, w, processedRef(t, "r", tc.refKind, 500, nil), false)
		if tx.Status() != StatusProcessed || s.LedgerEntry.Direction != tc.dir || w.Balance().MinorUnits() != tc.after {
			t.Fatalf("ROLLBACK of %s: status=%s dir=%s balance=%s", tc.refKind, tx.Status(), s.LedgerEntry.Direction, w.Balance())
		}
	}
}

func TestWalletChecks(t *testing.T) {
	w := walletWith(t, 1000)
	req := baseReq(KindBet, 100)
	req.PlayerID = "intruder"
	tx := newTx(t, "t1", req)
	settle(t, tx, w, nil, false)
	if tx.FailureCode() != FailureWalletPlayerMismatch || tx.ResultBalance() != nil {
		t.Fatal("player mismatch must reject without disclosing balance")
	}

	req = baseReq(KindBet, 100)
	req.Money = money.MustNew(100, money.USD)
	tx = newTx(t, "t2", req)
	settle(t, tx, w, nil, false)
	if tx.FailureCode() != FailureCurrencyMismatch {
		t.Fatalf("currency mismatch: %s", tx.FailureCode())
	}
}

func TestPendingReferenceLifecycle(t *testing.T) {
	w := walletWith(t, 0)
	tx := newTx(t, "t1", reversalReq(KindRefund, 500))

	s := settle(t, tx, w, nil, false)
	if tx.Status() != StatusPendingReference || tx.ReferenceAttempts() != 1 {
		t.Fatalf("first attempt: %s attempts=%d", tx.Status(), tx.ReferenceAttempts())
	}
	if !tx.NextReferenceAttemptAt().Equal(t0.Add(time.Second)) {
		t.Fatalf("next attempt %v", tx.NextReferenceAttemptAt())
	}
	if got := eventTypes(s); fmt.Sprint(got) != "[WagerTransactionPendingReference]" {
		t.Fatalf("events %v", got)
	}

	s = settle(t, tx, w, nil, false)
	if tx.ReferenceAttempts() != 2 || len(s.Events) != 0 || !tx.NextReferenceAttemptAt().Equal(t0.Add(2*time.Second)) {
		t.Fatalf("retry must back off without new events: attempts=%d next=%v events=%d", tx.ReferenceAttempts(), tx.NextReferenceAttemptAt(), len(s.Events))
	}

	// The reference now exists but is itself pending: keep waiting.
	pendingRef := processedRef(t, "b", KindBet, 500, func(s *Snapshot) { s.Status = StatusPendingReference })
	settle(t, tx, w, pendingRef, false)
	if tx.Status() != StatusPendingReference || tx.ReferenceAttempts() != 3 {
		t.Fatal("pending reference must keep waiting")
	}

	// Reference processed: resolve.
	s = settle(t, tx, w, processedRef(t, "b", KindBet, 500, nil), false)
	if tx.Status() != StatusProcessed || w.Balance().MinorUnits() != 500 || s.LedgerEntry == nil {
		t.Fatalf("resolution: %s %s", tx.Status(), tx.FailureCode())
	}
}

func TestPendingReferenceExpires(t *testing.T) {
	w := walletWith(t, 0)
	tx := newTx(t, "t1", reversalReq(KindRollback, 500))
	for i := 0; i < retry.MaxAttempts-1; i++ {
		settle(t, tx, w, nil, false)
	}
	if tx.Status() != StatusPendingReference {
		t.Fatal("should still be pending")
	}
	s := settle(t, tx, w, nil, false)
	if tx.Status() != StatusRejected || tx.FailureCode() != FailureReferenceNotFound {
		t.Fatalf("expected REFERENCE_NOT_FOUND, got %s/%s", tx.Status(), tx.FailureCode())
	}
	if got := eventTypes(s); fmt.Sprint(got) != "[WagerTransactionRejected]" {
		t.Fatalf("events %v", got)
	}

	// TTL expiry with a reference that exists but is still pending.
	tx2 := newTx(t, "t2", reversalReq(KindRollback, 500))
	pendingRef := processedRef(t, "b", KindBet, 500, func(s *Snapshot) { s.Status = StatusPendingReference })
	_, err := Settle(SettleInput{Transaction: tx2, Wallet: w, Reference: pendingRef, Retry: retry,
		Now: t0.Add(2 * time.Hour), NewID: idGen()})
	if err != nil {
		t.Fatal(err)
	}
	if tx2.FailureCode() != FailureReferenceNotResolved {
		t.Fatalf("expected REFERENCE_NOT_RESOLVED, got %s", tx2.FailureCode())
	}
}

func TestRetryDelay(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: 5 * time.Second}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	for i, w := range want {
		if got := p.Delay(i + 1); got != w {
			t.Fatalf("attempt %d: got %v want %v", i+1, got, w)
		}
	}
}

func TestSettleRejectsTerminal(t *testing.T) {
	w := walletWith(t, 1000)
	tx := newTx(t, "t1", baseReq(KindBet, 100))
	settle(t, tx, w, nil, false)
	if _, err := Settle(SettleInput{Transaction: tx, Wallet: w, Retry: retry, Now: t0, NewID: idGen()}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("settling a terminal transaction must fail: %v", err)
	}
	if w.Balance().MinorUnits() != 900 {
		t.Fatal("replay must not reapply")
	}
}

func TestOpenWallet(t *testing.T) {
	res, err := OpenWallet(OpenWalletInput{WalletID: "w1", PlayerID: "p1", InitialBalance: brl(10000), CorrelationID: "c", Now: t0, NewID: idGen()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Wallet.Version() != 1 || res.Wallet.Balance().MinorUnits() != 10000 {
		t.Fatal("opening wallet must start at version 1 with the initial balance")
	}
	o := res.Opening.Snapshot()
	if o.Kind != KindOpening || o.Origin != OriginInternal || o.Status != StatusProcessed {
		t.Fatalf("opening %+v", o)
	}
	if o.ProviderID != "" || o.ExternalTransactionID != "" || o.IdempotencyKey != "" || o.RoundID != "" || o.GameID != "" {
		t.Fatal("OPENING must not carry external metadata")
	}
	if res.LedgerEntry == nil || res.LedgerEntry.TransactionID != o.ID || res.LedgerEntry.Direction != wallet.Credit {
		t.Fatal("opening must produce a credit entry")
	}
	if got := fmt.Sprint(eventTypesOf(res.Events)); got != "[WagerTransactionProcessed WalletBalanceChanged]" {
		t.Fatalf("events %v", got)
	}
	raw, _ := json.Marshal(res.Events[0])
	for _, field := range []string{"providerId", "externalTransactionId", "roundId", "gameId"} {
		if strings.Contains(string(raw), field) {
			t.Fatalf("internal event must not contain %s: %s", field, raw)
		}
	}

	zero, err := OpenWallet(OpenWalletInput{WalletID: "w2", PlayerID: "p1", InitialBalance: brl(0), Now: t0, NewID: idGen()})
	if err != nil {
		t.Fatal(err)
	}
	if zero.Opening != nil || zero.LedgerEntry != nil || len(zero.Events) != 0 || zero.Wallet.Version() != 1 {
		t.Fatal("zero balance must not create OPENING, ledger or events")
	}
}

func eventTypesOf(events []Event) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.EventType)
	}
	return out
}

func TestEventEnvelope(t *testing.T) {
	w := walletWith(t, 10000)
	tx := newTx(t, "t1", baseReq(KindBet, 2500))
	s := settle(t, tx, w, nil, false)
	raw, err := json.Marshal(s.Events[1])
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("missing %s in %s", k, raw)
		}
	}
	if m["eventType"] != EventWalletBalanceChanged || m["version"].(float64) != 1 || m["aggregateId"] != "w1" {
		t.Fatalf("bad envelope %s", raw)
	}
	if _, err := time.Parse(time.RFC3339, m["occurredAt"].(string)); err != nil {
		t.Fatal("occurredAt must be RFC3339")
	}
	data := m["data"].(map[string]any)
	if data["balanceBefore"].(map[string]any)["amount"] != "100.00" || data["balanceAfter"].(map[string]any)["amount"] != "75.00" {
		t.Fatalf("money must be decimal strings: %s", raw)
	}
	if data["walletVersion"].(float64) != 2 || data["direction"] != "DEBIT" {
		t.Fatalf("bad data %s", raw)
	}
}
