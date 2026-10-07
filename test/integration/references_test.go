//go:build integration

package integration

import (
	"testing"
	"time"
)

func txByExternal(t *testing.T, base, ext string) response {
	t.Helper()
	return call(t, "GET", base+"/providers/alpha/wagering/transactions/"+ext, token(t, "provider-alpha"), nil, nil)
}

// TestRefundBeforeBetResolves: a REFUND arriving before its BET waits in
// PENDING_REFERENCE and is completed by the worker once the BET exists.
func TestRefundBeforeBetResolves(t *testing.T) {
	cfg := baseConfig(t, createQueues(t, 5))
	cfg.ReferenceWorkerEnabled = true
	a := startApp(t, cfg)
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")
	betExt, refundExt := "bet-"+newID(), "refund-"+newID()

	refundKey := newID()
	refund := wager{ProviderID: "alpha", ExternalID: refundExt, PlayerID: player, WalletID: walletID, Kind: "REFUND", Amount: "30.00", Reference: betExt}
	r := postWager(t, a.base, "provider-alpha", refundKey, refund)
	if r.Status != 202 || r.Body["status"] != "PENDING_REFERENCE" || r.Body["balanceAfter"] != nil {
		t.Fatalf("refund before bet: %d %s", r.Status, r.Raw)
	}
	refundID := r.Body["transactionId"].(string)
	if countRows(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionPendingReference'`, refundID) != 1 {
		t.Fatal("missing WagerTransactionPendingReference event")
	}

	if b := postWager(t, a.base, "provider-alpha", newID(), wager{ProviderID: "alpha", ExternalID: betExt,
		PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "30.00"}); b.Status != 200 {
		t.Fatalf("bet: %d %s", b.Status, b.Raw)
	}
	eventually(t, 15*time.Second, func() bool { return txByExternal(t, a.base, refundExt).Body["status"] == "PROCESSED" }, "refund resolved")

	if bal, _ := walletBalance(t, walletID); bal != 10000 || ledgerNet(t, walletID) != 10000 {
		t.Fatalf("balance %d ledger %d", bal, ledgerNet(t, walletID))
	}
	if countRows(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionProcessed'`, refundID) != 1 {
		t.Fatal("missing WagerTransactionProcessed for the resolved refund")
	}
	// Replay now returns the final persisted result.
	rp := postWager(t, a.base, "provider-alpha", refundKey, refund)
	if rp.Status != 200 || rp.Body["idempotentReplay"] != true || rp.Body["status"] != "PROCESSED" {
		t.Fatalf("replay after resolution: %d %s", rp.Status, rp.Raw)
	}
}

// TestPendingReferenceExpires: the reference never arrives; after the retry
// budget the transaction is REJECTED with REFERENCE_NOT_FOUND and an event.
func TestPendingReferenceExpires(t *testing.T) {
	cfg := baseConfig(t, createQueues(t, 5))
	cfg.ReferenceWorkerEnabled = true
	cfg.ReferenceMaxAttempts = 3
	cfg.ReferenceRetryBase = 100 * time.Millisecond
	cfg.ReferenceRetryMax = 200 * time.Millisecond
	a := startApp(t, cfg)
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")
	ext := "rollback-" + newID()
	r := postWager(t, a.base, "provider-alpha", newID(), wager{ProviderID: "alpha", ExternalID: ext,
		PlayerID: player, WalletID: walletID, Kind: "ROLLBACK", Amount: "10.00", Reference: "never-" + newID()})
	if r.Status != 202 {
		t.Fatalf("rollback: %d %s", r.Status, r.Raw)
	}
	id := r.Body["transactionId"].(string)
	eventually(t, 15*time.Second, func() bool { return txByExternal(t, a.base, ext).Body["status"] == "REJECTED" }, "expired")
	got := txByExternal(t, a.base, ext)
	if got.Body["failureCode"] != "REFERENCE_NOT_FOUND" || got.Body["referenceAttempts"].(float64) != 2 {
		t.Fatalf("expired transaction: %s", got.Raw)
	}
	if countRows(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionRejected'
		AND payload->'data'->>'failureCode' = 'REFERENCE_NOT_FOUND'`, id) != 1 {
		t.Fatal("missing rejection event")
	}
	if bal, _ := walletBalance(t, walletID); bal != 10000 {
		t.Fatalf("balance changed: %d", bal)
	}
}

// TestPendingSurvivesRestart: instance A accepts a pending reversal and stops;
// instance B (fresh process state) resumes it from PostgreSQL. Idempotent
// replays keep working across the restart.
func TestPendingSurvivesRestart(t *testing.T) {
	q := createQueues(t, 5)
	first := startApp(t, baseConfig(t, q)) // worker disabled
	player := "player-" + newID()
	walletID := createWallet(t, first.base, player, "100.00")
	betExt, rbExt := "bet-"+newID(), "rb-"+newID()
	rbKey := newID()
	rb := wager{ProviderID: "alpha", ExternalID: rbExt, PlayerID: player, WalletID: walletID, Kind: "ROLLBACK", Amount: "20.00", Reference: betExt}
	if r := postWager(t, first.base, "provider-alpha", rbKey, rb); r.Status != 202 {
		t.Fatalf("rollback: %d %s", r.Status, r.Raw)
	}
	betKey := newID()
	bet := wager{ProviderID: "alpha", ExternalID: betExt, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "20.00"}
	if r := postWager(t, first.base, "provider-alpha", betKey, bet); r.Status != 200 {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	first.stop(t)
	if txStatusByExternal(t, rbExt) != "PENDING_REFERENCE" {
		t.Fatal("rollback must still be pending while no worker runs")
	}

	cfg := baseConfig(t, q)
	cfg.ReferenceWorkerEnabled = true
	second := startApp(t, cfg)
	eventually(t, 15*time.Second, func() bool { return txStatusByExternal(t, rbExt) == "PROCESSED" }, "resumed by second instance")
	if bal, version := walletBalance(t, walletID); bal != 10000 || version != 3 {
		t.Fatalf("balance=%d version=%d", bal, version)
	}
	r := postWager(t, second.base, "provider-alpha", betKey, bet)
	if r.Body["idempotentReplay"] != true || r.Body["balanceAfter"].(map[string]any)["amount"] != "80.00" {
		t.Fatalf("replay after restart: %s", r.Raw)
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID); n != 3 {
		t.Fatalf("ledger entries = %d (opening, bet, rollback)", n)
	}

}
