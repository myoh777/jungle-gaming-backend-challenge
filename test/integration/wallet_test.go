//go:build integration

package integration

import (
	"context"
	"testing"
)

func TestCreateWalletWithOpeningIsAtomic(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	player := "player-" + newID()
	r := call(t, "POST", a.base+"/wallets", token(t, "wallet-admin"), map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"},
	}, nil)
	if r.Status != 201 {
		t.Fatalf("create: %d %s", r.Status, r.Raw)
	}
	walletID := r.Body["walletId"].(string)
	openingID := r.Body["openingTransactionId"].(string)
	if r.Body["version"].(float64) != 1 || r.Body["balance"].(map[string]any)["amount"] != "100.00" {
		t.Fatalf("unexpected wallet %s", r.Raw)
	}

	if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'
		AND origin = 'INTERNAL' AND status = 'PROCESSED' AND provider_id IS NULL AND idempotency_key IS NULL`, walletID); n != 1 {
		t.Fatalf("opening rows = %d", n)
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND transaction_id = $2
		AND direction = 'CREDIT' AND amount = 10000 AND balance_before = 0 AND balance_after = 10000`, walletID, openingID); n != 1 {
		t.Fatalf("opening ledger rows = %d", n)
	}
	if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE
		(event_type = 'WagerTransactionProcessed' AND aggregate_id = $1 AND payload->'data'->>'origin' = 'INTERNAL'
		 AND NOT (payload->'data' ? 'providerId'))
		OR (event_type = 'WalletBalanceChanged' AND aggregate_id = $2 AND payload->'data'->>'walletVersion' = '1')`, openingID, walletID); n != 2 {
		t.Fatalf("opening events = %d", n)
	}

	// Duplicate player/currency -> 409 and nothing new written.
	dup := call(t, "POST", a.base+"/wallets", token(t, "wallet-admin"), map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": "5.00", "currency": "BRL"},
	}, nil)
	if dup.Status != 409 {
		t.Fatalf("duplicate wallet: %d %s", dup.Status, dup.Raw)
	}
	if n := countRows(t, `SELECT count(*) FROM wallets WHERE player_id = $1`, player); n != 1 {
		t.Fatalf("wallets for player = %d", n)
	}

	got := call(t, "GET", a.base+"/wallets/"+walletID, token(t, "wallet-admin"), nil, nil)
	if got.Status != 200 || got.Body["playerId"] != player {
		t.Fatalf("get wallet: %d %s", got.Status, got.Raw)
	}
}

func TestCreateWalletZeroBalance(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	walletID := createWallet(t, a.base, "player-"+newID(), "0.00")
	bal, version := walletBalance(t, walletID)
	if bal != 0 || version != 1 {
		t.Fatalf("balance=%d version=%d", bal, version)
	}
	if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, walletID); n != 0 {
		t.Fatal("zero wallet must not create OPENING")
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID); n != 0 {
		t.Fatal("zero wallet must not create ledger entries")
	}
	if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, walletID); n != 0 {
		t.Fatal("zero wallet must not create financial events")
	}
}

func TestCreateWalletRejectsInvalidMoney(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	for _, amount := range []string{"-1.00", "1.001", "NaN", "1e2", ""} {
		r := call(t, "POST", a.base+"/wallets", token(t, "wallet-admin"), map[string]any{
			"playerId": "player-" + newID(), "initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
		}, nil)
		if r.Status != 400 {
			t.Fatalf("amount %q: got %d %s", amount, r.Status, r.Raw)
		}
	}
	r := call(t, "POST", a.base+"/wallets", token(t, "wallet-admin"),
		`{"playerId":"p","initialBalance":{"amount":10.5,"currency":"BRL"}}`, nil)
	if r.Status != 400 {
		t.Fatalf("numeric amount must be rejected: %d", r.Status)
	}
}

func TestLedgerPaginationAndReconciliation(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")
	for i := 0; i < 4; i++ {
		r := postWager(t, a.base, "provider-alpha", newID(), wager{ProviderID: "alpha", ExternalID: newID(),
			PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "5.00"})
		if r.Status != 200 {
			t.Fatalf("bet %d: %d %s", i, r.Status, r.Raw)
		}
	}

	var seen []string
	cursor := ""
	for page := 0; page < 10; page++ {
		url := a.base + "/wallets/" + walletID + "/ledger?limit=2"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		r := call(t, "GET", url, token(t, "wallet-admin"), nil, nil)
		if r.Status != 200 {
			t.Fatalf("ledger: %d %s", r.Status, r.Raw)
		}
		for _, e := range r.Body["entries"].([]any) {
			seen = append(seen, e.(map[string]any)["entryId"].(string))
		}
		next, _ := r.Body["nextCursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 5 {
		t.Fatalf("expected 5 entries (opening + 4 bets), got %d", len(seen))
	}
	if r := call(t, "GET", a.base+"/wallets/"+walletID+"/ledger?cursor=%%%", token(t, "wallet-admin"), nil, nil); r.Status != 400 {
		t.Fatalf("malformed cursor: %d", r.Status)
	}

	rec := call(t, "POST", a.base+"/wallets/"+walletID+"/reconciliation", token(t, "wallet-admin"), nil, nil)
	if rec.Status != 200 || rec.Body["consistent"] != true || rec.Body["checkedEntries"].(float64) != 5 ||
		rec.Body["storedBalance"].(map[string]any)["amount"] != "80.00" || rec.Body["difference"].(map[string]any)["amount"] != "0.00" {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}

	// Tamper with the stored balance directly (the ledger cannot be changed):
	// reconciliation must report the divergence and must not fix it.
	_, err := db.Exec(context.Background(), `UPDATE wallets SET balance_amount = balance_amount + 1234 WHERE id = $1`, walletID)
	must(t, err)
	rec = call(t, "POST", a.base+"/wallets/"+walletID+"/reconciliation", token(t, "wallet-admin"), nil, nil)
	if rec.Body["consistent"] != false || rec.Body["difference"].(map[string]any)["amount"] != "12.34" ||
		rec.Body["calculatedBalance"].(map[string]any)["amount"] != "80.00" {
		t.Fatalf("divergence not reported: %s", rec.Raw)
	}
	if bal, _ := walletBalance(t, walletID); bal != 8000+1234 {
		t.Fatalf("reconciliation must not change the balance, got %d", bal)
	}
}
