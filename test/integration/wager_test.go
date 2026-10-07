//go:build integration

package integration

import (
	"fmt"
	"sync"
	"testing"
)

func TestWagerFlowAndIdempotency(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")

	betKey, betExt := newID(), newID()
	bet := wager{ProviderID: "alpha", ExternalID: betExt, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "25.00"}
	r := postWager(t, a.base, "provider-alpha", betKey, bet)
	if r.Status != 200 || r.Body["status"] != "PROCESSED" || r.Body["idempotentReplay"] != false ||
		r.Body["balanceAfter"].(map[string]any)["amount"] != "75.00" || r.Body["walletVersion"].(float64) != 2 {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	txID := r.Body["transactionId"].(string)

	// Balance changes after the bet...
	win := postWager(t, a.base, "provider-alpha", newID(), wager{ProviderID: "alpha", ExternalID: newID(),
		PlayerID: player, WalletID: walletID, Kind: "WIN", Amount: "10.00", Reference: betExt})
	if win.Status != 200 || win.Body["referenceTransactionId"] != txID || win.Body["balanceAfter"].(map[string]any)["amount"] != "85.00" {
		t.Fatalf("win: %d %s", win.Status, win.Raw)
	}

	// ...but a replay still returns the balance observed by the original processing.
	replay := postWager(t, a.base, "provider-alpha", betKey, bet)
	if replay.Status != 200 || replay.Body["idempotentReplay"] != true || replay.Body["transactionId"] != txID ||
		replay.Body["balanceAfter"].(map[string]any)["amount"] != "75.00" {
		t.Fatalf("replay: %d %s", replay.Status, replay.Raw)
	}

	// Same key, different payload -> 409, no effect.
	changed := bet
	changed.Amount = "26.00"
	if c := postWager(t, a.base, "provider-alpha", betKey, changed); c.Status != 409 || c.Body["error"].(map[string]any)["code"] != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("key reuse: %d %s", c.Status, c.Raw)
	}
	// Same external id under another key -> 409, not reapplied.
	if c := postWager(t, a.base, "provider-alpha", newID(), bet); c.Status != 409 || c.Body["error"].(map[string]any)["code"] != "DUPLICATE_EXTERNAL_TRANSACTION" {
		t.Fatalf("external id reuse: %d %s", c.Status, c.Raw)
	}
	if bal, version := walletBalance(t, walletID); bal != 8500 || version != 3 {
		t.Fatalf("balance=%d version=%d", bal, version)
	}

	// LOSS: processed, no ledger, no version change, no WalletBalanceChanged.
	loss := postWager(t, a.base, "provider-alpha", newID(), wager{ProviderID: "alpha", ExternalID: newID(),
		PlayerID: player, WalletID: walletID, Kind: "LOSS", Amount: "0.00"})
	if loss.Status != 200 || loss.Body["status"] != "PROCESSED" || loss.Body["walletVersion"].(float64) != 3 {
		t.Fatalf("loss: %d %s", loss.Status, loss.Raw)
	}
	lossID := loss.Body["transactionId"].(string)
	if countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id = $1`, lossID) != 0 {
		t.Fatal("LOSS must not create ledger entries")
	}
	if countRows(t, `SELECT count(*) FROM outbox_events WHERE payload->'data'->>'transactionId' = $1 AND event_type = 'WalletBalanceChanged'`, lossID) != 0 ||
		countRows(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionProcessed'`, lossID) != 1 {
		t.Fatal("LOSS must emit only WagerTransactionProcessed")
	}

	// Zero policy and OPENING are invalid input, nothing persisted.
	for name, w := range map[string]wager{
		"loss with amount": {ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "LOSS", Amount: "1.00"},
		"bet zero":         {ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "0.00"},
		"opening":          {ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "OPENING", Amount: "1.00"},
		"negative":         {ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "-1.00"},
		"scale":            {ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "1.001"},
		"refund no ref":    {ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "REFUND", Amount: "1.00"},
	} {
		if r := postWager(t, a.base, "provider-alpha", newID(), w); r.Status != 400 {
			t.Fatalf("%s: expected 400, got %d %s", name, r.Status, r.Raw)
		}
	}

	// Insufficient funds -> REJECTED (422), persisted and replayable.
	poorKey := newID()
	poor := wager{ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "1000.00"}
	rj := postWager(t, a.base, "provider-alpha", poorKey, poor)
	if rj.Status != 422 || rj.Body["status"] != "REJECTED" || rj.Body["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Fatalf("insufficient: %d %s", rj.Status, rj.Raw)
	}
	if rr := postWager(t, a.base, "provider-alpha", poorKey, poor); rr.Status != 422 || rr.Body["idempotentReplay"] != true {
		t.Fatalf("rejected replay: %d %s", rr.Status, rr.Raw)
	}

	// Wallet not found -> 404, nothing persisted.
	if r := postWager(t, a.base, "provider-alpha", newID(), wager{ProviderID: "alpha", ExternalID: newID(),
		PlayerID: player, WalletID: newID(), Kind: "BET", Amount: "1.00"}); r.Status != 404 {
		t.Fatalf("unknown wallet: %d", r.Status)
	}

	// Queries expose status, failure code and persisted result.
	q := call(t, "GET", a.base+"/providers/alpha/wagering/transactions/"+poor.ExternalID, token(t, "provider-alpha"), nil, nil)
	if q.Status != 200 || q.Body["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Fatalf("query by external id: %d %s", q.Status, q.Raw)
	}
	q = call(t, "GET", a.base+"/wagering/transactions/"+txID, token(t, "provider-alpha"), nil, nil)
	if q.Status != 200 || q.Body["status"] != "PROCESSED" || q.Body["balanceAfter"].(map[string]any)["amount"] != "75.00" {
		t.Fatalf("query by id: %d %s", q.Status, q.Raw)
	}

	if ledgerNet(t, walletID) != 8500 {
		t.Fatalf("ledger net %d != stored balance", ledgerNet(t, walletID))
	}
}

func TestReversalsAndDoubleRefundPolicy(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")
	alpha := func(kind, amount, ref string) response {
		return postWager(t, a.base, "provider-alpha", newID(), wager{ProviderID: "alpha", ExternalID: "ext-" + newID(),
			PlayerID: player, WalletID: walletID, Kind: kind, Amount: amount, Reference: ref})
	}
	extOf := func(r response) string { return r.Body["externalTransactionId"].(string) }

	bet := alpha("BET", "30.00", "")
	refund := alpha("REFUND", "30.00", extOf(bet))
	if refund.Body["status"] != "PROCESSED" || refund.Body["balanceAfter"].(map[string]any)["amount"] != "100.00" {
		t.Fatalf("refund: %s", refund.Raw)
	}
	// A second REFUND, or a ROLLBACK, of the same bet would return the debit twice.
	if r := alpha("REFUND", "30.00", extOf(bet)); r.Body["failureCode"] != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("double refund: %s", r.Raw)
	}
	if r := alpha("ROLLBACK", "30.00", extOf(bet)); r.Body["failureCode"] != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("rollback after refund: %s", r.Raw)
	}
	// Rolling back the REFUND itself is allowed and debits again.
	if r := alpha("ROLLBACK", "30.00", extOf(refund)); r.Body["status"] != "PROCESSED" || r.Body["balanceAfter"].(map[string]any)["amount"] != "70.00" {
		t.Fatalf("rollback of refund: %s", r.Raw)
	}

	// ROLLBACK of a WIN that would make the balance negative -> distinct code.
	win := alpha("WIN", "50.00", "")
	if r := alpha("BET", "120.00", ""); r.Body["status"] != "PROCESSED" { // balance 120 -> 0
		t.Fatalf("bet all: %s", r.Raw)
	}
	if r := alpha("ROLLBACK", "50.00", extOf(win)); r.Body["failureCode"] != "REVERSAL_INSUFFICIENT_FUNDS" {
		t.Fatalf("rollback insufficient: %s", r.Raw)
	}
	// Amount mismatch.
	bet2 := alpha("WIN", "5.00", "")
	if r := alpha("ROLLBACK", "4.00", extOf(bet2)); r.Body["failureCode"] != "REVERSAL_AMOUNT_MISMATCH" {
		t.Fatalf("amount mismatch: %s", r.Raw)
	}
	if bal, _ := walletBalance(t, walletID); int64(bal) != ledgerNet(t, walletID) || bal != 500 {
		t.Fatalf("balance %d vs ledger %d", bal, ledgerNet(t, walletID))
	}
}

// TestSameBetFiftyTimesInParallel: 50 concurrent copies of one bet -> one debit.
func TestSameBetFiftyTimesInParallel(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")
	key := newID()
	bet := wager{ProviderID: "alpha", ExternalID: newID(), PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "10.00"}

	var wg sync.WaitGroup
	results := make([]response, 50)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = postWager(t, a.base, "provider-alpha", key, bet)
		}(i)
	}
	wg.Wait()

	fresh, ids := 0, map[string]bool{}
	for _, r := range results {
		if r.Status != 200 || r.Body["status"] != "PROCESSED" || r.Body["balanceAfter"].(map[string]any)["amount"] != "90.00" {
			t.Fatalf("unexpected response %d %s", r.Status, r.Raw)
		}
		if r.Body["idempotentReplay"] == false {
			fresh++
		}
		ids[r.Body["transactionId"].(string)] = true
	}
	if fresh != 1 || len(ids) != 1 {
		t.Fatalf("expected exactly one fresh processing and one transaction id, got fresh=%d ids=%d", fresh, len(ids))
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if bal, version := walletBalance(t, walletID); bal != 9000 || version != 2 {
		t.Fatalf("balance=%d version=%d", bal, version)
	}
}

// TestDistinctWalletsInParallel: independent wallets progress concurrently and
// each ends with balance == ledger net.
func TestDistinctWalletsInParallel(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	const wallets, betsPerWallet = 10, 10
	type w struct{ id, player string }
	ws := make([]w, wallets)
	for i := range ws {
		ws[i].player = "player-" + newID()
		ws[i].id = createWallet(t, a.base, ws[i].player, "50.00")
	}
	var wg sync.WaitGroup
	errs := make(chan string, wallets*betsPerWallet)
	for _, wl := range ws {
		for j := 0; j < betsPerWallet; j++ {
			wg.Add(1)
			go func(wl w) {
				defer wg.Done()
				r := postWager(t, a.base, "provider-alpha", newID(), wager{ProviderID: "alpha", ExternalID: newID(),
					PlayerID: wl.player, WalletID: wl.id, Kind: "BET", Amount: "1.00"})
				if r.Status != 200 {
					errs <- fmt.Sprintf("%d %s", r.Status, r.Raw)
				}
			}(wl)
		}
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	for _, wl := range ws {
		bal, version := walletBalance(t, wl.id)
		if bal != 4000 || version != 1+betsPerWallet || ledgerNet(t, wl.id) != bal {
			t.Fatalf("wallet %s: balance=%d version=%d ledger=%d", wl.id, bal, version, ledgerNet(t, wl.id))
		}
	}
}
