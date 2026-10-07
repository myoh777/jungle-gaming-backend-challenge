//go:build integration

package integration

import (
	"strings"
	"testing"
	"time"
)

// TestAuthentication uses real tokens from Keycloak.
func TestAuthentication(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")
	ext := newID()
	bet := wager{ProviderID: "alpha", ExternalID: ext, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "10.00"}
	key := map[string]string{"Idempotency-Key": newID()}

	// Missing and invalid credentials.
	if r := call(t, "POST", a.base+"/wagering/transactions", "", bet.body(), key); r.Status != 401 {
		t.Fatalf("missing token: %d", r.Status)
	}
	valid := token(t, "provider-alpha")
	parts := strings.Split(valid, ".")
	tampered := parts[0] + "." + parts[1] + "." + strings.Repeat("A", len(parts[2]))
	for name, tok := range map[string]string{"garbage": "abc.def.ghi", "bad signature": tampered} {
		if r := call(t, "POST", a.base+"/wagering/transactions", tok, bet.body(), key); r.Status != 401 {
			t.Fatalf("%s: %d", name, r.Status)
		}
	}

	// Expired token: provider-alpha-short tokens live 3 seconds.
	short := fetchToken(t, "provider-alpha-short")
	time.Sleep(5 * time.Second)
	if r := call(t, "POST", a.base+"/wagering/transactions", short, bet.body(), key); r.Status != 401 {
		t.Fatalf("expired token: %d %s", r.Status, r.Raw)
	}

	// Valid token without business role.
	if r := call(t, "POST", a.base+"/wagering/transactions", token(t, "no-role-client"), bet.body(), key); r.Status != 403 {
		t.Fatalf("no role: %d", r.Status)
	}

	// None of the above had financial effects.
	if bal, version := walletBalance(t, walletID); bal != 10000 || version != 1 {
		t.Fatalf("unauthorized calls changed the wallet: %d v%d", bal, version)
	}
	if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND origin = 'EXTERNAL'`, walletID); n != 0 {
		t.Fatalf("unauthorized calls persisted %d transactions", n)
	}
}

func TestProviderIsolationAndInternalEndpoints(t *testing.T) {
	a := startApp(t, baseConfig(t, createQueues(t, 5)))
	player := "player-" + newID()
	walletID := createWallet(t, a.base, player, "100.00")
	alphaKey, ext := newID(), newID()
	bet := wager{ProviderID: "alpha", ExternalID: ext, PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "10.00"}
	r := postWager(t, a.base, "provider-alpha", alphaKey, bet)
	if r.Status != 200 {
		t.Fatalf("alpha bet: %d %s", r.Status, r.Raw)
	}
	txID := r.Body["transactionId"].(string)
	beta := token(t, "provider-beta")

	// Beta cannot read alpha's transaction by id (indistinguishable from not found)...
	if r := call(t, "GET", a.base+"/wagering/transactions/"+txID, beta, nil, nil); r.Status != 404 || strings.Contains(r.Raw, walletID) {
		t.Fatalf("beta read alpha tx: %d %s", r.Status, r.Raw)
	}
	// ...nor through alpha's provider path...
	if r := call(t, "GET", a.base+"/providers/alpha/wagering/transactions/"+ext, beta, nil, nil); r.Status != 403 || strings.Contains(r.Raw, walletID) {
		t.Fatalf("beta read alpha path: %d %s", r.Status, r.Raw)
	}
	// ...nor replay it by impersonating alpha in the body.
	if r := postWager(t, a.base, "provider-beta", alphaKey, bet); r.Status != 403 || strings.Contains(r.Raw, txID) {
		t.Fatalf("beta replay as alpha: %d %s", r.Status, r.Raw)
	}
	// Beta's own namespace does not see alpha's external id.
	if r := call(t, "GET", a.base+"/providers/beta/wagering/transactions/"+ext, beta, nil, nil); r.Status != 404 {
		t.Fatalf("beta namespace: %d %s", r.Status, r.Raw)
	}
	// Internal client may read any transaction.
	if r := call(t, "GET", a.base+"/wagering/transactions/"+txID, token(t, "wallet-admin"), nil, nil); r.Status != 200 {
		t.Fatalf("internal read: %d", r.Status)
	}

	// Providers cannot use internal wallet endpoints.
	alpha := token(t, "provider-alpha")
	otherPlayer := "player-" + newID()
	for _, c := range []struct{ method, path string }{
		{"POST", "/wallets"},
		{"GET", "/wallets/" + walletID},
		{"GET", "/wallets/" + walletID + "/ledger"},
		{"POST", "/wallets/" + walletID + "/reconciliation"},
	} {
		r := call(t, c.method, a.base+c.path, alpha, map[string]any{"playerId": otherPlayer,
			"initialBalance": map[string]string{"amount": "1000.00", "currency": "BRL"}}, nil)
		if r.Status != 403 || strings.Contains(r.Raw, "balance") {
			t.Fatalf("%s %s by provider: %d %s", c.method, c.path, r.Status, r.Raw)
		}
	}
	if countRows(t, `SELECT count(*) FROM wallets WHERE player_id = $1`, otherPlayer) != 0 {
		t.Fatal("provider created a wallet")
	}
	if bal, _ := walletBalance(t, walletID); bal != 9000 {
		t.Fatalf("balance changed by unauthorized calls: %d", bal)
	}
	if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND origin = 'EXTERNAL'`, walletID); n != 1 {
		t.Fatalf("expected only alpha's bet, got %d", n)
	}
}
