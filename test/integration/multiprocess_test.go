//go:build integration

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// buildServer compiles cmd/server once per test run.
var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

func serverBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "wagering-bin")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "server")
		if runtime.GOOS == "windows" {
			binPath += ".exe"
		}
		out, err := exec.Command("go", "build", "-o", binPath, "../../cmd/server").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

// startProcess runs the service as a separate OS process (own memory and
// connection pool) and waits until it is ready.
func startProcess(t *testing.T, q testQueues, name string) string {
	t.Helper()
	addr := freeAddr(t)
	cmd := exec.Command(serverBinary(t))
	cmd.Env = append(os.Environ(),
		"INSTANCE_ID="+name, "LOG_LEVEL=warn", "HTTP_ADDR="+addr, "DATABASE_URL="+env.DatabaseURL,
		"AWS_REGION=us-east-1", "AWS_ENDPOINT_URL="+env.AWSEndpoint, "AWS_ACCESS_KEY_ID=test", "AWS_SECRET_ACCESS_KEY=test",
		"SQS_WAGER_QUEUE="+q.Wager, "SQS_WAGER_DLQ="+q.DLQ, "SQS_EVENTS_QUEUE="+q.Events,
		"OIDC_ISSUER="+env.OIDCIssuer,
		"OIDC_JWKS_URL="+env.KeycloakURL+"/realms/wagering/protocol/openid-connect/certs",
		"CONSUMER_ENABLED=false", "PUBLISHER_ENABLED=false", "REFERENCE_WORKER_ENABLED=false",
	)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	must(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	base := "http://" + addr
	eventually(t, 30*time.Second, func() bool {
		resp, err := httpClient.Get(base + "/health/ready")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	}, name+" ready")
	return base
}

// TestThreeProcessesCompetingBets: three independent processes receive two
// different 80.00 bets on a 100.00 wallet at the same time. Exactly one bet
// wins, the other is rejected for insufficient funds, the balance is 20.00 and
// there is one debit. Re-sending everything does not change the outcome.
func TestThreeProcessesCompetingBets(t *testing.T) {
	q := createQueues(t, 5)
	bases := []string{startProcess(t, q, "proc-1"), startProcess(t, q, "proc-2"), startProcess(t, q, "proc-3")}
	player := "player-" + newID()
	walletID := createWallet(t, bases[0], player, "100.00")

	type bet struct {
		key string
		w   wager
	}
	bets := []bet{
		{newID(), wager{ProviderID: "alpha", ExternalID: "bet-a-" + newID(), PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "80.00"}},
		{newID(), wager{ProviderID: "alpha", ExternalID: "bet-b-" + newID(), PlayerID: player, WalletID: walletID, Kind: "BET", Amount: "80.00"}},
	}

	send := func() map[string]map[string]bool {
		var mu sync.Mutex
		var wg sync.WaitGroup
		statuses := map[string]map[string]bool{} // externalId -> set of statuses
		start := make(chan struct{})
		for _, b := range bets {
			for _, base := range bases {
				wg.Add(1)
				go func(b bet, base string) {
					defer wg.Done()
					<-start
					r := postWager(t, base, "provider-alpha", b.key, b.w)
					mu.Lock()
					defer mu.Unlock()
					if statuses[b.w.ExternalID] == nil {
						statuses[b.w.ExternalID] = map[string]bool{}
					}
					statuses[b.w.ExternalID][fmt.Sprint(r.Body["status"], "/", r.Body["failureCode"])] = true
				}(b, base)
			}
		}
		close(start)
		wg.Wait()
		return statuses
	}

	check := func(round string, statuses map[string]map[string]bool) {
		processed, rejected := 0, 0
		for ext, set := range statuses {
			if len(set) != 1 {
				t.Fatalf("%s: %s got inconsistent answers %v", round, ext, set)
			}
			switch {
			case set["PROCESSED/<nil>"]:
				processed++
			case set["REJECTED/INSUFFICIENT_FUNDS"]:
				rejected++
			default:
				t.Fatalf("%s: unexpected outcome %v", round, set)
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("%s: processed=%d rejected=%d", round, processed, rejected)
		}
		if bal, version := walletBalance(t, walletID); bal != 2000 || version != 2 {
			t.Fatalf("%s: balance=%d version=%d", round, bal, version)
		}
		if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 1 {
			t.Fatalf("%s: debits=%d", round, n)
		}
		if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND origin = 'EXTERNAL'`, walletID); n != 2 {
			t.Fatalf("%s: transactions=%d", round, n)
		}
	}

	check("first round", send())
	check("resend", send())
	if ledgerNet(t, walletID) != 2000 {
		t.Fatal("ledger does not match balance")
	}
}
