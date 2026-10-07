//go:build integration

// Package integration runs the service against real PostgreSQL, Keycloak and
// LocalStack started by docker compose. See README.md ("Testes de integração").
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"wagering/internal/config"
	"wagering/internal/fxapp"
	"wagering/internal/postgres"
	"wagering/internal/sqsauth"
	"wagering/internal/sqsx"
	"wagering/migrations"
)

// Defaults use 127.0.0.1 rather than localhost: on Windows, localhost may
// resolve to ::1 first, where another process (e.g. a WSL/Hyper-V relay) can
// accept the connection and never answer, so tests hang instead of failing.
var env = struct {
	DatabaseURL      string
	AdminDatabaseURL string
	AWSEndpoint      string
	KeycloakURL      string // where tokens and keys are fetched
	OIDCIssuer       string // the "iss" Keycloak puts in tokens (fixed by KC_HOSTNAME)
}{
	DatabaseURL:      getenv("TEST_DATABASE_URL", "postgres://wagering:wagering@127.0.0.1:5432/wagering_test?sslmode=disable"),
	AdminDatabaseURL: getenv("TEST_ADMIN_DATABASE_URL", "postgres://wagering:wagering@127.0.0.1:5432/postgres?sslmode=disable"),
	AWSEndpoint:      getenv("TEST_AWS_ENDPOINT_URL", "http://127.0.0.1:4566"),
	KeycloakURL:      getenv("TEST_KEYCLOAK_URL", "http://127.0.0.1:8081"),
	OIDCIssuer:       getenv("TEST_OIDC_ISSUER", "http://localhost:8081/realms/wagering"),
}

// Client credentials provisioned by deploy/keycloak/wagering-realm.json (local only).
var clients = map[string]string{
	"provider-alpha":       "provider-alpha-local-secret",
	"provider-beta":        "provider-beta-local-secret",
	"provider-alpha-short": "provider-alpha-short-local-secret",
	"wallet-admin":         "wallet-admin-local-secret",
	"no-role-client":       "no-role-client-local-secret",
}

// Fictitious HMAC keys for the SQS gateway signature (local tests only).
var signingKeys = sqsauth.Keys{
	"alpha": []byte("LOCAL-ONLY-FAKE-KEY-provider-alpha-do-not-use"),
	"beta":  []byte("LOCAL-ONLY-FAKE-KEY-provider-beta-do-not-use!"),
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	db        *pgxpool.Pool
	sqsClient *sqs.Client
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	var err error
	db, err = pgxpool.New(ctx, env.DatabaseURL)
	if err == nil {
		err = db.Ping(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests need `docker compose up -d postgres keycloak localstack`:", err)
		os.Exit(1)
	}
	if _, err := postgres.MigrateUp(ctx, db, migrations.FS); err != nil {
		fmt.Fprintln(os.Stderr, "migrate test database:", err)
		os.Exit(1)
	}
	sqsClient, err = sqsx.NewClient(ctx, config.Config{AWSRegion: "us-east-1", AWSEndpoint: env.AWSEndpoint, AWSAccessKeyID: "test", AWSSecretKey: "test"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "sqs client:", err)
		os.Exit(1)
	}
	cancel()
	code := m.Run()
	db.Close()
	os.Exit(code)
}

// ---------- queues ----------

type testQueues struct {
	Wager, DLQ, Events          string // names
	WagerURL, DLQURL, EventsURL string
}

// createQueues creates an isolated FIFO queue + FIFO DLQ (redrive after
// maxReceive receives) + standard events queue for one test.
func createQueues(t *testing.T, maxReceive int) testQueues {
	t.Helper()
	ctx := context.Background()
	prefix := "it-" + uuid.NewString()[:8]
	q := testQueues{Wager: prefix + "-wager.fifo", DLQ: prefix + "-dlq.fifo", Events: prefix + "-events"}

	dlq, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(q.DLQ),
		Attributes: map[string]string{"FifoQueue": "true"}})
	must(t, err)
	q.DLQURL = aws.ToString(dlq.QueueUrl)
	attrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: dlq.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	must(t, err)
	redrive := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"%d"}`, attrs.Attributes["QueueArn"], maxReceive)
	wq, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(q.Wager),
		Attributes: map[string]string{"FifoQueue": "true", "VisibilityTimeout": "5", "RedrivePolicy": redrive}})
	must(t, err)
	q.WagerURL = aws.ToString(wq.QueueUrl)
	eq, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(q.Events)})
	must(t, err)
	q.EventsURL = aws.ToString(eq.QueueUrl)

	t.Cleanup(func() {
		for _, u := range []string{q.WagerURL, q.DLQURL, q.EventsURL} {
			_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		}
	})
	return q
}

// ---------- app ----------

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	addr := l.Addr().String()
	l.Close()
	return addr
}

// baseConfig returns a valid config with every worker disabled; tests enable
// what they exercise.
func baseConfig(t *testing.T, q testQueues) config.Config {
	t.Helper()
	cfg := config.Config{
		InstanceID: "it-" + uuid.NewString()[:8], LogLevel: getenv("TEST_LOG_LEVEL", "warn"),
		HTTPAddr: freeAddr(t), ShutdownTimeout: 10 * time.Second,
		DatabaseURL: env.DatabaseURL, DBMaxConns: 20,
		AWSRegion: "us-east-1", AWSEndpoint: env.AWSEndpoint, AWSAccessKeyID: "test", AWSSecretKey: "test",
		WagerQueueName: q.Wager, WagerDLQName: q.DLQ, EventsQueueName: q.Events,
		OIDCIssuer:     env.OIDCIssuer,
		OIDCJWKSURL:    env.KeycloakURL + "/realms/wagering/protocol/openid-connect/certs",
		OIDCAudience:   "wagering-api",
		SQSSigningKeys: signingKeys,

		ConsumerName: "it-consumer", ConsumerWaitSeconds: 1, ConsumerVisibilityTimeout: 5, ConsumerMaxMessages: 10,
		ConsumerProcessTimeout: 4 * time.Second, ConsumerRetryBase: time.Second, ConsumerRetryMax: 2 * time.Second,

		PublisherPollInterval: 100 * time.Millisecond, PublisherBatchSize: 50, PublisherLease: 30 * time.Second,
		PublisherRetryBase: 200 * time.Millisecond, PublisherRetryMax: 2 * time.Second,

		ReferencePollInterval: 100 * time.Millisecond, ReferenceBatchSize: 50,
		ReferenceRetryBase: 200 * time.Millisecond, ReferenceRetryMax: time.Second,
		ReferenceMaxAttempts: 10, ReferenceTTL: time.Hour,
	}
	must(t, cfg.Validate())
	return cfg
}

type runningApp struct {
	app  *fx.App
	base string
}

// startApp starts the real Fx application in-process and stops it at cleanup.
func startApp(t *testing.T, cfg config.Config, extra ...fx.Option) *runningApp {
	t.Helper()
	a := fxapp.New(cfg, extra...)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	must(t, a.Start(ctx))
	ra := &runningApp{app: a, base: "http://" + cfg.HTTPAddr}
	t.Cleanup(func() { ra.stop(t) })
	return ra
}

func (r *runningApp) stop(t *testing.T) {
	if r.app == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := r.app.Stop(ctx); err != nil {
		t.Errorf("stop app: %v", err)
	}
	r.app = nil
}

// ---------- tokens ----------

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]string{}
)

func fetchToken(t *testing.T, client string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {clients[client]}}
	resp, err := http.PostForm(env.KeycloakURL+"/realms/wagering/protocol/openid-connect/token", form)
	must(t, err)
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	must(t, json.NewDecoder(resp.Body).Decode(&body))
	if resp.StatusCode != 200 || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d", client, resp.StatusCode)
	}
	return body.AccessToken
}

// token returns a cached token (lifespan 300s; tests run well within it).
func token(t *testing.T, client string) string {
	t.Helper()
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if tok, ok := tokenCache[client]; ok {
		return tok
	}
	tok := fetchToken(t, client)
	tokenCache[client] = tok
	return tok
}

// ---------- HTTP ----------

type response struct {
	Status int
	Body   map[string]any
	Raw    string
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

func call(t *testing.T, method, url, tok string, body any, headers map[string]string) response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rdr = strings.NewReader(b)
		default:
			raw, err := json.Marshal(b)
			must(t, err)
			rdr = bytes.NewReader(raw)
		}
	}
	req, err := http.NewRequest(method, url, rdr)
	must(t, err)
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	must(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := response{Status: resp.StatusCode, Raw: string(raw)}
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

func createWallet(t *testing.T, base, player, amount string) string {
	t.Helper()
	r := call(t, "POST", base+"/wallets", token(t, "wallet-admin"), map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	}, nil)
	if r.Status != 201 {
		t.Fatalf("create wallet: %d %s", r.Status, r.Raw)
	}
	return r.Body["walletId"].(string)
}

type wager struct {
	ProviderID, ExternalID, PlayerID, WalletID, Kind, Amount, Reference, Round string
}

func (w wager) body() map[string]any {
	round := w.Round
	if round == "" {
		round = "round-1"
	}
	b := map[string]any{
		"providerId": w.ProviderID, "externalTransactionId": w.ExternalID, "playerId": w.PlayerID,
		"walletId": w.WalletID, "roundId": round, "gameId": "game-1", "kind": w.Kind,
		"money": map[string]string{"amount": w.Amount, "currency": "BRL"},
	}
	if w.Reference != "" {
		b["referenceExternalTransactionId"] = w.Reference
	}
	return b
}

func postWager(t *testing.T, base, client, key string, w wager) response {
	t.Helper()
	return call(t, "POST", base+"/wagering/transactions", token(t, client), w.body(), map[string]string{"Idempotency-Key": key})
}

// ---------- database assertions ----------

func walletBalance(t *testing.T, walletID string) (balance, version int64) {
	t.Helper()
	must(t, db.QueryRow(context.Background(), `SELECT balance_amount, version FROM wallets WHERE id = $1`, walletID).Scan(&balance, &version))
	return
}

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	must(t, db.QueryRow(context.Background(), query, args...).Scan(&n))
	return n
}

// ledgerNet is SUM(credits) - SUM(debits) for a wallet.
func ledgerNet(t *testing.T, walletID string) int64 {
	t.Helper()
	var n int64
	must(t, db.QueryRow(context.Background(), `
		SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)::bigint
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&n))
	return n
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func newID() string { return uuid.NewString() }
