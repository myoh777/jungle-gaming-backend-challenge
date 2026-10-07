package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"wagering/internal/auth"
	"wagering/internal/observability"
)

// fakeVerifier maps raw tokens to principals. These tests cover the HTTP
// guard rails only; the services are nil, so any request that wrongly reached
// a use case would panic and surface as a 500 instead of the expected code.
type fakeVerifier map[string]auth.Principal

func (f fakeVerifier) Verify(_ context.Context, raw string) (auth.Principal, error) {
	p, ok := f[raw]
	if !ok {
		return auth.Principal{}, auth.ErrUnauthenticated
	}
	return p, nil
}

func newTestHandler() http.Handler {
	v := fakeVerifier{
		"alpha":    {ProviderID: "alpha", Roles: []string{auth.RoleProvider}},
		"internal": {Roles: []string{auth.RoleInternal}},
		"norole":   {ProviderID: "alpha"},
	}
	h := NewHandler(nil, nil, v, nil, observability.NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	return h.Routes()
}

func do(t *testing.T, h http.Handler, method, path, token, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

const wagerBody = `{"providerId":"%s","externalTransactionId":"e1","playerId":"p1",
	"walletId":"0190a0e0-0000-7000-8000-000000000001","roundId":"r1","gameId":"g1","kind":"BET",
	"money":{"amount":"10.00","currency":"BRL"}}`

func TestAuthGuards(t *testing.T) {
	h := newTestHandler()
	key := map[string]string{"Idempotency-Key": "k1"}
	cases := []struct {
		name, method, path, token, body string
		headers                         map[string]string
		want                            int
	}{
		{"no token", "POST", "/wagering/transactions", "", "{}", key, 401},
		{"invalid token", "POST", "/wagering/transactions", "forged", "{}", key, 401},
		{"no token on wallet", "GET", "/wallets/abc", "", "", nil, 401},
		{"no token on reconciliation", "POST", "/wallets/abc/reconciliation", "", "", nil, 401},
		{"provider cannot create wallet", "POST", "/wallets", "alpha", `{}`, nil, 403},
		{"provider cannot read wallet", "GET", "/wallets/abc", "alpha", "", nil, 403},
		{"provider cannot read ledger", "GET", "/wallets/abc/ledger", "alpha", "", nil, 403},
		{"provider cannot reconcile", "POST", "/wallets/abc/reconciliation", "alpha", "", nil, 403},
		{"internal cannot post wagers", "POST", "/wagering/transactions", "internal", "{}", key, 403},
		{"token without role", "POST", "/wagering/transactions", "norole", "{}", key, 403},
		{"provider acting as another", "POST", "/wagering/transactions", "alpha", strings.Replace(wagerBody, "%s", "beta", 1), key, 403},
		{"provider reading another provider", "GET", "/providers/beta/wagering/transactions/e1", "alpha", "", nil, 403},
		{"missing idempotency key", "POST", "/wagering/transactions", "alpha", strings.Replace(wagerBody, "%s", "alpha", 1), nil, 400},
		{"numeric amount", "POST", "/wagering/transactions", "alpha", `{"providerId":"alpha","money":{"amount":10.0,"currency":"BRL"}}`, key, 400},
		{"unknown field", "POST", "/wagering/transactions", "alpha", `{"providerId":"alpha","extra":1}`, key, 400},
		{"malformed ledger query", "GET", "/wallets/abc/ledger?cursor=%%%", "internal", "", nil, 400},
		{"liveness is public", "GET", "/health/live", "", "", nil, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, tc.method, tc.path, tc.token, tc.body, tc.headers)
			if rec.Code != tc.want {
				t.Fatalf("got %d (%s), want %d", rec.Code, rec.Body.String(), tc.want)
			}
			if tc.want == 401 && rec.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("401 must carry WWW-Authenticate")
			}
		})
	}
}

func TestCorrelationIDPropagated(t *testing.T) {
	rec := do(t, newTestHandler(), "GET", "/health/live", "", "", map[string]string{"X-Correlation-ID": "corr-123"})
	if rec.Header().Get("X-Correlation-ID") != "corr-123" {
		t.Fatal("correlation id must be echoed")
	}
	rec = do(t, newTestHandler(), "GET", "/health/live", "", "", nil)
	if rec.Header().Get("X-Correlation-ID") == "" {
		t.Fatal("correlation id must be generated")
	}
}

func TestReadinessDraining(t *testing.T) {
	h := NewHandler(nil, nil, fakeVerifier{}, map[string]ReadinessCheck{
		"postgres": func(context.Context) error { return nil },
	}, observability.NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if rec := do(t, h.Routes(), "GET", "/health/ready", "", "", nil); rec.Code != 200 {
		t.Fatalf("ready: %d", rec.Code)
	}
	h.SetDraining()
	if rec := do(t, h.Routes(), "GET", "/health/ready", "", "", nil); rec.Code != 503 {
		t.Fatalf("draining must be 503, got %d", rec.Code)
	}
}

func TestReadinessDependencyDown(t *testing.T) {
	h := NewHandler(nil, nil, fakeVerifier{}, map[string]ReadinessCheck{
		"sqs": func(context.Context) error { return io.ErrUnexpectedEOF },
	}, observability.NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := do(t, h.Routes(), "GET", "/health/ready", "", "", nil)
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), `"sqs":"down"`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}
