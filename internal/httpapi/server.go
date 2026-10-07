// Package httpapi exposes the REST API. Every business endpoint requires a
// valid bearer token from the external IdP; health checks and /metrics do not.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"wagering/internal/app"
	"wagering/internal/auth"
	"wagering/internal/observability"
)

// TokenVerifier is implemented by auth.Verifier.
type TokenVerifier interface {
	Verify(ctx context.Context, raw string) (auth.Principal, error)
}

// ReadinessCheck reports whether a dependency is usable.
type ReadinessCheck func(ctx context.Context) error

// Handler holds the HTTP dependencies.
type Handler struct {
	wagers   *app.WagerService
	wallets  *app.WalletService
	verifier TokenVerifier
	checks   map[string]ReadinessCheck
	metrics  *observability.Metrics
	log      *slog.Logger
	draining atomic.Bool
}

func NewHandler(wagers *app.WagerService, wallets *app.WalletService, verifier TokenVerifier,
	checks map[string]ReadinessCheck, metrics *observability.Metrics, log *slog.Logger) *Handler {
	return &Handler{wagers: wagers, wallets: wallets, verifier: verifier, checks: checks, metrics: metrics, log: log}
}

// SetDraining makes readiness fail so load balancers stop routing new traffic
// during shutdown.
func (h *Handler) SetDraining() { h.draining.Store(true) }

type access int

const (
	internalOnly access = iota
	providerOnly
	providerOrInternal
)

// Routes builds the router.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, acc access, fn func(http.ResponseWriter, *http.Request, auth.Principal)) {
		mux.Handle(pattern, h.instrument(pattern, h.authenticate(acc, fn)))
	}
	route("POST /wallets", internalOnly, h.createWallet)
	route("GET /wallets/{walletId}", internalOnly, h.getWallet)
	route("GET /wallets/{walletId}/ledger", internalOnly, h.getLedger)
	route("POST /wallets/{walletId}/reconciliation", internalOnly, h.reconcile)
	route("POST /wagering/transactions", providerOnly, h.postTransaction)
	route("GET /wagering/transactions/{transactionId}", providerOrInternal, h.getTransaction)
	route("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", providerOrInternal, h.getTransactionByExternalID)

	mux.Handle("GET /health/live", h.instrument("GET /health/live", http.HandlerFunc(h.live)))
	mux.Handle("GET /health/ready", h.instrument("GET /health/ready", http.HandlerFunc(h.ready)))
	mux.Handle("GET /metrics", promhttp.HandlerFor(h.metrics.Registry, promhttp.HandlerOpts{}))
	return h.recoverer(mux)
}

type ctxKey int

const correlationKey ctxKey = 1

func correlationFrom(r *http.Request) string {
	v, _ := r.Context().Value(correlationKey).(string)
	return v
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// instrument assigns a correlation id, records latency and logs the request.
// Request bodies, tokens and headers other than the correlation id are never logged.
func (h *Handler) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corr := r.Header.Get("X-Correlation-ID")
		if corr == "" || len(corr) > 128 {
			corr = app.NewID()
		}
		w.Header().Set("X-Correlation-ID", corr)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), correlationKey, corr)))
		elapsed := time.Since(start)
		h.metrics.HTTPDuration.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).Observe(elapsed.Seconds())
		if !strings.HasPrefix(route, "GET /health") {
			h.log.Info("http request", "route", route, "status", rec.status,
				"durationMs", elapsed.Milliseconds(), "correlationId", corr)
		}
	})
}

// authenticate verifies the bearer token and enforces the access rule before
// the handler runs, so unauthorized calls never reach a use case.
func (h *Handler) authenticate(acc access, fn func(http.ResponseWriter, *http.Request, auth.Principal)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || strings.TrimSpace(raw) == "" {
			w.Header().Set("WWW-Authenticate", `Bearer`)
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "missing bearer token")
			return
		}
		p, err := h.verifier.Verify(r.Context(), strings.TrimSpace(raw))
		if err != nil {
			h.log.Info("token rejected", "correlationId", correlationFrom(r), "reason", err.Error())
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "invalid or expired token")
			return
		}
		allowed := false
		switch acc {
		case internalOnly:
			allowed = p.IsInternal()
		case providerOnly:
			allowed = p.IsProvider()
		case providerOrInternal:
			allowed = p.IsInternal() || p.IsProvider()
		}
		if !allowed {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "client is not allowed to call this endpoint")
			return
		}
		fn(w, r, p)
	})
}

func (h *Handler) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				h.log.Error("panic in handler", "panic", v, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "draining"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	result := map[string]string{}
	ok := true
	for name, check := range h.checks {
		if err := check(ctx); err != nil {
			ok = false
			result[name] = "down"
			h.log.Warn("readiness check failed", "dependency", name, "error", err.Error())
		} else {
			result[name] = "up"
		}
	}
	status := http.StatusOK
	state := "ready"
	if !ok {
		status, state = http.StatusServiceUnavailable, "not_ready"
	}
	writeJSON(w, status, map[string]any{"status": state, "checks": result})
}

// ---------- responses ----------

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg}})
}

// writeAppError maps use-case errors to HTTP. Internal details are logged, not returned.
func (h *Handler) writeAppError(w http.ResponseWriter, r *http.Request, err error) {
	var invalid *app.InvalidInputError
	switch {
	case errors.As(err, &invalid):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: errorDetail{Code: "INVALID_INPUT", Message: invalid.Reason, Field: invalid.Field}})
	case errors.Is(err, app.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found")
	case errors.Is(err, app.ErrTransactionNotFound):
		writeError(w, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found")
	case errors.Is(err, app.ErrWalletAlreadyExists):
		writeError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", err.Error())
	case errors.Is(err, app.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", err.Error())
	case errors.Is(err, app.ErrDuplicateExternalTransaction):
		writeError(w, http.StatusConflict, "DUPLICATE_EXTERNAL_TRANSACTION", err.Error())
	case errors.Is(err, app.ErrUnavailable), errors.Is(err, context.DeadlineExceeded):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE", "temporarily unavailable, retry with the same Idempotency-Key")
	default:
		h.log.Error("unexpected error", "correlationId", correlationFrom(r), "error", err.Error())
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
	}
}
