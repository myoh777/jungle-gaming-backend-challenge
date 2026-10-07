// Package observability provides the JSON logger and Prometheus metrics.
package observability

import (
	"log/slog"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// NewLogger returns a JSON slog logger. Callers must only log identifiers
// (correlationId, messageId, transactionId, walletId, providerId), never
// tokens, secrets or full financial payloads.
func NewLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

// Metrics groups every application metric. Each instance owns its own
// registry so tests can build several apps in one process.
type Metrics struct {
	Registry *prometheus.Registry

	WagerResults          *prometheus.CounterVec // labels: source, status
	IdempotentReplays     *prometheus.CounterVec // labels: source
	IdempotencyConflicts  *prometheus.CounterVec // labels: reason
	ConcurrencyRetries    prometheus.Counter
	SQSMessages           *prometheus.CounterVec // labels: result
	SQSRetries            prometheus.Counter
	SQSDeadLettered       prometheus.Counter
	OutboxPublished       prometheus.Counter
	OutboxPublishFailures prometheus.Counter
	OutboxOldestPending   prometheus.Gauge
	ReferenceRetries      prometheus.Counter
	ReconciliationRuns    *prometheus.CounterVec // labels: consistent
	ReconciliationDiverge prometheus.Counter
	HTTPDuration          *prometheus.HistogramVec // labels: route, method, code
	WagerDuration         *prometheus.HistogramVec // labels: source
}

// NewMetrics registers all metrics on a fresh registry.
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { r.MustRegister(c) }

	m := &Metrics{Registry: r}
	m.WagerResults = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wager_transactions_total", Help: "Wager transaction outcomes by source and final status.",
	}, []string{"source", "status"})
	m.IdempotentReplays = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wager_idempotent_replays_total", Help: "Duplicate requests answered from the persisted result.",
	}, []string{"source"})
	m.IdempotencyConflicts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wager_idempotency_conflicts_total", Help: "Idempotency key or external id reuse with different payload.",
	}, []string{"reason"})
	m.ConcurrencyRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "db_concurrency_retries_total", Help: "SQL transactions retried after serialization failure, deadlock, lock timeout or version conflict.",
	})
	m.SQSMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sqs_messages_total", Help: "SQS messages handled by result.",
	}, []string{"result"})
	m.SQSRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sqs_message_retries_total", Help: "SQS messages left for redelivery after a transient failure.",
	})
	m.SQSDeadLettered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sqs_messages_dead_lettered_total", Help: "SQS messages explicitly sent to the DLQ by this consumer.",
	})
	m.OutboxPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_events_published_total", Help: "Outbox events published and confirmed.",
	})
	m.OutboxPublishFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_publish_failures_total", Help: "Outbox publish attempts that failed and were rescheduled.",
	})
	m.OutboxOldestPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_oldest_pending_age_seconds", Help: "Age of the oldest unpublished outbox event (outbox lag).",
	})
	m.ReferenceRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pending_reference_retries_total", Help: "Resolution attempts of PENDING_REFERENCE transactions.",
	})
	m.ReconciliationRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "reconciliation_runs_total", Help: "Wallet reconciliations by result.",
	}, []string{"consistent"})
	m.ReconciliationDiverge = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "reconciliation_divergences_total", Help: "Reconciliations where stored balance differs from the ledger.",
	})
	m.HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds", Help: "HTTP latency.", Buckets: prometheus.DefBuckets,
	}, []string{"route", "method", "code"})
	m.WagerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "wager_processing_duration_seconds", Help: "Latency of the wager use case.", Buckets: prometheus.DefBuckets,
	}, []string{"source"})

	for _, c := range []prometheus.Collector{
		m.WagerResults, m.IdempotentReplays, m.IdempotencyConflicts, m.ConcurrencyRetries,
		m.SQSMessages, m.SQSRetries, m.SQSDeadLettered, m.OutboxPublished, m.OutboxPublishFailures,
		m.OutboxOldestPending, m.ReferenceRetries, m.ReconciliationRuns, m.ReconciliationDiverge,
		m.HTTPDuration, m.WagerDuration,
	} {
		f(c)
	}
	return m
}
