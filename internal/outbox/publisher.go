// Package outbox publishes committed outbox events. Events are only visible to
// the publisher after the SQL transaction that created them commits, so
// nothing is ever published before commit. Delivery is at-least-once: an event
// published but not yet confirmed (crash, lost lease) is published again with
// the same eventId, and consumers deduplicate by eventId.
package outbox

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"wagering/internal/observability"
)

// Record is one claimed outbox row. Payload is the immutable event envelope.
type Record struct {
	EventID     string
	EventType   string
	AggregateID string
	Payload     []byte
	Attempts    int
	Seq         int64
}

// Repository is the persistence port of the publisher.
type Repository interface {
	Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]Record, error)
	MarkPublished(ctx context.Context, eventID, owner string) (bool, error)
	MarkFailed(ctx context.Context, eventID, owner, lastError string, retryAfter time.Duration) error
	OldestPendingAge(ctx context.Context) (time.Duration, error)
}

// Sender delivers one event to the broker.
type Sender interface {
	Send(ctx context.Context, eventID, eventType string, payload []byte) error
}

// Config of the publisher loop.
type Config struct {
	Owner        string // unique per publisher instance
	PollInterval time.Duration
	BatchSize    int
	Lease        time.Duration
	RetryBase    time.Duration
	RetryMax     time.Duration
}

// Publisher drains the outbox in a loop. Several publishers (in one or many
// processes) can run at the same time.
type Publisher struct {
	repo    Repository
	sender  Sender
	cfg     Config
	metrics *observability.Metrics
	log     *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func NewPublisher(repo Repository, sender Sender, cfg Config, metrics *observability.Metrics, log *slog.Logger) *Publisher {
	return &Publisher{repo: repo, sender: sender, cfg: cfg, metrics: metrics, log: log.With("component", "outbox-publisher", "owner", cfg.Owner)}
}

// Start launches the loop. It returns immediately.
func (p *Publisher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.done = make(chan struct{})
	go p.run(ctx)
	p.log.Info("outbox publisher started")
}

// Stop stops claiming new events and waits for the current batch, up to ctx's
// deadline. Unconfirmed events keep their lease and are retried after it expires.
func (p *Publisher) Stop(ctx context.Context) error {
	if p.cancel == nil {
		return nil
	}
	p.once.Do(p.cancel)
	select {
	case <-p.done:
		p.log.Info("outbox publisher stopped")
		return nil
	case <-ctx.Done():
		p.log.Warn("outbox publisher stop timed out; leases will expire and events will be republished")
		return ctx.Err()
	}
}

func (p *Publisher) run(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		// Drain while there is work, then wait for the next tick.
		for {
			n, err := p.PublishBatch(ctx)
			if err != nil && ctx.Err() == nil {
				p.log.Warn("outbox batch failed", "error", err.Error())
			}
			if n < p.cfg.BatchSize || err != nil || ctx.Err() != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// PublishBatch claims and publishes one batch; returns the number claimed.
// The in-flight batch runs on a context detached from cancellation so a stop
// request does not abort a send whose confirmation would then be lost.
func (p *Publisher) PublishBatch(ctx context.Context) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if age, err := p.repo.OldestPendingAge(ctx); err == nil {
		p.metrics.OutboxOldestPending.Set(age.Seconds())
	}
	recs, err := p.repo.Claim(ctx, p.cfg.Owner, p.cfg.Lease, p.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.Lease)
	defer cancel()
	for _, rec := range recs {
		p.publishOne(work, rec)
	}
	return len(recs), nil
}

func (p *Publisher) publishOne(ctx context.Context, rec Record) {
	log := p.log.With("eventId", rec.EventID, "eventType", rec.EventType, "aggregateId", rec.AggregateID, "attempt", rec.Attempts)
	if err := p.sender.Send(ctx, rec.EventID, rec.EventType, rec.Payload); err != nil {
		p.metrics.OutboxPublishFailures.Inc()
		retry := p.backoff(rec.Attempts)
		log.Warn("outbox publish failed", "error", err.Error(), "retryIn", retry.String())
		if merr := p.repo.MarkFailed(ctx, rec.EventID, p.cfg.Owner, err.Error(), retry); merr != nil {
			log.Error("could not reschedule outbox event; lease expiry will recover it", "error", merr.Error())
		}
		return
	}
	confirmed, err := p.repo.MarkPublished(ctx, rec.EventID, p.cfg.Owner)
	switch {
	case err != nil:
		// Published but not confirmed: it will be republished with the same eventId.
		log.Warn("outbox event published but not confirmed", "error", err.Error())
	case !confirmed:
		log.Warn("outbox lease lost before confirmation; event may be published twice")
	default:
		p.metrics.OutboxPublished.Inc()
		log.Debug("outbox event published")
	}
}

func (p *Publisher) backoff(attempts int) time.Duration {
	d := p.cfg.RetryBase
	for i := 1; i < attempts && d < p.cfg.RetryMax; i++ {
		d *= 2
	}
	if d > p.cfg.RetryMax {
		d = p.cfg.RetryMax
	}
	return d
}
