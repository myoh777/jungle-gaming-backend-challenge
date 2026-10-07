// Package pendingref runs the background loop that retries PENDING_REFERENCE
// transactions. State lives in PostgreSQL (status, attempts,
// next_reference_attempt_at), so retries survive restarts and any instance
// can pick them up.
package pendingref

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Resolver is app.ReferenceResolver.
type Resolver interface {
	ResolveDue(ctx context.Context, limit int) (int, error)
}

type Worker struct {
	resolver Resolver
	interval time.Duration
	batch    int
	log      *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func NewWorker(resolver Resolver, interval time.Duration, batch int, log *slog.Logger) *Worker {
	return &Worker{resolver: resolver, interval: interval, batch: batch, log: log.With("component", "reference-worker")}
}

func (w *Worker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.done = make(chan struct{})
	go w.run(ctx)
	w.log.Info("reference worker started")
}

// Stop cancels the loop; an in-flight SQL transaction is rolled back by
// context cancellation and simply retried later by some instance.
func (w *Worker) Stop(ctx context.Context) error {
	if w.cancel == nil {
		return nil
	}
	w.once.Do(w.cancel)
	select {
	case <-w.done:
		w.log.Info("reference worker stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		n, err := w.resolver.ResolveDue(ctx, w.batch)
		if err != nil && ctx.Err() == nil {
			w.log.Warn("reference retry batch failed", "error", err.Error())
		}
		if n >= w.batch && ctx.Err() == nil {
			continue // more due work
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
