package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"wagering/internal/domain/wagering"
	"wagering/internal/observability"
)

// FailurePermanentInternal is recorded when a pending transaction cannot be
// resolved because of a non-transient infrastructure/invariant error, so the
// worker stops retrying it and it remains auditable as FAILED.
const FailurePermanentInternal wagering.FailureCode = "INTERNAL_PERMANENT_ERROR"

// ReferenceResolver retries PENDING_REFERENCE transactions. It is safe to run
// in many instances: each attempt locks the wallet, then re-reads and locks the
// transaction, and skips it if another instance already advanced it.
type ReferenceResolver struct {
	store   Store
	cfg     WagerConfig
	metrics *observability.Metrics
	log     *slog.Logger
	now     func() time.Time
	newID   func() string
}

func NewReferenceResolver(store Store, cfg WagerConfig, metrics *observability.Metrics, log *slog.Logger) *ReferenceResolver {
	return &ReferenceResolver{store: store, cfg: cfg, metrics: metrics, log: log, now: time.Now, newID: NewID}
}

// ResolveDue processes up to limit due transactions and returns how many it attempted.
func (r *ReferenceResolver) ResolveDue(ctx context.Context, limit int) (int, error) {
	due, err := r.store.ListDueReferencePending(ctx, r.now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	for _, p := range due {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		r.metrics.ReferenceRetries.Inc()
		err := r.resolveOne(ctx, p)
		switch {
		case err == nil:
		case errors.Is(err, ErrUnavailable) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			r.log.Warn("pending reference retry failed transiently", "transactionId", p.TransactionID, "walletId", p.WalletID, "error", err.Error())
		default:
			r.log.Error("pending reference failed permanently", "transactionId", p.TransactionID, "walletId", p.WalletID, "error", err.Error())
			if ferr := r.markFailed(ctx, p); ferr != nil {
				r.log.Error("could not mark transaction FAILED", "transactionId", p.TransactionID, "error", ferr.Error())
			}
		}
	}
	return len(due), nil
}

func (r *ReferenceResolver) resolveOne(ctx context.Context, p PendingRef) error {
	return r.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		now := r.now().UTC()
		// Lock order is always wallet -> transaction, the same as the request path.
		w, err := tx.LockWallet(ctx, p.WalletID)
		if err != nil {
			return err
		}
		t, err := tx.LockTransaction(ctx, p.TransactionID)
		if err != nil {
			return err
		}
		if t.Status() != wagering.StatusPendingReference || t.NextReferenceAttemptAt() == nil || t.NextReferenceAttemptAt().After(now) {
			return nil // another instance already handled this attempt
		}
		versionBefore := w.Version()

		ref, err := tx.FindByExternalID(ctx, t.ProviderID(), t.ReferenceExternalTransactionID())
		if err != nil {
			return err
		}
		reversed := false
		if ref != nil && t.Kind().IsReversal() {
			if reversed, err = tx.HasSuccessfulReversal(ctx, ref.ID()); err != nil {
				return err
			}
		}
		settlement, err := wagering.Settle(wagering.SettleInput{
			Transaction: t, Wallet: w, Reference: ref, ReferenceAlreadyReversed: reversed,
			Retry: r.cfg.Retry, Now: now, NewID: r.newID, CausationID: t.ID(),
		})
		if err != nil {
			return err
		}
		if err := tx.UpdatePendingReference(ctx, t.Snapshot()); err != nil {
			return err
		}
		if settlement.LedgerEntry != nil {
			if err := tx.UpdateWalletBalance(ctx, w, versionBefore); err != nil {
				return err
			}
			if err := tx.InsertLedgerEntry(ctx, *settlement.LedgerEntry); err != nil {
				return err
			}
		}
		if err := tx.InsertOutboxEvents(ctx, settlement.Events); err != nil {
			return err
		}
		if t.Status().IsTerminal() {
			r.metrics.WagerResults.WithLabelValues("reference-worker", string(t.Status())).Inc()
			r.log.Info("pending reference resolved", "transactionId", t.ID(), "walletId", t.WalletID(),
				"providerId", t.ProviderID(), "correlationId", t.CorrelationID(),
				"status", t.Status(), "failureCode", t.FailureCode())
		}
		return nil
	})
}

func (r *ReferenceResolver) markFailed(ctx context.Context, p PendingRef) error {
	return r.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		t, err := tx.LockTransaction(ctx, p.TransactionID)
		if err != nil {
			return err
		}
		if t.Status() != wagering.StatusPendingReference {
			return nil
		}
		if err := t.MarkFailed(FailurePermanentInternal, r.now()); err != nil {
			return err
		}
		return tx.MarkTransactionFailed(ctx, t.Snapshot())
	})
}
