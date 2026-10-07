package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"wagering/internal/domain/wagering"
	"wagering/internal/money"
	"wagering/internal/observability"
)

// InvalidInputError is a request that can never succeed as sent. Nothing is persisted.
type InvalidInputError struct {
	Field  string
	Reason string
}

func (e *InvalidInputError) Error() string { return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason) }

// WagerInput is the transport-neutral form of a provider operation, shared by
// HTTP and SQS. Amount is the external decimal string.
type WagerInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
	CorrelationID                  string
	CausationID                    string
	Source                         string // "http" or "sqs", for metrics and logs
}

// InboxMessage identifies an SQS message so the inbox row is written in the
// same SQL transaction as the financial effects.
type InboxMessage struct {
	ConsumerName string
	MessageID    string
	ReceivedAt   time.Time
}

// WagerResult is the persisted state of the transaction. Replay is true when
// the request was answered from a previously committed result.
type WagerResult struct {
	Transaction wagering.Snapshot
	Replay      bool
}

// Config for the wager use case.
type WagerConfig struct {
	Retry wagering.RetryPolicy
}

// WagerService is the single entry point for provider operations.
type WagerService struct {
	store   Store
	cfg     WagerConfig
	metrics *observability.Metrics
	log     *slog.Logger
	now     func() time.Time
	newID   func() string
}

func NewWagerService(store Store, cfg WagerConfig, metrics *observability.Metrics, log *slog.Logger) *WagerService {
	return &WagerService{store: store, cfg: cfg, metrics: metrics, log: log, now: time.Now, newID: NewID}
}

// NewID returns a time-ordered UUIDv7.
func NewID() string { return uuid.Must(uuid.NewV7()).String() }

// BuildRequest validates and converts transport input into the domain request.
// External amounts must be non-negative decimals with at most two places.
func BuildRequest(in WagerInput) (wagering.ExternalRequest, error) {
	kind, err := wagering.ParseExternalKind(in.Kind)
	if err != nil {
		return wagering.ExternalRequest{}, toInvalidInput(err)
	}
	if in.Amount == "" || in.Currency == "" {
		return wagering.ExternalRequest{}, &InvalidInputError{Field: "money", Reason: "amount and currency are required"}
	}
	m, err := money.ParseNonNegative(in.Amount, in.Currency)
	if err != nil {
		return wagering.ExternalRequest{}, &InvalidInputError{Field: "money", Reason: err.Error()}
	}
	req := wagering.ExternalRequest{
		ProviderID: in.ProviderID, ExternalTransactionID: in.ExternalTransactionID,
		IdempotencyKey: in.IdempotencyKey, PlayerID: in.PlayerID, WalletID: in.WalletID,
		RoundID: in.RoundID, GameID: in.GameID, Kind: kind, Money: m,
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
		CorrelationID:                  in.CorrelationID,
	}
	if err := req.Validate(); err != nil {
		return wagering.ExternalRequest{}, toInvalidInput(err)
	}
	if _, err := uuid.Parse(req.WalletID); err != nil {
		return wagering.ExternalRequest{}, &InvalidInputError{Field: "walletId", Reason: "must be a UUID"}
	}
	return req, nil
}

func toInvalidInput(err error) error {
	var ve *wagering.ValidationError
	if errors.As(err, &ve) {
		return &InvalidInputError{Field: ve.Field, Reason: ve.Reason}
	}
	return &InvalidInputError{Field: "request", Reason: err.Error()}
}

// Process runs a provider operation exactly once per (providerId,
// idempotencyKey). Within one SQL transaction it: records the inbox row (SQS
// only), detects replays/conflicts, locks the wallet, settles the operation
// and writes transaction, balance, ledger and outbox together.
func (s *WagerService) Process(ctx context.Context, in WagerInput, inbox *InboxMessage) (WagerResult, error) {
	start := time.Now()
	defer func() { s.metrics.WagerDuration.WithLabelValues(in.Source).Observe(time.Since(start).Seconds()) }()

	req, err := BuildRequest(in)
	if err != nil {
		return WagerResult{}, err
	}
	hash := wagering.PayloadHash(req)

	var result WagerResult
	err = s.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		result = WagerResult{}
		now := s.now().UTC()

		if inbox != nil {
			inserted, err := tx.InsertInbox(ctx, InboxRecord{
				ConsumerName: inbox.ConsumerName, MessageID: inbox.MessageID, PayloadHash: hash,
				ReceivedAt: inbox.ReceivedAt, ProcessedAt: now,
			})
			if err != nil {
				return err
			}
			if !inserted {
				rec, err := tx.GetInbox(ctx, inbox.ConsumerName, inbox.MessageID)
				if err != nil {
					return err
				}
				if rec.PayloadHash != hash {
					return ErrInboxHashMismatch
				}
				t, err := tx.GetTransaction(ctx, rec.TransactionID)
				if err != nil {
					return err
				}
				result = WagerResult{Transaction: t.Snapshot(), Replay: true}
				return nil
			}
		}

		if existing, err := findExisting(ctx, tx, req, hash); err != nil || existing != nil {
			if existing != nil {
				result = WagerResult{Transaction: existing.Snapshot(), Replay: true}
				return completeInbox(ctx, tx, inbox, existing.ID())
			}
			return err
		}

		w, err := tx.LockWallet(ctx, req.WalletID)
		if err != nil {
			return err
		}
		versionBefore := w.Version()

		t, err := wagering.NewExternal(s.newID(), req, hash, now)
		if err != nil {
			return toInvalidInput(err)
		}

		var ref *wagering.Transaction
		reversed := false
		if req.ReferenceExternalTransactionID != "" {
			ref, err = tx.FindByExternalID(ctx, req.ProviderID, req.ReferenceExternalTransactionID)
			if err != nil {
				return err
			}
			if ref != nil && req.Kind.IsReversal() {
				if reversed, err = tx.HasSuccessfulReversal(ctx, ref.ID()); err != nil {
					return err
				}
			}
		}

		settlement, err := wagering.Settle(wagering.SettleInput{
			Transaction: t, Wallet: w, Reference: ref, ReferenceAlreadyReversed: reversed,
			Retry: s.cfg.Retry, Now: now, NewID: s.newID, CausationID: in.CausationID,
		})
		if err != nil {
			return err
		}

		inserted, err := tx.InsertTransaction(ctx, t.Snapshot())
		if err != nil {
			return err
		}
		if !inserted {
			// A concurrent request with the same key or external id committed
			// while we waited on the unique index. Nothing of ours is written.
			existing, err := findExisting(ctx, tx, req, hash)
			if err != nil {
				return err
			}
			if existing == nil {
				return fmt.Errorf("%w: duplicate insert without visible winner", ErrConcurrentModification)
			}
			result = WagerResult{Transaction: existing.Snapshot(), Replay: true}
			return completeInbox(ctx, tx, inbox, existing.ID())
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
		result = WagerResult{Transaction: t.Snapshot()}
		return completeInbox(ctx, tx, inbox, t.ID())
	})

	s.record(in, result, err)
	return result, err
}

// findExisting implements the idempotency rules:
//   - same key and same hash      -> the persisted transaction (replay)
//   - same key, different hash    -> ErrIdempotencyConflict
//   - same external id, other key -> ErrDuplicateExternalTransaction
func findExisting(ctx context.Context, tx Tx, req wagering.ExternalRequest, hash string) (*wagering.Transaction, error) {
	byKey, err := tx.FindByIdempotencyKey(ctx, req.ProviderID, req.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if byKey != nil {
		return matchKey(byKey, hash)
	}
	byExt, err := tx.FindByExternalID(ctx, req.ProviderID, req.ExternalTransactionID)
	if err != nil {
		return nil, err
	}
	if byExt != nil {
		// Under READ COMMITTED each lookup sees a new snapshot: a concurrent
		// request with the same key may commit between the two queries, so
		// the row found here can still be ours.
		if byExt.IdempotencyKey() == req.IdempotencyKey {
			return matchKey(byExt, hash)
		}
		return nil, ErrDuplicateExternalTransaction
	}
	return nil, nil
}

func matchKey(t *wagering.Transaction, hash string) (*wagering.Transaction, error) {
	if t.PayloadHash() != hash {
		return nil, ErrIdempotencyConflict
	}
	return t, nil
}

func completeInbox(ctx context.Context, tx Tx, inbox *InboxMessage, transactionID string) error {
	if inbox == nil {
		return nil
	}
	return tx.SetInboxTransaction(ctx, inbox.ConsumerName, inbox.MessageID, transactionID)
}

func (s *WagerService) record(in WagerInput, res WagerResult, err error) {
	attrs := []any{
		"source", in.Source, "providerId", in.ProviderID, "walletId", in.WalletID,
		"correlationId", in.CorrelationID, "externalTransactionId", in.ExternalTransactionID,
	}
	switch {
	case err == nil && res.Replay:
		s.metrics.IdempotentReplays.WithLabelValues(in.Source).Inc()
		s.log.Info("wager replayed", append(attrs, "transactionId", res.Transaction.ID, "status", res.Transaction.Status)...)
	case err == nil:
		s.metrics.WagerResults.WithLabelValues(in.Source, string(res.Transaction.Status)).Inc()
		s.log.Info("wager handled", append(attrs, "transactionId", res.Transaction.ID,
			"status", res.Transaction.Status, "failureCode", res.Transaction.FailureCode)...)
	case errors.Is(err, ErrIdempotencyConflict):
		s.metrics.IdempotencyConflicts.WithLabelValues("payload_mismatch").Inc()
		s.log.Warn("idempotency conflict", attrs...)
	case errors.Is(err, ErrDuplicateExternalTransaction):
		s.metrics.IdempotencyConflicts.WithLabelValues("external_id_reused").Inc()
		s.log.Warn("duplicate external transaction", attrs...)
	default:
		s.log.Warn("wager not handled", append(attrs, "error", err.Error())...)
	}
}
