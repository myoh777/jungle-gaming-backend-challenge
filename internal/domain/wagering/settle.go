package wagering

import (
	"errors"
	"fmt"
	"time"

	"wagering/internal/domain/wallet"
	"wagering/internal/money"
)

// RetryPolicy controls how long a transaction waits for its reference.
// The n-th failed attempt schedules the next one after
// min(BaseDelay * 2^(n-1), MaxDelay). The transaction is rejected once it has
// failed MaxAttempts times or once TTL has elapsed since it was created.
type RetryPolicy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
	TTL         time.Duration
}

// Delay returns the wait after the given number of failed attempts (>= 1).
func (p RetryPolicy) Delay(failedAttempts int) time.Duration {
	d := p.BaseDelay
	for i := 1; i < failedAttempts && d < p.MaxDelay; i++ {
		d *= 2
	}
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	return d
}

// SettleInput is everything Settle needs. The caller must hold the wallet
// lock for the whole SQL transaction so Wallet, Reference and
// ReferenceAlreadyReversed are a consistent view.
type SettleInput struct {
	Transaction *Transaction
	Wallet      *wallet.Wallet
	// Reference is the transaction resolved by (providerId,
	// referenceExternalTransactionId), or nil when it does not exist yet.
	Reference *Transaction
	// ReferenceAlreadyReversed reports whether Reference already has a
	// PROCESSED REFUND or ROLLBACK.
	ReferenceAlreadyReversed bool
	Retry                    RetryPolicy
	Now                      time.Time
	NewID                    func() string
	CausationID              string
}

// Settlement is the result to persist atomically: the (already mutated)
// transaction and wallet, an optional ledger entry and the events.
type Settlement struct {
	LedgerEntry *wallet.LedgerEntry
	Events      []Event
}

// Settle decides the outcome of a PENDING or PENDING_REFERENCE transaction
// and mutates the transaction and wallet accordingly. Business rejections are
// recorded on the transaction (REJECTED + failure code), never returned as
// errors. An error means a programming or invariant violation.
func Settle(in SettleInput) (Settlement, error) {
	t, w := in.Transaction, in.Wallet
	if t.Status() != StatusPending && t.Status() != StatusPendingReference {
		return Settlement{}, fmt.Errorf("%w: cannot settle %s", ErrInvalidTransition, t.Status())
	}
	if t.Origin() != OriginExternal {
		return Settlement{}, fmt.Errorf("%w: only external transactions are settled", ErrInvalidTransaction)
	}
	if w.ID() != t.WalletID() {
		return Settlement{}, fmt.Errorf("%w: wallet %s does not match transaction wallet %s", ErrInvalidTransaction, w.ID(), t.WalletID())
	}

	// The wallet belongs to someone else: do not disclose its balance.
	if w.PlayerID() != t.PlayerID() {
		return reject(in, FailureWalletPlayerMismatch, false)
	}
	if w.Currency() != t.Money().Currency() {
		return reject(in, FailureCurrencyMismatch, true)
	}

	var ref *Transaction
	if t.ReferenceExternalTransactionID() != "" {
		r := in.Reference
		if r == nil || r.Status() == StatusPending || r.Status() == StatusPendingReference {
			return waitOrExpire(in)
		}
		if r.Status() != StatusProcessed {
			return reject(in, FailureReferenceNotProcessed, true)
		}
		if r.ProviderID() != t.ProviderID() || r.WalletID() != t.WalletID() || r.PlayerID() != t.PlayerID() ||
			r.Money().Currency() != t.Money().Currency() || r.RoundID() != t.RoundID() {
			return reject(in, FailureReferenceMismatch, true)
		}
		if !referenceKindAllowed(t.Kind(), r.Kind()) {
			return reject(in, FailureReferenceKindNotAllowed, true)
		}
		if t.Kind().IsReversal() {
			if !r.Money().Equal(t.Money()) {
				return reject(in, FailureReversalAmountMismatch, true)
			}
			if in.ReferenceAlreadyReversed {
				return reject(in, FailureReferenceAlreadyReversed, true)
			}
		}
		ref = r
		t.s.ReferenceTransactionID = r.ID()
	}

	dir, moves := movement(t.Kind(), ref)
	var entry *wallet.LedgerEntry
	if moves {
		var e wallet.LedgerEntry
		var err error
		if dir == wallet.Debit {
			e, err = w.Debit(t.Money(), t.ID(), in.NewID(), in.Now)
		} else {
			e, err = w.Credit(t.Money(), t.ID(), in.NewID(), in.Now)
		}
		if errors.Is(err, wallet.ErrInsufficientFunds) {
			code := FailureInsufficientFunds
			if t.Kind() == KindRollback {
				code = FailureReversalInsufficientFunds
			}
			return reject(in, code, true)
		}
		if err != nil {
			return Settlement{}, err
		}
		entry = &e
	}

	if err := t.markProcessed(w.Balance(), w.Version(), in.Now); err != nil {
		return Settlement{}, err
	}
	events := []Event{NewWagerTransactionProcessedEvent(in.NewID(), t, in.CausationID, in.Now)}
	if entry != nil {
		events = append(events, NewWalletBalanceChangedEvent(in.NewID(), *entry, w.Version(), t.CorrelationID(), in.CausationID, in.Now))
	}
	return Settlement{LedgerEntry: entry, Events: events}, nil
}

// referenceKindAllowed encodes which references each kind accepts:
// WIN -> BET; REFUND -> BET; ROLLBACK -> BET, WIN or REFUND.
func referenceKindAllowed(kind, refKind Kind) bool {
	switch kind {
	case KindWin, KindRefund:
		return refKind == KindBet
	case KindRollback:
		return refKind == KindBet || refKind == KindWin || refKind == KindRefund
	default:
		return false
	}
}

// movement returns the ledger direction for a kind. ROLLBACK moves opposite
// to its reference: BET was a debit so its rollback credits; WIN and REFUND
// were credits so their rollback debits.
func movement(kind Kind, ref *Transaction) (wallet.Direction, bool) {
	switch kind {
	case KindBet:
		return wallet.Debit, true
	case KindWin, KindRefund:
		return wallet.Credit, true
	case KindRollback:
		if ref.Kind() == KindBet {
			return wallet.Credit, true
		}
		return wallet.Debit, true
	default: // LOSS
		return "", false
	}
}

func reject(in SettleInput, code FailureCode, discloseBalance bool) (Settlement, error) {
	var balance *money.Money
	var version *int64
	if discloseBalance {
		b, v := in.Wallet.Balance(), in.Wallet.Version()
		balance, version = &b, &v
	}
	if err := in.Transaction.markRejected(code, balance, version, in.Now); err != nil {
		return Settlement{}, err
	}
	return Settlement{Events: []Event{NewWagerTransactionRejectedEvent(in.NewID(), in.Transaction, in.CausationID, in.Now)}}, nil
}

// waitOrExpire keeps the transaction in PENDING_REFERENCE with exponential
// backoff, or rejects it once the retry budget or TTL is exhausted.
func waitOrExpire(in SettleInput) (Settlement, error) {
	t := in.Transaction
	failed := t.ReferenceAttempts() + 1
	expired := failed >= in.Retry.MaxAttempts || !in.Now.Before(t.CreatedAt().Add(in.Retry.TTL))
	if expired {
		code := FailureReferenceNotFound
		if in.Reference != nil {
			code = FailureReferenceNotResolved
		}
		return reject(in, code, true)
	}
	firstWait := t.Status() == StatusPending
	if err := t.waitForReference(failed, in.Now.Add(in.Retry.Delay(failed)), in.Now); err != nil {
		return Settlement{}, err
	}
	if !firstWait {
		return Settlement{}, nil
	}
	return Settlement{Events: []Event{NewWagerTransactionPendingReferenceEvent(in.NewID(), t, in.CausationID, in.Now)}}, nil
}
