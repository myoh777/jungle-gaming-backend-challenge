// Package wagering contains the WagerTransaction aggregate, its state machine,
// the settlement rules for BET/WIN/LOSS/REFUND/ROLLBACK, wallet opening and the
// domain events. It does not depend on HTTP, SQS, PostgreSQL or Fx.
package wagering

import (
	"errors"
	"fmt"
	"time"

	"wagering/internal/money"
)

// Kind of a wager transaction.
type Kind string

const (
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
	// KindOpening is reserved for the internal wallet opening credit and is
	// rejected on every external entry point.
	KindOpening Kind = "OPENING"
)

// ParseExternalKind accepts only the kinds a provider may send.
func ParseExternalKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, nil
	case KindOpening:
		return "", invalid("kind", "OPENING is reserved for internal use")
	default:
		return "", invalid("kind", fmt.Sprintf("unknown kind %q", s))
	}
}

// IsReversal reports whether the kind reverts another transaction.
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// Status of a wager transaction.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// IsTerminal reports whether no further transition is allowed.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Origin distinguishes provider operations from internal ones (OPENING).
type Origin string

const (
	OriginExternal Origin = "EXTERNAL"
	OriginInternal Origin = "INTERNAL"
)

var (
	ErrInvalidTransaction = errors.New("wagering: invalid transaction")
	ErrInvalidTransition  = errors.New("wagering: invalid status transition")
)

// ValidationError describes which input field is invalid. It matches
// ErrInvalidTransaction with errors.Is.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}

func (e *ValidationError) Is(target error) bool { return target == ErrInvalidTransaction }

func invalid(field, reason string) error { return &ValidationError{Field: field, Reason: reason} }

// ExternalRequest carries the business fields of a provider operation.
type ExternalRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
	CorrelationID                  string
}

// Validate applies the structural rules and the zero-amount policy:
// LOSS must be exactly 0.00; every other kind must be > 0.
// REFUND and ROLLBACK require a reference.
func (r ExternalRequest) Validate() error {
	required := map[string]string{
		"providerId":            r.ProviderID,
		"externalTransactionId": r.ExternalTransactionID,
		"idempotencyKey":        r.IdempotencyKey,
		"playerId":              r.PlayerID,
		"walletId":              r.WalletID,
		"roundId":               r.RoundID,
		"gameId":                r.GameID,
	}
	for _, field := range []string{"providerId", "externalTransactionId", "idempotencyKey", "playerId", "walletId", "roundId", "gameId"} {
		if required[field] == "" {
			return invalid(field, "required")
		}
		if len(required[field]) > 200 {
			return invalid(field, "too long (max 200)")
		}
	}
	if _, err := ParseExternalKind(string(r.Kind)); err != nil {
		return err
	}
	if r.Money.Currency() == "" {
		return invalid("money", "required")
	}
	if r.Money.IsNegative() {
		return invalid("money.amount", "must not be negative")
	}
	if r.Kind == KindLoss && !r.Money.IsZero() {
		return invalid("money.amount", "LOSS must have amount 0.00")
	}
	if r.Kind != KindLoss && !r.Money.IsPositive() {
		return invalid("money.amount", fmt.Sprintf("%s must have amount greater than 0.00", r.Kind))
	}
	if r.Kind.IsReversal() && r.ReferenceExternalTransactionID == "" {
		return invalid("referenceExternalTransactionId", fmt.Sprintf("required for %s", r.Kind))
	}
	if r.ReferenceExternalTransactionID != "" {
		if r.Kind == KindBet || r.Kind == KindLoss {
			return invalid("referenceExternalTransactionId", fmt.Sprintf("not allowed for %s", r.Kind))
		}
		if r.ReferenceExternalTransactionID == r.ExternalTransactionID {
			return invalid("referenceExternalTransactionId", "must differ from externalTransactionId")
		}
	}
	return nil
}

// Snapshot is the full persisted state of a transaction, used for
// rehydration and persistence. It carries no behaviour.
type Snapshot struct {
	ID                             string
	Origin                         Origin
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
	ReferenceTransactionID         string
	Status                         Status
	FailureCode                    FailureCode
	ResultBalance                  *money.Money
	ResultWalletVersion            *int64
	ReferenceAttempts              int
	NextReferenceAttemptAt         *time.Time
	CorrelationID                  string
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

// Transaction is the WagerTransaction aggregate.
type Transaction struct {
	s Snapshot
}

// NewExternal creates a provider transaction in PENDING. It is settled in the
// same SQL transaction; PENDING is never committed on its own.
func NewExternal(id string, req ExternalRequest, payloadHash string, now time.Time) (*Transaction, error) {
	if id == "" || payloadHash == "" {
		return nil, invalid("id", "id and payload hash are required")
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	now = now.UTC()
	return &Transaction{s: Snapshot{
		ID:                             id,
		Origin:                         OriginExternal,
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		IdempotencyKey:                 req.IdempotencyKey,
		PayloadHash:                    payloadHash,
		WalletID:                       req.WalletID,
		PlayerID:                       req.PlayerID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           req.Kind,
		Money:                          req.Money,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		Status:                         StatusPending,
		CorrelationID:                  req.CorrelationID,
		CreatedAt:                      now,
		UpdatedAt:                      now,
	}}, nil
}

// newOpening creates the internal OPENING credit. It has no provider,
// external id, idempotency key, round or game.
func newOpening(id, walletID, playerID string, amount money.Money, correlationID string, now time.Time) (*Transaction, error) {
	if id == "" || walletID == "" || playerID == "" {
		return nil, invalid("opening", "id, walletId and playerId are required")
	}
	if !amount.IsPositive() {
		return nil, invalid("initialBalance", "opening amount must be positive")
	}
	now = now.UTC()
	return &Transaction{s: Snapshot{
		ID: id, Origin: OriginInternal, WalletID: walletID, PlayerID: playerID,
		Kind: KindOpening, Money: amount, Status: StatusPending,
		CorrelationID: correlationID, CreatedAt: now, UpdatedAt: now,
	}}, nil
}

// Rehydrate rebuilds a transaction from storage without applying any
// transition, financial effect or event.
func Rehydrate(s Snapshot) (*Transaction, error) {
	if s.ID == "" || s.WalletID == "" || s.Kind == "" || s.Status == "" {
		return nil, fmt.Errorf("%w: incomplete snapshot", ErrInvalidTransaction)
	}
	switch s.Status {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
	default:
		return nil, fmt.Errorf("%w: unknown status %q", ErrInvalidTransaction, s.Status)
	}
	if (s.Origin == OriginInternal) != (s.Kind == KindOpening) {
		return nil, fmt.Errorf("%w: origin %s does not match kind %s", ErrInvalidTransaction, s.Origin, s.Kind)
	}
	if (s.Status == StatusRejected || s.Status == StatusFailed) && s.FailureCode == "" {
		return nil, fmt.Errorf("%w: %s without failure code", ErrInvalidTransaction, s.Status)
	}
	return &Transaction{s: s}, nil
}

// Snapshot returns a copy of the current state.
func (t *Transaction) Snapshot() Snapshot { return t.s }

func (t *Transaction) ID() string                    { return t.s.ID }
func (t *Transaction) Origin() Origin                { return t.s.Origin }
func (t *Transaction) ProviderID() string            { return t.s.ProviderID }
func (t *Transaction) ExternalTransactionID() string { return t.s.ExternalTransactionID }
func (t *Transaction) IdempotencyKey() string        { return t.s.IdempotencyKey }
func (t *Transaction) PayloadHash() string           { return t.s.PayloadHash }
func (t *Transaction) WalletID() string              { return t.s.WalletID }
func (t *Transaction) PlayerID() string              { return t.s.PlayerID }
func (t *Transaction) RoundID() string               { return t.s.RoundID }
func (t *Transaction) GameID() string                { return t.s.GameID }
func (t *Transaction) Kind() Kind                    { return t.s.Kind }
func (t *Transaction) Money() money.Money            { return t.s.Money }
func (t *Transaction) ReferenceExternalTransactionID() string {
	return t.s.ReferenceExternalTransactionID
}
func (t *Transaction) ReferenceTransactionID() string     { return t.s.ReferenceTransactionID }
func (t *Transaction) Status() Status                     { return t.s.Status }
func (t *Transaction) FailureCode() FailureCode           { return t.s.FailureCode }
func (t *Transaction) ResultBalance() *money.Money        { return t.s.ResultBalance }
func (t *Transaction) ResultWalletVersion() *int64        { return t.s.ResultWalletVersion }
func (t *Transaction) ReferenceAttempts() int             { return t.s.ReferenceAttempts }
func (t *Transaction) NextReferenceAttemptAt() *time.Time { return t.s.NextReferenceAttemptAt }
func (t *Transaction) CorrelationID() string              { return t.s.CorrelationID }
func (t *Transaction) CreatedAt() time.Time               { return t.s.CreatedAt }
func (t *Transaction) UpdatedAt() time.Time               { return t.s.UpdatedAt }

// allowedTransitions is the state machine:
//
//	PENDING           -> PROCESSED | REJECTED | PENDING_REFERENCE | FAILED
//	PENDING_REFERENCE -> PENDING_REFERENCE (retry) | PROCESSED | REJECTED | FAILED
//	PROCESSED, REJECTED, FAILED are terminal.
var allowedTransitions = map[Status]map[Status]bool{
	StatusPending:          {StatusProcessed: true, StatusRejected: true, StatusPendingReference: true, StatusFailed: true},
	StatusPendingReference: {StatusPendingReference: true, StatusProcessed: true, StatusRejected: true, StatusFailed: true},
}

func (t *Transaction) transition(to Status, now time.Time) error {
	if !allowedTransitions[t.s.Status][to] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.s.Status, to)
	}
	t.s.Status = to
	t.s.UpdatedAt = now.UTC()
	return nil
}

func (t *Transaction) markProcessed(balance money.Money, walletVersion int64, now time.Time) error {
	if err := t.transition(StatusProcessed, now); err != nil {
		return err
	}
	t.s.ResultBalance = &balance
	t.s.ResultWalletVersion = &walletVersion
	t.s.NextReferenceAttemptAt = nil
	return nil
}

// markRejected records a business rejection. balance may be nil when the
// observed balance must not be disclosed (e.g. the wallet belongs to someone else).
func (t *Transaction) markRejected(code FailureCode, balance *money.Money, walletVersion *int64, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: rejection requires a failure code", ErrInvalidTransaction)
	}
	if err := t.transition(StatusRejected, now); err != nil {
		return err
	}
	t.s.FailureCode = code
	t.s.ResultBalance = balance
	t.s.ResultWalletVersion = walletVersion
	t.s.NextReferenceAttemptAt = nil
	return nil
}

// MarkFailed records a permanent infrastructure failure for audit purposes.
func (t *Transaction) MarkFailed(code FailureCode, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: failure requires a failure code", ErrInvalidTransaction)
	}
	if err := t.transition(StatusFailed, now); err != nil {
		return err
	}
	t.s.FailureCode = code
	t.s.NextReferenceAttemptAt = nil
	return nil
}

func (t *Transaction) waitForReference(attempts int, next time.Time, now time.Time) error {
	if err := t.transition(StatusPendingReference, now); err != nil {
		return err
	}
	next = next.UTC()
	t.s.ReferenceAttempts = attempts
	t.s.NextReferenceAttemptAt = &next
	return nil
}
