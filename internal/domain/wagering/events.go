package wagering

import (
	"time"

	"wagering/internal/domain/wallet"
	"wagering/internal/money"
)

// Event is the envelope written to the outbox and published. Type and Version
// come from the concrete payload, never from the caller. Once built, an event
// is serialized into the outbox as an immutable snapshot.
type Event struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    string    `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          EventData `json:"data"`
}

// EventData is implemented only by the concrete payload types in this package.
type EventData interface {
	eventType() string
	eventVersion() int
}

const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

func newEvent(eventID, aggregateID, correlationID, causationID string, at time.Time, data EventData) Event {
	return Event{
		EventID:       eventID,
		EventType:     data.eventType(),
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    at.UTC().Format(time.RFC3339),
		Version:       data.eventVersion(),
		Data:          data,
	}
}

// WagerTransactionProcessed is emitted when a transaction reaches PROCESSED.
// External metadata is omitted for internal OPENING transactions.
type WagerTransactionProcessed struct {
	TransactionID          string     `json:"transactionId"`
	Origin                 Origin     `json:"origin"`
	ProviderID             string     `json:"providerId,omitempty"`
	ExternalTransactionID  string     `json:"externalTransactionId,omitempty"`
	WalletID               string     `json:"walletId"`
	PlayerID               string     `json:"playerId"`
	RoundID                string     `json:"roundId,omitempty"`
	GameID                 string     `json:"gameId,omitempty"`
	Kind                   Kind       `json:"kind"`
	Money                  money.JSON `json:"money"`
	ReferenceTransactionID string     `json:"referenceTransactionId,omitempty"`
	BalanceAfter           money.JSON `json:"balanceAfter"`
	WalletVersion          int64      `json:"walletVersion"`
}

func (WagerTransactionProcessed) eventType() string { return EventWagerTransactionProcessed }
func (WagerTransactionProcessed) eventVersion() int { return 1 }

// WagerTransactionRejected is emitted when a transaction reaches REJECTED.
type WagerTransactionRejected struct {
	TransactionID                  string      `json:"transactionId"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	WalletID                       string      `json:"walletId"`
	PlayerID                       string      `json:"playerId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           Kind        `json:"kind"`
	Money                          money.JSON  `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	FailureCode                    FailureCode `json:"failureCode"`
}

func (WagerTransactionRejected) eventType() string { return EventWagerTransactionRejected }
func (WagerTransactionRejected) eventVersion() int { return 1 }

// WalletBalanceChanged is emitted for every ledger entry.
type WalletBalanceChanged struct {
	WalletID      string           `json:"walletId"`
	TransactionID string           `json:"transactionId"`
	Direction     wallet.Direction `json:"direction"`
	Money         money.JSON       `json:"money"`
	BalanceBefore money.JSON       `json:"balanceBefore"`
	BalanceAfter  money.JSON       `json:"balanceAfter"`
	WalletVersion int64            `json:"walletVersion"`
}

func (WalletBalanceChanged) eventType() string { return EventWalletBalanceChanged }
func (WalletBalanceChanged) eventVersion() int { return 1 }

// WagerTransactionPendingReference is emitted when a transaction first waits
// for its reference. Subsequent retries do not emit it again.
type WagerTransactionPendingReference struct {
	TransactionID                  string     `json:"transactionId"`
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	WalletID                       string     `json:"walletId"`
	PlayerID                       string     `json:"playerId"`
	Kind                           Kind       `json:"kind"`
	Money                          money.JSON `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId"`
	NextAttemptAt                  string     `json:"nextAttemptAt"`
}

func (WagerTransactionPendingReference) eventType() string {
	return EventWagerTransactionPendingReference
}
func (WagerTransactionPendingReference) eventVersion() int { return 1 }

// NewWagerTransactionProcessedEvent builds the event from a PROCESSED transaction.
func NewWagerTransactionProcessedEvent(eventID string, t *Transaction, causationID string, at time.Time) Event {
	s := t.Snapshot()
	data := WagerTransactionProcessed{
		TransactionID: s.ID, Origin: s.Origin, ProviderID: s.ProviderID,
		ExternalTransactionID: s.ExternalTransactionID, WalletID: s.WalletID, PlayerID: s.PlayerID,
		RoundID: s.RoundID, GameID: s.GameID, Kind: s.Kind, Money: s.Money.ToJSON(),
		ReferenceTransactionID: s.ReferenceTransactionID,
	}
	if s.ResultBalance != nil {
		data.BalanceAfter = s.ResultBalance.ToJSON()
	}
	if s.ResultWalletVersion != nil {
		data.WalletVersion = *s.ResultWalletVersion
	}
	return newEvent(eventID, s.ID, s.CorrelationID, causationID, at, data)
}

// NewWagerTransactionRejectedEvent builds the event from a REJECTED transaction.
func NewWagerTransactionRejectedEvent(eventID string, t *Transaction, causationID string, at time.Time) Event {
	s := t.Snapshot()
	return newEvent(eventID, s.ID, s.CorrelationID, causationID, at, WagerTransactionRejected{
		TransactionID: s.ID, ProviderID: s.ProviderID, ExternalTransactionID: s.ExternalTransactionID,
		WalletID: s.WalletID, PlayerID: s.PlayerID, RoundID: s.RoundID, GameID: s.GameID,
		Kind: s.Kind, Money: s.Money.ToJSON(),
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID, FailureCode: s.FailureCode,
	})
}

// NewWalletBalanceChangedEvent builds the event from a ledger entry.
func NewWalletBalanceChangedEvent(eventID string, e wallet.LedgerEntry, walletVersion int64, correlationID, causationID string, at time.Time) Event {
	return newEvent(eventID, e.WalletID, correlationID, causationID, at, WalletBalanceChanged{
		WalletID: e.WalletID, TransactionID: e.TransactionID, Direction: e.Direction,
		Money: e.Amount.ToJSON(), BalanceBefore: e.BalanceBefore.ToJSON(),
		BalanceAfter: e.BalanceAfter.ToJSON(), WalletVersion: walletVersion,
	})
}

// NewWagerTransactionPendingReferenceEvent builds the event from a
// PENDING_REFERENCE transaction.
func NewWagerTransactionPendingReferenceEvent(eventID string, t *Transaction, causationID string, at time.Time) Event {
	s := t.Snapshot()
	next := ""
	if s.NextReferenceAttemptAt != nil {
		next = s.NextReferenceAttemptAt.UTC().Format(time.RFC3339)
	}
	return newEvent(eventID, s.ID, s.CorrelationID, causationID, at, WagerTransactionPendingReference{
		TransactionID: s.ID, ProviderID: s.ProviderID, ExternalTransactionID: s.ExternalTransactionID,
		WalletID: s.WalletID, PlayerID: s.PlayerID, Kind: s.Kind, Money: s.Money.ToJSON(),
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID, NextAttemptAt: next,
	})
}
