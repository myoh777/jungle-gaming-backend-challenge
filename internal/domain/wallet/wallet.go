// Package wallet contains the Wallet aggregate and its append-only ledger entries.
// It has no knowledge of HTTP, SQS, PostgreSQL or Fx.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"wagering/internal/money"
)

var (
	ErrInvalidWallet     = errors.New("wallet: invalid wallet")
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
	ErrNonPositiveAmount = errors.New("wallet: movement amount must be positive")
	ErrCurrencyMismatch  = errors.New("wallet: currency mismatch")
	ErrInvalidLedger     = errors.New("wallet: invalid ledger entry")
)

// InitialVersion is the version of a freshly created wallet, with or without
// an opening balance.
const InitialVersion int64 = 1

// Wallet holds a player's balance in one currency. Balance changes only
// through Debit and Credit, each of which returns the ledger entry that must
// be committed in the same SQL transaction as the new balance.
type Wallet struct {
	id        string
	playerID  string
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// New creates an empty wallet (balance zero, version 1).
func New(id, playerID string, currency money.Currency, now time.Time) (*Wallet, error) {
	if id == "" || playerID == "" {
		return nil, fmt.Errorf("%w: id and playerId are required", ErrInvalidWallet)
	}
	zero, err := money.Zero(currency)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidWallet, err)
	}
	return &Wallet{
		id: id, playerID: playerID, currency: currency, balance: zero,
		version: InitialVersion, createdAt: now.UTC(), updatedAt: now.UTC(),
	}, nil
}

// NewWithOpeningBalance creates a wallet whose opening credit is part of its
// creation. The returned ledger entry (0 -> opening) must be persisted in the
// same SQL transaction. The wallet version stays at 1 because creation and
// opening are one indivisible event.
func NewWithOpeningBalance(id, playerID string, opening money.Money, openingTxID, entryID string, now time.Time) (*Wallet, LedgerEntry, error) {
	if !opening.IsPositive() {
		return nil, LedgerEntry{}, fmt.Errorf("%w: opening balance must be positive", ErrNonPositiveAmount)
	}
	w, err := New(id, playerID, opening.Currency(), now)
	if err != nil {
		return nil, LedgerEntry{}, err
	}
	entry, err := NewLedgerEntry(entryID, w.id, openingTxID, Credit, opening, w.balance, opening, w.updatedAt)
	if err != nil {
		return nil, LedgerEntry{}, err
	}
	w.balance = opening
	return w, entry, nil
}

// Rehydrate rebuilds a wallet from storage. It validates invariants but never
// applies financial effects or bumps the version.
func Rehydrate(id, playerID string, balance money.Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id == "" || playerID == "" {
		return nil, fmt.Errorf("%w: id and playerId are required", ErrInvalidWallet)
	}
	if balance.IsNegative() {
		return nil, fmt.Errorf("%w: negative balance", ErrInvalidWallet)
	}
	if version < InitialVersion {
		return nil, fmt.Errorf("%w: version %d", ErrInvalidWallet, version)
	}
	return &Wallet{
		id: id, playerID: playerID, currency: balance.Currency(), balance: balance,
		version: version, createdAt: createdAt, updatedAt: updatedAt,
	}, nil
}

func (w *Wallet) ID() string               { return w.id }
func (w *Wallet) PlayerID() string         { return w.playerID }
func (w *Wallet) Currency() money.Currency { return w.currency }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

// Debit removes amount from the balance. It fails with ErrInsufficientFunds
// instead of letting the balance go negative; on failure the wallet is unchanged.
func (w *Wallet) Debit(amount money.Money, transactionID, entryID string, now time.Time) (LedgerEntry, error) {
	return w.move(Debit, amount, transactionID, entryID, now)
}

// Credit adds amount to the balance.
func (w *Wallet) Credit(amount money.Money, transactionID, entryID string, now time.Time) (LedgerEntry, error) {
	return w.move(Credit, amount, transactionID, entryID, now)
}

func (w *Wallet) move(dir Direction, amount money.Money, transactionID, entryID string, now time.Time) (LedgerEntry, error) {
	if amount.Currency() != w.currency {
		return LedgerEntry{}, fmt.Errorf("%w: wallet %s, amount %s", ErrCurrencyMismatch, w.currency, amount.Currency())
	}
	if !amount.IsPositive() {
		return LedgerEntry{}, ErrNonPositiveAmount
	}
	var after money.Money
	var err error
	if dir == Debit {
		after, err = w.balance.Sub(amount)
	} else {
		after, err = w.balance.Add(amount)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	if after.IsNegative() {
		return LedgerEntry{}, ErrInsufficientFunds
	}
	entry, err := NewLedgerEntry(entryID, w.id, transactionID, dir, amount, w.balance, after, now)
	if err != nil {
		return LedgerEntry{}, err
	}
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
	return entry, nil
}
