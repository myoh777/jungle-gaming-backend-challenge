package wallet

import (
	"fmt"
	"time"

	"wagering/internal/money"
)

// Direction of a ledger movement.
type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// LedgerEntry is an immutable, append-only record of one balance change.
type LedgerEntry struct {
	ID            string
	WalletID      string
	TransactionID string
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

// NewLedgerEntry validates that balanceAfter is exactly balanceBefore moved by
// amount in the given direction, that the amount is positive, that all values
// share a currency and that no balance is negative.
func NewLedgerEntry(id, walletID, transactionID string, dir Direction, amount, before, after money.Money, at time.Time) (LedgerEntry, error) {
	if id == "" || walletID == "" || transactionID == "" {
		return LedgerEntry{}, fmt.Errorf("%w: ids are required", ErrInvalidLedger)
	}
	if !amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: amount must be positive", ErrInvalidLedger)
	}
	if amount.Currency() != before.Currency() || amount.Currency() != after.Currency() {
		return LedgerEntry{}, fmt.Errorf("%w: currency mismatch", ErrInvalidLedger)
	}
	if before.IsNegative() || after.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: negative balance", ErrInvalidLedger)
	}
	var expected money.Money
	var err error
	switch dir {
	case Credit:
		expected, err = before.Add(amount)
	case Debit:
		expected, err = before.Sub(amount)
	default:
		return LedgerEntry{}, fmt.Errorf("%w: direction %q", ErrInvalidLedger, dir)
	}
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedger, err)
	}
	if !expected.Equal(after) {
		return LedgerEntry{}, fmt.Errorf("%w: balanceAfter %s does not match %s %s from %s",
			ErrInvalidLedger, after.Amount(), dir, amount.Amount(), before.Amount())
	}
	return LedgerEntry{
		ID: id, WalletID: walletID, TransactionID: transactionID, Direction: dir,
		Amount: amount, BalanceBefore: before, BalanceAfter: after, CreatedAt: at.UTC(),
	}, nil
}
