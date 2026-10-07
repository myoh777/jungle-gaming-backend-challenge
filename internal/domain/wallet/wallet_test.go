package wallet

import (
	"errors"
	"testing"
	"time"

	"wagering/internal/money"
)

var now = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func brl(minor int64) money.Money { return money.MustNew(minor, money.BRL) }

func TestNewWallet(t *testing.T) {
	w, err := New("w1", "p1", money.BRL, now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 1 || !w.Balance().IsZero() || w.Currency() != money.BRL {
		t.Fatalf("unexpected wallet %+v", w)
	}
	if _, err := New("", "p1", money.BRL, now); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("expected invalid wallet, got %v", err)
	}
}

func TestNewWithOpeningBalance(t *testing.T) {
	w, entry, err := NewWithOpeningBalance("w1", "p1", brl(10000), "tx-open", "e1", now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 1 || w.Balance().MinorUnits() != 10000 {
		t.Fatalf("unexpected wallet version=%d balance=%s", w.Version(), w.Balance())
	}
	if entry.Direction != Credit || !entry.BalanceBefore.IsZero() || entry.BalanceAfter.MinorUnits() != 10000 {
		t.Fatalf("unexpected entry %+v", entry)
	}
	if _, _, err := NewWithOpeningBalance("w1", "p1", brl(0), "tx", "e", now); err == nil {
		t.Fatal("zero opening must be rejected")
	}
}

func TestDebitCredit(t *testing.T) {
	w, _, _ := NewWithOpeningBalance("w1", "p1", brl(10000), "tx-open", "e1", now)

	e, err := w.Debit(brl(8000), "tx1", "e2", now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().MinorUnits() != 2000 || w.Version() != 2 {
		t.Fatalf("after debit balance=%s version=%d", w.Balance(), w.Version())
	}
	if e.BalanceBefore.MinorUnits() != 10000 || e.BalanceAfter.MinorUnits() != 2000 || e.Direction != Debit {
		t.Fatalf("bad entry %+v", e)
	}

	if _, err := w.Credit(brl(500), "tx2", "e3", now); err != nil {
		t.Fatal(err)
	}
	if w.Balance().MinorUnits() != 2500 || w.Version() != 3 {
		t.Fatalf("after credit balance=%s version=%d", w.Balance(), w.Version())
	}
}

func TestDebitNeverGoesNegative(t *testing.T) {
	w, _, _ := NewWithOpeningBalance("w1", "p1", brl(10000), "tx-open", "e1", now)
	if _, err := w.Debit(brl(10001), "tx1", "e2", now); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("expected insufficient funds, got %v", err)
	}
	if w.Balance().MinorUnits() != 10000 || w.Version() != 1 {
		t.Fatal("failed debit must not change the wallet")
	}
	// Debiting the full balance is allowed.
	if _, err := w.Debit(brl(10000), "tx2", "e3", now); err != nil {
		t.Fatal(err)
	}
	if !w.Balance().IsZero() {
		t.Fatal("expected zero balance")
	}
}

func TestMovementValidation(t *testing.T) {
	w, _ := New("w1", "p1", money.BRL, now)
	if _, err := w.Credit(brl(0), "tx", "e", now); !errors.Is(err, ErrNonPositiveAmount) {
		t.Fatalf("zero credit: %v", err)
	}
	if _, err := w.Credit(money.MustNew(100, money.USD), "tx", "e", now); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency mismatch: %v", err)
	}
	if w.Version() != 1 {
		t.Fatal("rejected movements must not bump the version")
	}
}

func TestRehydrateDoesNotApplyEffects(t *testing.T) {
	w, err := Rehydrate("w1", "p1", brl(2000), 7, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 7 || w.Balance().MinorUnits() != 2000 {
		t.Fatal("rehydrate must preserve stored state")
	}
	if _, err := Rehydrate("w1", "p1", brl(-1), 1, now, now); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("negative balance must be rejected: %v", err)
	}
	if _, err := Rehydrate("w1", "p1", brl(0), 0, now, now); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("version 0 must be rejected: %v", err)
	}
}

func TestLedgerEntryValidation(t *testing.T) {
	if _, err := NewLedgerEntry("e", "w", "t", Credit, brl(100), brl(0), brl(100), now); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLedgerEntry("e", "w", "t", Credit, brl(100), brl(0), brl(99), now); !errors.Is(err, ErrInvalidLedger) {
		t.Fatalf("wrong balanceAfter: %v", err)
	}
	if _, err := NewLedgerEntry("e", "w", "t", Debit, brl(100), brl(0), brl(-100), now); !errors.Is(err, ErrInvalidLedger) {
		t.Fatalf("negative after: %v", err)
	}
	if _, err := NewLedgerEntry("e", "w", "t", "SIDEWAYS", brl(1), brl(0), brl(1), now); !errors.Is(err, ErrInvalidLedger) {
		t.Fatalf("bad direction: %v", err)
	}
	if _, err := NewLedgerEntry("e", "w", "t", Credit, money.MustNew(1, money.USD), brl(0), brl(1), now); !errors.Is(err, ErrInvalidLedger) {
		t.Fatalf("currency: %v", err)
	}
}
