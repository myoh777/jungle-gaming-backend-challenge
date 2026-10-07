package wagering

import (
	"time"

	"wagering/internal/domain/wallet"
	"wagering/internal/money"
)

// OpenWalletInput describes a wallet creation request.
type OpenWalletInput struct {
	WalletID       string
	PlayerID       string
	InitialBalance money.Money
	CorrelationID  string
	Now            time.Time
	NewID          func() string
}

// OpenWalletResult is persisted atomically. With a zero initial balance only
// Wallet is set: no OPENING, no ledger entry and no financial events.
type OpenWalletResult struct {
	Wallet      *wallet.Wallet
	Opening     *Transaction
	LedgerEntry *wallet.LedgerEntry
	Events      []Event
}

// OpenWallet creates a wallet. A positive initial balance produces an internal
// OPENING transaction (PROCESSED), its credit ledger entry and the
// WagerTransactionProcessed + WalletBalanceChanged events.
func OpenWallet(in OpenWalletInput) (OpenWalletResult, error) {
	if in.InitialBalance.IsNegative() {
		return OpenWalletResult{}, invalid("initialBalance", "must not be negative")
	}
	if in.InitialBalance.IsZero() {
		w, err := wallet.New(in.WalletID, in.PlayerID, in.InitialBalance.Currency(), in.Now)
		if err != nil {
			return OpenWalletResult{}, err
		}
		return OpenWalletResult{Wallet: w}, nil
	}

	opening, err := newOpening(in.NewID(), in.WalletID, in.PlayerID, in.InitialBalance, in.CorrelationID, in.Now)
	if err != nil {
		return OpenWalletResult{}, err
	}
	w, entry, err := wallet.NewWithOpeningBalance(in.WalletID, in.PlayerID, in.InitialBalance, opening.ID(), in.NewID(), in.Now)
	if err != nil {
		return OpenWalletResult{}, err
	}
	if err := opening.markProcessed(w.Balance(), w.Version(), in.Now); err != nil {
		return OpenWalletResult{}, err
	}
	events := []Event{
		NewWagerTransactionProcessedEvent(in.NewID(), opening, "", in.Now),
		NewWalletBalanceChangedEvent(in.NewID(), entry, w.Version(), in.CorrelationID, "", in.Now),
	}
	return OpenWalletResult{Wallet: w, Opening: opening, LedgerEntry: &entry, Events: events}, nil
}
