package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"wagering/internal/domain/wagering"
	"wagering/internal/domain/wallet"
	"wagering/internal/money"
	"wagering/internal/observability"
)

// Viewer is the authenticated caller, as far as use cases care: either the
// internal wallet service or one provider.
type Viewer struct {
	ProviderID string
	Internal   bool
}

// CanSee reports whether the viewer may read a transaction. Providers only see
// their own; internal callers see everything.
func (v Viewer) CanSee(t wagering.Snapshot) bool {
	return v.Internal || (v.ProviderID != "" && v.ProviderID == t.ProviderID)
}

// WalletService implements wallet creation, queries and reconciliation.
type WalletService struct {
	store   Store
	metrics *observability.Metrics
	log     *slog.Logger
	now     func() time.Time
	newID   func() string
}

func NewWalletService(store Store, metrics *observability.Metrics, log *slog.Logger) *WalletService {
	return &WalletService{store: store, metrics: metrics, log: log, now: time.Now, newID: NewID}
}

// CreateWalletInput is the request to open a wallet.
type CreateWalletInput struct {
	PlayerID      string
	Amount        string
	Currency      string
	CorrelationID string
}

// CreateWalletResult carries the wallet and, if any, the OPENING transaction.
type CreateWalletResult struct {
	Wallet  *wallet.Wallet
	Opening *wagering.Snapshot
}

// Create opens a wallet. Wallet, OPENING, its ledger entry and its events are
// committed in the same SQL transaction.
func (s *WalletService) Create(ctx context.Context, in CreateWalletInput) (CreateWalletResult, error) {
	if in.PlayerID == "" || len(in.PlayerID) > 200 {
		return CreateWalletResult{}, &InvalidInputError{Field: "playerId", Reason: "required (max 200 chars)"}
	}
	initial, err := money.ParseNonNegative(in.Amount, in.Currency)
	if err != nil {
		return CreateWalletResult{}, &InvalidInputError{Field: "initialBalance", Reason: err.Error()}
	}

	var result CreateWalletResult
	err = s.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		res, err := wagering.OpenWallet(wagering.OpenWalletInput{
			WalletID: s.newID(), PlayerID: in.PlayerID, InitialBalance: initial,
			CorrelationID: in.CorrelationID, Now: s.now(), NewID: s.newID,
		})
		if err != nil {
			return toInvalidInput(err)
		}
		if err := tx.InsertWallet(ctx, res.Wallet); err != nil {
			return err
		}
		result = CreateWalletResult{Wallet: res.Wallet}
		if res.Opening == nil {
			return nil
		}
		snap := res.Opening.Snapshot()
		inserted, err := tx.InsertTransaction(ctx, snap)
		if err != nil {
			return err
		}
		if !inserted {
			return fmt.Errorf("opening transaction for wallet %s was not inserted", res.Wallet.ID())
		}
		if err := tx.InsertLedgerEntry(ctx, *res.LedgerEntry); err != nil {
			return err
		}
		if err := tx.InsertOutboxEvents(ctx, res.Events); err != nil {
			return err
		}
		result.Opening = &snap
		return nil
	})
	if err == nil {
		s.log.Info("wallet created", "walletId", result.Wallet.ID(), "correlationId", in.CorrelationID)
	}
	return result, err
}

// GetWallet returns the wallet or ErrWalletNotFound.
func (s *WalletService) GetWallet(ctx context.Context, id string) (*wallet.Wallet, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, ErrWalletNotFound
	}
	return s.store.GetWallet(ctx, id)
}

// LedgerPage is one page of ledger entries in insertion order.
type LedgerPage struct {
	Entries    []wallet.LedgerEntry
	NextCursor string
}

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
)

// ListLedger pages the ledger ordered by an immutable sequence. The cursor is
// opaque to clients (base64url of the last sequence returned).
func (s *WalletService) ListLedger(ctx context.Context, walletID, cursor string, limit int) (LedgerPage, error) {
	if _, err := s.GetWallet(ctx, walletID); err != nil {
		return LedgerPage{}, err
	}
	if limit <= 0 {
		limit = DefaultLedgerLimit
	}
	if limit > MaxLedgerLimit {
		return LedgerPage{}, &InvalidInputError{Field: "limit", Reason: fmt.Sprintf("must be between 1 and %d", MaxLedgerLimit)}
	}
	after, err := decodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	// Fetch one extra row to know whether another page exists.
	items, err := s.store.ListLedger(ctx, walletID, after, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{}
	if len(items) > limit {
		items = items[:limit]
		page.NextCursor = encodeCursor(items[len(items)-1].Seq)
	}
	for _, it := range items {
		page.Entries = append(page.Entries, it.Entry)
	}
	return page, nil
}

func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("seq:" + strconv.FormatInt(seq, 10)))
}

func decodeCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, &InvalidInputError{Field: "cursor", Reason: "malformed"}
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(string(raw), "seq:"), 10, 64)
	if err != nil || !strings.HasPrefix(string(raw), "seq:") || n < 0 {
		return 0, &InvalidInputError{Field: "cursor", Reason: "malformed"}
	}
	return n, nil
}

// GetTransaction returns a transaction the viewer is allowed to see. Other
// providers' transactions are reported as not found, so existence is not leaked.
func (s *WalletService) GetTransaction(ctx context.Context, viewer Viewer, id string) (wagering.Snapshot, error) {
	if _, err := uuid.Parse(id); err != nil {
		return wagering.Snapshot{}, ErrTransactionNotFound
	}
	t, err := s.store.GetTransaction(ctx, id)
	if err != nil {
		return wagering.Snapshot{}, err
	}
	if !viewer.CanSee(t.Snapshot()) {
		return wagering.Snapshot{}, ErrTransactionNotFound
	}
	return t.Snapshot(), nil
}

// GetTransactionByExternalID looks up (providerId, externalTransactionId).
func (s *WalletService) GetTransactionByExternalID(ctx context.Context, viewer Viewer, providerID, externalID string) (wagering.Snapshot, error) {
	t, err := s.store.GetTransactionByExternalID(ctx, providerID, externalID)
	if err != nil {
		return wagering.Snapshot{}, err
	}
	if !viewer.CanSee(t.Snapshot()) {
		return wagering.Snapshot{}, ErrTransactionNotFound
	}
	return t.Snapshot(), nil
}

// ReconciliationResult compares the stored balance with the ledger.
// Difference = stored - calculated.
type ReconciliationResult struct {
	WalletID          string
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

// Reconcile rebuilds the balance from the ledger (opening included) in one
// consistent snapshot. It never changes the stored balance.
func (s *WalletService) Reconcile(ctx context.Context, walletID string) (ReconciliationResult, error) {
	if _, err := uuid.Parse(walletID); err != nil {
		return ReconciliationResult{}, ErrWalletNotFound
	}
	data, err := s.store.ReconciliationSnapshot(ctx, walletID)
	if err != nil {
		return ReconciliationResult{}, err
	}
	calculated, err := money.New(data.LedgerNet, data.StoredBalance.Currency())
	if err != nil {
		return ReconciliationResult{}, err
	}
	diff, err := data.StoredBalance.Sub(calculated)
	if err != nil {
		return ReconciliationResult{}, err
	}
	res := ReconciliationResult{
		WalletID: walletID, StoredBalance: data.StoredBalance, CalculatedBalance: calculated,
		Difference: diff, Consistent: diff.IsZero(), CheckedEntries: data.CheckedEntries,
	}
	s.metrics.ReconciliationRuns.WithLabelValues(strconv.FormatBool(res.Consistent)).Inc()
	if !res.Consistent {
		s.metrics.ReconciliationDiverge.Inc()
		s.log.Error("reconciliation divergence", "walletId", walletID,
			"storedBalance", data.StoredBalance.Amount(), "calculatedBalance", calculated.Amount(),
			"difference", diff.Amount(), "checkedEntries", data.CheckedEntries)
	} else {
		s.log.Info("reconciliation consistent", "walletId", walletID, "checkedEntries", data.CheckedEntries)
	}
	return res, nil
}
