// Package app holds the use cases. It orchestrates the domain inside SQL
// transactions through the Store/Tx ports, which the postgres package implements.
// Both HTTP and SQS call the same WagerService.
package app

import (
	"context"
	"errors"
	"time"

	"wagering/internal/domain/wagering"
	"wagering/internal/domain/wallet"
	"wagering/internal/money"
)

// Errors returned by use cases. Adapters map them to HTTP codes / SQS actions.
var (
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrWalletAlreadyExists = errors.New("wallet already exists for player and currency")
	ErrTransactionNotFound = errors.New("transaction not found")
	// ErrIdempotencyConflict: same (providerId, Idempotency-Key) with a different payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrDuplicateExternalTransaction: same (providerId, externalTransactionId) under another key.
	ErrDuplicateExternalTransaction = errors.New("external transaction already exists under another idempotency key")
	// ErrInboxHashMismatch: an SQS messageId was redelivered with a different payload.
	ErrInboxHashMismatch = errors.New("inbox message redelivered with a different payload")
	// ErrConcurrentModification: optimistic version check failed; retried by the store.
	ErrConcurrentModification = errors.New("concurrent modification")
	// ErrUnavailable marks transient infrastructure failures (retry later).
	ErrUnavailable = errors.New("temporarily unavailable")
)

// Store is the persistence port. InTx runs fn in one SQL transaction
// (READ COMMITTED) and may re-run it on retryable conflicts, so fn must not
// keep state across attempts.
type Store interface {
	InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error

	GetWallet(ctx context.Context, id string) (*wallet.Wallet, error)
	ListLedger(ctx context.Context, walletID string, afterSeq int64, limit int) ([]LedgerItem, error)
	GetTransaction(ctx context.Context, id string) (*wagering.Transaction, error)
	GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error)
	ListDueReferencePending(ctx context.Context, now time.Time, limit int) ([]PendingRef, error)
	// ReconciliationSnapshot reads the stored balance and the ledger sum in one
	// consistent (REPEATABLE READ) snapshot.
	ReconciliationSnapshot(ctx context.Context, walletID string) (ReconciliationData, error)
}

// Tx is the set of operations available inside one SQL transaction.
type Tx interface {
	InsertWallet(ctx context.Context, w *wallet.Wallet) error
	// LockWallet loads the wallet with SELECT ... FOR NO KEY UPDATE.
	LockWallet(ctx context.Context, id string) (*wallet.Wallet, error)
	// UpdateWalletBalance writes balance and version, guarded by expectedVersion.
	UpdateWalletBalance(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error

	// InsertTransaction inserts with ON CONFLICT DO NOTHING and reports whether
	// the row was inserted (false = a concurrent duplicate committed first).
	InsertTransaction(ctx context.Context, t wagering.Snapshot) (bool, error)
	// UpdatePendingReference persists the new state of a PENDING_REFERENCE row.
	UpdatePendingReference(ctx context.Context, t wagering.Snapshot) error
	MarkTransactionFailed(ctx context.Context, t wagering.Snapshot) error
	GetTransaction(ctx context.Context, id string) (*wagering.Transaction, error)
	LockTransaction(ctx context.Context, id string) (*wagering.Transaction, error)
	FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wagering.Transaction, error)
	FindByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error)
	HasSuccessfulReversal(ctx context.Context, referenceTransactionID string) (bool, error)

	InsertLedgerEntry(ctx context.Context, e wallet.LedgerEntry) error
	InsertOutboxEvents(ctx context.Context, events []wagering.Event) error

	// InsertInbox inserts with ON CONFLICT DO NOTHING; false means the message
	// was already handled by a committed transaction.
	InsertInbox(ctx context.Context, r InboxRecord) (bool, error)
	GetInbox(ctx context.Context, consumer, messageID string) (InboxRecord, error)
	SetInboxTransaction(ctx context.Context, consumer, messageID, transactionID string) error
}

// LedgerItem is a ledger entry plus its stable ordering key.
type LedgerItem struct {
	Seq   int64
	Entry wallet.LedgerEntry
}

// PendingRef identifies a PENDING_REFERENCE transaction due for retry.
type PendingRef struct {
	TransactionID string
	WalletID      string
}

// InboxRecord is one row of the SQS inbox.
type InboxRecord struct {
	ConsumerName  string
	MessageID     string
	PayloadHash   string
	TransactionID string
	ReceivedAt    time.Time
	ProcessedAt   time.Time
}

// ReconciliationData is read in one consistent snapshot.
type ReconciliationData struct {
	StoredBalance money.Money
	// LedgerNet is SUM(credits) - SUM(debits) in minor units, opening included.
	LedgerNet      int64
	CheckedEntries int64
}
