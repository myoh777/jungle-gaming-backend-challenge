package app

import (
	"context"
	"errors"
	"testing"

	"wagering/internal/domain/wagering"
)

// raceTx reproduces READ COMMITTED: each statement sees a fresh snapshot, so a
// concurrent winner can commit between the lookup by key and the lookup by
// external id. Only the two lookups are implemented.
type raceTx struct {
	Tx
	committed *wagering.Transaction // visible from the second statement on
}

func (r *raceTx) FindByIdempotencyKey(context.Context, string, string) (*wagering.Transaction, error) {
	return nil, nil // the winner has not committed yet
}

func (r *raceTx) FindByExternalID(context.Context, string, string) (*wagering.Transaction, error) {
	return r.committed, nil
}

func committedBet(t *testing.T, key, hash string) *wagering.Transaction {
	t.Helper()
	tr, err := wagering.Rehydrate(wagering.Snapshot{
		ID: "tx-1", Origin: wagering.OriginExternal, ProviderID: "alpha", ExternalTransactionID: "bet-1",
		IdempotencyKey: key, PayloadHash: hash, WalletID: "w-1", Kind: wagering.KindBet, Status: wagering.StatusProcessed,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestFindExistingSameKeyCommittedBetweenLookups(t *testing.T) {
	req := wagering.ExternalRequest{ProviderID: "alpha", ExternalTransactionID: "bet-1", IdempotencyKey: "key-1"}
	ctx := context.Background()

	got, err := findExisting(ctx, &raceTx{committed: committedBet(t, "key-1", "h")}, req, "h")
	if err != nil || got == nil || got.ID() != "tx-1" {
		t.Fatalf("same key and payload must replay, got %v, %v", got, err)
	}
	if _, err := findExisting(ctx, &raceTx{committed: committedBet(t, "key-1", "other")}, req, "h"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same key, other payload: want ErrIdempotencyConflict, got %v", err)
	}
	if _, err := findExisting(ctx, &raceTx{committed: committedBet(t, "key-2", "h")}, req, "h"); !errors.Is(err, ErrDuplicateExternalTransaction) {
		t.Fatalf("other key: want ErrDuplicateExternalTransaction, got %v", err)
	}
}
