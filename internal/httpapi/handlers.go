package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"wagering/internal/app"
	"wagering/internal/auth"
	"wagering/internal/domain/wagering"
	"wagering/internal/domain/wallet"
	"wagering/internal/money"
)

const maxBodyBytes = 64 << 10

// decodeStrict rejects unknown fields, trailing data and oversized bodies.
// Monetary amounts are declared as strings, so JSON numbers fail to decode.
func decodeStrict(r *http.Request, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return &app.InvalidInputError{Field: "body", Reason: err.Error()}
	}
	if dec.More() {
		return &app.InvalidInputError{Field: "body", Reason: "trailing data"}
	}
	return nil
}

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// ---------- wallets ----------

type createWalletRequest struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}

type walletResponse struct {
	WalletID  string     `json:"walletId"`
	PlayerID  string     `json:"playerId"`
	Balance   money.JSON `json:"balance"`
	Version   int64      `json:"version"`
	CreatedAt string     `json:"createdAt"`
	UpdatedAt string     `json:"updatedAt"`
}

type createWalletResponse struct {
	walletResponse
	OpeningTransactionID string `json:"openingTransactionId,omitempty"`
}

func toWalletResponse(w *wallet.Wallet) walletResponse {
	return walletResponse{
		WalletID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance().ToJSON(), Version: w.Version(),
		CreatedAt: w.CreatedAt().Format(time.RFC3339Nano), UpdatedAt: w.UpdatedAt().Format(time.RFC3339Nano),
	}
}

func (h *Handler) createWallet(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var req createWalletRequest
	if err := decodeStrict(r, &req); err != nil {
		h.writeAppError(w, r, err)
		return
	}
	res, err := h.wallets.Create(r.Context(), app.CreateWalletInput{
		PlayerID: req.PlayerID, Amount: req.InitialBalance.Amount, Currency: req.InitialBalance.Currency,
		CorrelationID: correlationFrom(r),
	})
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	out := createWalletResponse{walletResponse: toWalletResponse(res.Wallet)}
	if res.Opening != nil {
		out.OpeningTransactionID = res.Opening.ID
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) getWallet(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	wl, err := h.wallets.GetWallet(r.Context(), r.PathValue("walletId"))
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(wl))
}

type ledgerEntryResponse struct {
	EntryID       string     `json:"entryId"`
	WalletID      string     `json:"walletId"`
	TransactionID string     `json:"transactionId"`
	Direction     string     `json:"direction"`
	Money         money.JSON `json:"money"`
	BalanceBefore money.JSON `json:"balanceBefore"`
	BalanceAfter  money.JSON `json:"balanceAfter"`
	CreatedAt     string     `json:"createdAt"`
}

type ledgerResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

func (h *Handler) getLedger(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	// r.URL.Query() silently drops badly escaped pairs, which would turn a
	// malformed cursor into "first page"; reject the request instead.
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		h.writeAppError(w, r, &app.InvalidInputError{Field: "query", Reason: "malformed query string"})
		return
	}
	limit := 0
	if v := query.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			h.writeAppError(w, r, &app.InvalidInputError{Field: "limit", Reason: "must be a positive integer"})
			return
		}
		limit = n
	}
	page, err := h.wallets.ListLedger(r.Context(), r.PathValue("walletId"), query.Get("cursor"), limit)
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	out := ledgerResponse{Entries: []ledgerEntryResponse{}, NextCursor: page.NextCursor}
	for _, e := range page.Entries {
		out.Entries = append(out.Entries, ledgerEntryResponse{
			EntryID: e.ID, WalletID: e.WalletID, TransactionID: e.TransactionID, Direction: string(e.Direction),
			Money: e.Amount.ToJSON(), BalanceBefore: e.BalanceBefore.ToJSON(), BalanceAfter: e.BalanceAfter.ToJSON(),
			CreatedAt: e.CreatedAt.Format(time.RFC3339Nano),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type reconciliationResponse struct {
	WalletID          string     `json:"walletId"`
	StoredBalance     money.JSON `json:"storedBalance"`
	CalculatedBalance money.JSON `json:"calculatedBalance"`
	Difference        money.JSON `json:"difference"`
	Consistent        bool       `json:"consistent"`
	CheckedEntries    int64      `json:"checkedEntries"`
}

func (h *Handler) reconcile(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	res, err := h.wallets.Reconcile(r.Context(), r.PathValue("walletId"))
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: res.WalletID, StoredBalance: res.StoredBalance.ToJSON(),
		CalculatedBalance: res.CalculatedBalance.ToJSON(), Difference: res.Difference.ToJSON(),
		Consistent: res.Consistent, CheckedEntries: res.CheckedEntries,
	})
}

// ---------- wagering ----------

type wagerRequest struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId,omitempty"`
}

// TransactionResponse is the schema returned by every transaction endpoint.
type TransactionResponse struct {
	TransactionID                  string      `json:"transactionId"`
	Origin                         string      `json:"origin"`
	ProviderID                     string      `json:"providerId,omitempty"`
	ExternalTransactionID          string      `json:"externalTransactionId,omitempty"`
	WalletID                       string      `json:"walletId"`
	PlayerID                       string      `json:"playerId"`
	RoundID                        string      `json:"roundId,omitempty"`
	GameID                         string      `json:"gameId,omitempty"`
	Kind                           string      `json:"kind"`
	Money                          money.JSON  `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string      `json:"referenceTransactionId,omitempty"`
	Status                         string      `json:"status"`
	FailureCode                    string      `json:"failureCode,omitempty"`
	BalanceAfter                   *money.JSON `json:"balanceAfter"`
	WalletVersion                  *int64      `json:"walletVersion"`
	ReferenceAttempts              int         `json:"referenceAttempts,omitempty"`
	NextReferenceAttemptAt         string      `json:"nextReferenceAttemptAt,omitempty"`
	IdempotentReplay               *bool       `json:"idempotentReplay,omitempty"`
	CreatedAt                      string      `json:"createdAt"`
	UpdatedAt                      string      `json:"updatedAt"`
}

func toTransactionResponse(s wagering.Snapshot) TransactionResponse {
	out := TransactionResponse{
		TransactionID: s.ID, Origin: string(s.Origin), ProviderID: s.ProviderID,
		ExternalTransactionID: s.ExternalTransactionID, WalletID: s.WalletID, PlayerID: s.PlayerID,
		RoundID: s.RoundID, GameID: s.GameID, Kind: string(s.Kind), Money: s.Money.ToJSON(),
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID,
		ReferenceTransactionID:         s.ReferenceTransactionID,
		Status:                         string(s.Status), FailureCode: string(s.FailureCode),
		WalletVersion: s.ResultWalletVersion, ReferenceAttempts: s.ReferenceAttempts,
		CreatedAt: s.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: s.UpdatedAt.Format(time.RFC3339Nano),
	}
	if s.ResultBalance != nil {
		b := s.ResultBalance.ToJSON()
		out.BalanceAfter = &b
	}
	if s.NextReferenceAttemptAt != nil {
		out.NextReferenceAttemptAt = s.NextReferenceAttemptAt.Format(time.RFC3339)
	}
	return out
}

// statusCode distinguishes outcomes: 200 processed, 202 pending reference,
// 422 rejected by a business rule, 500 for FAILED (never produced synchronously).
func statusCode(s wagering.Status) int {
	switch s {
	case wagering.StatusProcessed:
		return http.StatusOK
	case wagering.StatusPendingReference, wagering.StatusPending:
		return http.StatusAccepted
	case wagering.StatusRejected:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func (h *Handler) postTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 200 {
		h.writeAppError(w, r, &app.InvalidInputError{Field: "Idempotency-Key", Reason: "header is required (max 200 chars)"})
		return
	}
	var req wagerRequest
	if err := decodeStrict(r, &req); err != nil {
		h.writeAppError(w, r, err)
		return
	}
	// The token decides which provider the caller is; it may only act as itself.
	if req.ProviderID != p.ProviderID {
		writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "providerId does not match the authenticated provider")
		return
	}
	res, err := h.wagers.Process(r.Context(), app.WagerInput{
		ProviderID: req.ProviderID, ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: key,
		PlayerID: req.PlayerID, WalletID: req.WalletID, RoundID: req.RoundID, GameID: req.GameID,
		Kind: req.Kind, Amount: req.Money.Amount, Currency: req.Money.Currency,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		CorrelationID:                  correlationFrom(r), CausationID: key, Source: "http",
	}, nil)
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	out := toTransactionResponse(res.Transaction)
	replay := res.Replay
	out.IdempotentReplay = &replay
	writeJSON(w, statusCode(res.Transaction.Status), out)
}

func viewerOf(p auth.Principal) app.Viewer {
	if p.IsInternal() {
		return app.Viewer{Internal: true}
	}
	return app.Viewer{ProviderID: p.ProviderID}
}

func (h *Handler) getTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	t, err := h.wallets.GetTransaction(r.Context(), viewerOf(p), r.PathValue("transactionId"))
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}

func (h *Handler) getTransactionByExternalID(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	providerID := r.PathValue("providerId")
	if !p.IsInternal() && providerID != p.ProviderID {
		writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "cannot read another provider's transactions")
		return
	}
	t, err := h.wallets.GetTransactionByExternalID(r.Context(), viewerOf(p), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}
