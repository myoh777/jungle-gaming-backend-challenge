package wagering

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// PayloadHash computes the deterministic business hash used for idempotency
// and inbox checks.
//
// Algorithm: SHA-256 (hex, lowercase) over canonical JSON. The JSON object
// has its keys sorted (encoding/json sorts map keys) and contains exactly:
// providerId, externalTransactionId, playerId, walletId, roundId, gameId,
// kind, amount, currency and, only when present, referenceExternalTransactionId.
// amount is normalized to two decimals (Money.Amount), so "25.5" and "25.50"
// hash equally. The idempotency key, correlation id and transport metadata
// (headers, SQS message ids, timestamps) are excluded, so HTTP and SQS produce
// the same hash for the same business operation. Strings are not trimmed or
// case-folded.
func PayloadHash(r ExternalRequest) string {
	fields := map[string]string{
		"providerId":            r.ProviderID,
		"externalTransactionId": r.ExternalTransactionID,
		"playerId":              r.PlayerID,
		"walletId":              r.WalletID,
		"roundId":               r.RoundID,
		"gameId":                r.GameID,
		"kind":                  string(r.Kind),
		"amount":                r.Money.Amount(),
		"currency":              string(r.Money.Currency()),
	}
	if r.ReferenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = r.ReferenceExternalTransactionID
	}
	// Marshalling a map[string]string cannot fail.
	canonical, _ := json.Marshal(fields)
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
