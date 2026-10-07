// Package sqsauth authenticates wager operations received over SQS.
//
// Trust model: a trusted internal gateway authenticates the provider and only
// then signs the operation with that provider's key (HMAC-SHA-256). Keys are
// managed by the service and the gateway; they are never given to providers.
// The consumer recomputes the signature with the key of the providerId the
// message declares, so a message is accepted only if it was signed with the
// key of the provider it claims to act for.
//
// The signature is independent of the idempotency payload hash
// (wagering.PayloadHash): it covers the idempotency key as well, and it is
// verified before the use case runs.
package sqsauth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"wagering/internal/domain/wagering"
)

// Version prefixes the canonical form. A different format needs a new version.
const Version = "wagering.sqs.v1"

// MinKeyLength is the minimum key size in bytes.
const MinKeyLength = 32

var (
	// ErrUnauthenticated wraps every verification failure.
	ErrUnauthenticated    = errors.New("unauthenticated message")
	ErrMissingSignature   = errors.New("missing signature")
	ErrMalformedSignature = errors.New("malformed signature")
	ErrUnknownProvider    = errors.New("no signing key for provider")
	ErrInvalidSignature   = errors.New("signature does not match")
)

// Canonical returns the bytes covered by the signature. The request must be
// the validated domain request (app.BuildRequest), so the amount is already
// normalized to two decimals.
//
// Format: the line "wagering.sqs.v1" followed by one line per field, in this
// fixed order, as "<name>:<length in bytes>:<value>\n":
// providerId, externalTransactionId, idempotencyKey, playerId, walletId,
// roundId, gameId, kind, amount, currency and, only when present,
// referenceExternalTransactionId. The length prefix keeps the encoding
// unambiguous for any value. Strings are not trimmed or case-folded.
func Canonical(r wagering.ExternalRequest) []byte {
	var b bytes.Buffer
	b.WriteString(Version)
	b.WriteByte('\n')
	field := func(name, value string) {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte(':')
		b.WriteString(value)
		b.WriteByte('\n')
	}
	field("providerId", r.ProviderID)
	field("externalTransactionId", r.ExternalTransactionID)
	field("idempotencyKey", r.IdempotencyKey)
	field("playerId", r.PlayerID)
	field("walletId", r.WalletID)
	field("roundId", r.RoundID)
	field("gameId", r.GameID)
	field("kind", string(r.Kind))
	field("amount", r.Money.Amount())
	field("currency", string(r.Money.Currency()))
	if r.ReferenceExternalTransactionID != "" {
		field("referenceExternalTransactionId", r.ReferenceExternalTransactionID)
	}
	return b.Bytes()
}

// Sign returns the lowercase hex HMAC-SHA-256 of Canonical(r).
func Sign(key []byte, r wagering.ExternalRequest) string {
	return hex.EncodeToString(mac(key, Canonical(r)))
}

func mac(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

// Keys maps providerId to its signing key. Formatting and logging never print
// the key material.
type Keys map[string][]byte

func (k Keys) String() string               { return fmt.Sprintf("[%d signing keys redacted]", len(k)) }
func (k Keys) GoString() string             { return k.String() }
func (k Keys) LogValue() slog.Value         { return slog.StringValue(k.String()) }
func (k Keys) MarshalJSON() ([]byte, error) { return json.Marshal(k.String()) }

// ParseKeys decodes a JSON object {"providerId": "key", ...}. The key is the
// UTF-8 bytes of the string. Errors never include key material.
func ParseKeys(raw []byte) (Keys, error) {
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, errors.New("signing keys must be a JSON object of providerId to key strings")
	}
	keys := make(Keys, len(m))
	for provider, key := range m {
		keys[provider] = []byte(key)
	}
	return keys, keys.Validate()
}

// Validate checks provider ids and key sizes.
func (k Keys) Validate() error {
	var errs []error
	for provider, key := range k {
		if provider == "" || len(provider) > 200 {
			errs = append(errs, errors.New("signing key provider id must have 1 to 200 characters"))
		}
		if len(key) < MinKeyLength {
			errs = append(errs, fmt.Errorf("signing key for provider %q is shorter than %d bytes", provider, MinKeyLength))
		}
	}
	return errors.Join(errs...)
}

// Verifier checks signatures against the configured keys. A nil or empty
// Verifier rejects every message.
type Verifier struct {
	keys Keys
}

func NewVerifier(keys Keys) (*Verifier, error) {
	if err := keys.Validate(); err != nil {
		return nil, err
	}
	copied := make(Keys, len(keys))
	for p, k := range keys {
		copied[p] = append([]byte(nil), k...)
	}
	return &Verifier{keys: copied}, nil
}

// Verify fails closed: any problem returns an error wrapping ErrUnauthenticated.
// The comparison is constant-time (hmac.Equal).
func (v *Verifier) Verify(r wagering.ExternalRequest, signature string) error {
	if signature == "" {
		return fmt.Errorf("%w: %w", ErrUnauthenticated, ErrMissingSignature)
	}
	got, err := hex.DecodeString(signature)
	if err != nil || len(got) != sha256.Size {
		return fmt.Errorf("%w: %w", ErrUnauthenticated, ErrMalformedSignature)
	}
	var key []byte
	known := false
	if v != nil {
		key, known = v.keys[r.ProviderID]
	}
	if !known {
		// Still compute one MAC, so unknown providers take the same path.
		key = make([]byte, MinKeyLength)
	}
	ok := hmac.Equal(got, mac(key, Canonical(r)))
	switch {
	case !known:
		return fmt.Errorf("%w: %w", ErrUnauthenticated, ErrUnknownProvider)
	case !ok:
		return fmt.Errorf("%w: %w", ErrUnauthenticated, ErrInvalidSignature)
	}
	return nil
}
