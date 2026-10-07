package sqsauth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"wagering/internal/domain/wagering"
	"wagering/internal/money"
)

var (
	alphaKey = []byte("LOCAL-ONLY-FAKE-KEY-provider-alpha-do-not-use")
	betaKey  = []byte("LOCAL-ONLY-FAKE-KEY-provider-beta-do-not-use!")
)

func request(t *testing.T) wagering.ExternalRequest {
	t.Helper()
	m, err := money.ParseNonNegative("25", "BRL") // normalized to "25.00"
	if err != nil {
		t.Fatal(err)
	}
	return wagering.ExternalRequest{
		ProviderID: "alpha", ExternalTransactionID: "bet-1", IdempotencyKey: "bet-1-key",
		PlayerID: "player-1", WalletID: "01a11822-22dc-72cd-be06-541e1384c17e",
		RoundID: "r1", GameID: "g1", Kind: wagering.KindRefund, Money: m,
		ReferenceExternalTransactionID: "bet-0",
	}
}

// The vector is documented in ARCHITECTURE.md; the expected signature was
// computed independently (Python hmac/hashlib) from the literal below.
func TestCanonicalAndSignatureVector(t *testing.T) {
	want := "wagering.sqs.v1\n" +
		"providerId:5:alpha\n" +
		"externalTransactionId:5:bet-1\n" +
		"idempotencyKey:9:bet-1-key\n" +
		"playerId:8:player-1\n" +
		"walletId:36:01a11822-22dc-72cd-be06-541e1384c17e\n" +
		"roundId:2:r1\n" +
		"gameId:2:g1\n" +
		"kind:6:REFUND\n" +
		"amount:5:25.00\n" +
		"currency:3:BRL\n" +
		"referenceExternalTransactionId:5:bet-0\n"
	r := request(t)
	if got := string(Canonical(r)); got != want {
		t.Fatalf("canonical form changed:\n%q\nwant\n%q", got, want)
	}
	if got := Sign(alphaKey, r); got != "f8e325440ac598a0d32b7d2638851a7e690b37fc2c01ae435872adbdea48d57f" {
		t.Fatalf("signature vector changed: %s", got)
	}

	r.ReferenceExternalTransactionID = ""
	if strings.Contains(string(Canonical(r)), "referenceExternalTransactionId") {
		t.Fatal("absent reference must not appear in the canonical form")
	}
}

func TestCanonicalIsUnambiguous(t *testing.T) {
	a, b := request(t), request(t)
	a.RoundID, a.GameID = "r1\ngameId:2:g1", "x"
	b.RoundID, b.GameID = "r1", "g1\ngameId:1:x"
	if bytes.Equal(Canonical(a), Canonical(b)) {
		t.Fatal("different field values produced the same canonical bytes")
	}
}

func TestVerify(t *testing.T) {
	v, err := NewVerifier(Keys{"alpha": alphaKey, "beta": betaKey})
	if err != nil {
		t.Fatal(err)
	}
	r := request(t)
	sig := Sign(alphaKey, r)
	if err := v.Verify(r, sig); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := v.Verify(r, strings.ToUpper(sig)); err != nil {
		t.Fatalf("hex case must not matter: %v", err)
	}

	tamper := map[string]func(*wagering.ExternalRequest){
		"providerId":            func(r *wagering.ExternalRequest) { r.ProviderID = "beta" },
		"externalTransactionId": func(r *wagering.ExternalRequest) { r.ExternalTransactionID = "bet-2" },
		"idempotencyKey":        func(r *wagering.ExternalRequest) { r.IdempotencyKey = "other-key" },
		"playerId":              func(r *wagering.ExternalRequest) { r.PlayerID = "player-2" },
		"walletId":              func(r *wagering.ExternalRequest) { r.WalletID = "01a11822-0000-7000-8000-000000000000" },
		"roundId":               func(r *wagering.ExternalRequest) { r.RoundID = "r2" },
		"gameId":                func(r *wagering.ExternalRequest) { r.GameID = "g2" },
		"kind":                  func(r *wagering.ExternalRequest) { r.Kind = wagering.KindRollback },
		"amount":                func(r *wagering.ExternalRequest) { r.Money, _ = money.ParseNonNegative("2500.00", "BRL") },
		"currency":              func(r *wagering.ExternalRequest) { r.Money, _ = money.ParseNonNegative("25.00", "USD") },
		"reference":             func(r *wagering.ExternalRequest) { r.ReferenceExternalTransactionID = "bet-9" },
		"reference removed":     func(r *wagering.ExternalRequest) { r.ReferenceExternalTransactionID = "" },
	}
	for name, change := range tamper {
		t.Run("tampered "+name, func(t *testing.T) {
			changed := request(t)
			change(&changed)
			if err := v.Verify(changed, sig); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("tampered %s accepted: %v", name, err)
			}
		})
	}

	failures := []struct {
		name   string
		v      *Verifier
		r      wagering.ExternalRequest
		sig    string
		reason error
	}{
		{"missing", v, r, "", ErrMissingSignature},
		{"not hex", v, r, "zz" + sig[2:], ErrMalformedSignature},
		{"wrong length", v, r, sig[:62], ErrMalformedSignature},
		{"signed with another provider's key", v, r, Sign(betaKey, r), ErrInvalidSignature},
		{"provider without key", v, func() wagering.ExternalRequest { x := r; x.ProviderID = "gamma"; return x }(), sig, ErrUnknownProvider},
		{"nil verifier", nil, r, sig, ErrUnknownProvider},
	}
	for _, tc := range failures {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.v.Verify(tc.r, tc.sig)
			if !errors.Is(err, ErrUnauthenticated) || !errors.Is(err, tc.reason) {
				t.Fatalf("got %v, want %v", err, tc.reason)
			}
		})
	}

	empty, err := NewVerifier(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := empty.Verify(r, sig); !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("verifier without keys must reject: %v", err)
	}
}

func TestKeysValidation(t *testing.T) {
	if _, err := ParseKeys([]byte(`{"alpha":"short"}`)); err == nil {
		t.Fatal("short key accepted")
	}
	if _, err := ParseKeys([]byte(`{"":"` + string(alphaKey) + `"}`)); err == nil {
		t.Fatal("empty provider accepted")
	}
	if _, err := ParseKeys([]byte(`not json`)); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	keys, err := ParseKeys([]byte(`{"alpha":"` + string(alphaKey) + `"}`))
	if err != nil || !bytes.Equal(keys["alpha"], alphaKey) {
		t.Fatalf("valid keys: %v", err)
	}
	// A short key's error names the provider but never the key.
	_, err = ParseKeys([]byte(`{"alpha":"secret-but-too-short"}`))
	if err == nil || strings.Contains(err.Error(), "secret-but-too-short") {
		t.Fatalf("error must not leak key material: %v", err)
	}
}

func TestKeysAreNeverPrinted(t *testing.T) {
	keys := Keys{"alpha": alphaKey}
	cfg := struct{ Keys Keys }{keys}
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("cfg", "keys", keys)
	marshalled, _ := json.Marshal(cfg)
	for _, out := range []string{
		fmt.Sprint(keys), fmt.Sprintf("%v %+v %#v %s", cfg, cfg, cfg, keys), logged.String(), string(marshalled),
	} {
		// %v renders []byte as decimal bytes, so look for that form too.
		if strings.Contains(out, string(alphaKey)) || strings.Contains(out, "LOCAL-ONLY") ||
			strings.Contains(out, strings.Trim(fmt.Sprint(alphaKey), "[]")) {
			t.Fatalf("key material printed: %s", out)
		}
	}
}
