package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// These unit tests sign tokens with a local key served as a JWKS. The
// end-to-end tests against the real Keycloak live in test/integration.

type keyPair struct {
	key *rsa.PrivateKey
	kid string
}

func newKey(t *testing.T, kid string) keyPair {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return keyPair{key: k, kid: kid}
}

func jwksServer(t *testing.T, keys ...keyPair) *httptest.Server {
	t.Helper()
	set := jose.JSONWebKeySet{}
	for _, k := range keys {
		set.Keys = append(set.Keys, jose.JSONWebKey{Key: &k.key.PublicKey, KeyID: k.kid, Algorithm: "RS256", Use: "sig"})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func sign(t *testing.T, k keyPair, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: k.key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", k.kid))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

const issuer = "http://idp.test/realms/wagering"

func baseClaims() map[string]any {
	return map[string]any{
		"iss": issuer, "aud": []string{"wagering-api"}, "sub": "svc-alpha", "azp": "provider-alpha",
		"exp": time.Now().Add(5 * time.Minute).Unix(), "iat": time.Now().Unix(),
		"provider_id":  "alpha",
		"realm_access": map[string]any{"roles": []string{RoleProvider}},
	}
}

func TestVerify(t *testing.T) {
	good := newKey(t, "k1")
	srv := jwksServer(t, good)
	v := NewVerifier(Config{Issuer: issuer, JWKSURL: srv.URL, Audience: "wagering-api"})
	ctx := context.Background()

	p, err := v.Verify(ctx, sign(t, good, baseClaims()))
	if err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	if !p.IsProvider() || p.IsInternal() || p.ProviderID != "alpha" || p.ClientID != "provider-alpha" {
		t.Fatalf("unexpected principal %+v", p)
	}

	rogue := newKey(t, "k1") // same kid, different key: signature must fail
	cases := map[string]string{
		"garbage":        "not-a-jwt",
		"wrong key":      sign(t, rogue, baseClaims()),
		"expired":        sign(t, good, with(baseClaims(), "exp", time.Now().Add(-time.Minute).Unix())),
		"wrong issuer":   sign(t, good, with(baseClaims(), "iss", "http://evil/realms/wagering")),
		"wrong audience": sign(t, good, with(baseClaims(), "aud", []string{"account"})),
	}
	for name, tok := range cases {
		if _, err := v.Verify(ctx, tok); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("%s: expected ErrUnauthenticated, got %v", name, err)
		}
	}
}

func with(c map[string]any, k string, v any) map[string]any {
	c[k] = v
	return c
}

func TestPrincipalRoles(t *testing.T) {
	if (Principal{Roles: []string{RoleProvider}}).IsProvider() {
		t.Fatal("provider role without provider_id must not count as provider")
	}
	if !(Principal{Roles: []string{RoleInternal}}).IsInternal() {
		t.Fatal("internal role")
	}
}

func TestCheckJWKS(t *testing.T) {
	srv := jwksServer(t, newKey(t, "k"))
	if err := NewVerifier(Config{Issuer: issuer, JWKSURL: srv.URL, Audience: "a"}).CheckJWKS(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := NewVerifier(Config{Issuer: issuer, JWKSURL: "http://127.0.0.1:1/jwks", Audience: "a"}).CheckJWKS(context.Background()); err == nil {
		t.Fatal("unreachable IdP must fail")
	}
}
