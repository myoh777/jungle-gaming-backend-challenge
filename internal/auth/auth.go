// Package auth validates OAuth 2.0 access tokens (JWT) issued by an external
// OIDC provider (Keycloak). The service never issues tokens or stores passwords.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Roles granted in the IdP (realm roles).
const (
	RoleProvider = "wagering-provider" // game providers; must also carry provider_id
	RoleInternal = "wallet-internal"   // internal wallet service / back office
)

var ErrUnauthenticated = errors.New("unauthenticated")

// Principal is the authenticated identity derived from a verified token.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string // from the provider_id claim; empty for internal clients
	Roles      []string
}

func (p Principal) HasRole(role string) bool {
	for _, r := range p.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// IsInternal reports whether the caller is the internal wallet client.
func (p Principal) IsInternal() bool { return p.HasRole(RoleInternal) }

// IsProvider reports whether the caller is a game provider bound to a providerId.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// Config for token verification.
type Config struct {
	Issuer   string // expected iss
	JWKSURL  string // signing keys location
	Audience string // expected aud
}

// Verifier checks signature (JWKS, RS256), issuer, audience and expiry.
type Verifier struct {
	cfg      Config
	verifier *oidc.IDTokenVerifier
}

// NewVerifier builds a verifier. Keys are fetched lazily and cached; call
// CheckJWKS at startup to fail fast on a misconfigured IdP.
func NewVerifier(cfg Config) *Verifier {
	// The key set fetches keys with this context for the whole process lifetime.
	keySet := oidc.NewRemoteKeySet(context.Background(), cfg.JWKSURL)
	v := oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})
	return &Verifier{cfg: cfg, verifier: v}
}

type claims struct {
	AuthorizedParty string `json:"azp"`
	ClientID        string `json:"client_id"`
	ProviderID      string `json:"provider_id"`
	RealmAccess     struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verify validates a raw bearer token and extracts the principal.
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	tok, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: bad claims: %v", ErrUnauthenticated, err)
	}
	client := c.AuthorizedParty
	if client == "" {
		client = c.ClientID
	}
	return Principal{Subject: tok.Subject, ClientID: client, ProviderID: c.ProviderID, Roles: c.RealmAccess.Roles}, nil
}

// CheckJWKS fetches the key set once to validate IdP connectivity and configuration.
func (v *Verifier) CheckJWKS(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.JWKSURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch jwks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch jwks: status %d", resp.StatusCode)
	}
	return nil
}
