package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fakeKeys = `{"alpha":"LOCAL-ONLY-FAKE-KEY-provider-alpha-do-not-use"}`

func setRequired(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("OIDC_ISSUER", "http://idp/realms/r")
	t.Setenv("OIDC_JWKS_URL", "http://idp/certs")
	t.Setenv("INSTANCE_ID", "test")
	t.Setenv("SQS_PROVIDER_SIGNING_KEYS", fakeKeys)
}

func TestLoadDefaults(t *testing.T) {
	setRequired(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.WagerQueueName != "wager-transactions.fifo" || c.ReferenceMaxAttempts != 10 || !c.ConsumerEnabled {
		t.Fatalf("unexpected defaults %+v", c)
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	cases := map[string][2]string{
		"missing database":      {"DATABASE_URL", ""},
		"bad duration":          {"PUBLISHER_LEASE", "soon"},
		"bad bool":              {"CONSUMER_ENABLED", "maybe"},
		"non fifo queue":        {"SQS_WAGER_QUEUE", "wager-transactions"},
		"too few attempts":      {"REFERENCE_MAX_ATTEMPTS", "1"},
		"timeout > visibility":  {"CONSUMER_PROCESS_TIMEOUT", "60s"},
		"signing keys not json": {"SQS_PROVIDER_SIGNING_KEYS", "alpha=key"},
		"short signing key":     {"SQS_PROVIDER_SIGNING_KEYS", `{"alpha":"too-short"}`},
		"no signing keys":       {"SQS_PROVIDER_SIGNING_KEYS", `{}`},
	}
	for name, kv := range cases {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			t.Setenv(kv[0], kv[1])
			if _, err := Load(); err == nil {
				t.Fatalf("expected error for %s=%q", kv[0], kv[1])
			}
		})
	}
}

func TestSigningKeys(t *testing.T) {
	t.Run("consumer disabled needs no keys", func(t *testing.T) {
		setRequired(t)
		os.Unsetenv("SQS_PROVIDER_SIGNING_KEYS") // restored by t.Setenv cleanup
		t.Setenv("CONSUMER_ENABLED", "false")
		if _, err := Load(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("consumer enabled without keys fails closed", func(t *testing.T) {
		setRequired(t)
		os.Unsetenv("SQS_PROVIDER_SIGNING_KEYS")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "SQS_PROVIDER_SIGNING_KEYS") {
			t.Fatalf("expected missing keys error, got %v", err)
		}
	})
	t.Run("from file", func(t *testing.T) {
		setRequired(t)
		os.Unsetenv("SQS_PROVIDER_SIGNING_KEYS")
		path := filepath.Join(t.TempDir(), "keys.json")
		if err := os.WriteFile(path, []byte(fakeKeys), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SQS_PROVIDER_SIGNING_KEYS_FILE", path)
		c, err := Load()
		if err != nil || len(c.SQSSigningKeys["alpha"]) == 0 {
			t.Fatalf("keys from file: %v", err)
		}
		if s := fmt.Sprintf("%+v", c); strings.Contains(s, "LOCAL-ONLY") {
			t.Fatal("formatted config leaks a key")
		}
	})
	t.Run("both env and file", func(t *testing.T) {
		setRequired(t)
		t.Setenv("SQS_PROVIDER_SIGNING_KEYS_FILE", "/does/not/matter")
		if _, err := Load(); err == nil {
			t.Fatal("expected error when both are set")
		}
	})
	t.Run("missing file", func(t *testing.T) {
		setRequired(t)
		os.Unsetenv("SQS_PROVIDER_SIGNING_KEYS")
		t.Setenv("SQS_PROVIDER_SIGNING_KEYS_FILE", filepath.Join(t.TempDir(), "absent.json"))
		if _, err := Load(); err == nil {
			t.Fatal("expected error for missing file")
		}
	})
}
