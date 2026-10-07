package config

import "testing"

func setRequired(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("OIDC_ISSUER", "http://idp/realms/r")
	t.Setenv("OIDC_JWKS_URL", "http://idp/certs")
	t.Setenv("INSTANCE_ID", "test")
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
		"missing database":     {"DATABASE_URL", ""},
		"bad duration":         {"PUBLISHER_LEASE", "soon"},
		"bad bool":             {"CONSUMER_ENABLED", "maybe"},
		"non fifo queue":       {"SQS_WAGER_QUEUE", "wager-transactions"},
		"too few attempts":     {"REFERENCE_MAX_ATTEMPTS", "1"},
		"timeout > visibility": {"CONSUMER_PROCESS_TIMEOUT", "60s"},
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
