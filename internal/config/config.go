// Package config loads and validates configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	InstanceID string
	LogLevel   string

	HTTPAddr        string
	ShutdownTimeout time.Duration

	DatabaseURL string
	DBMaxConns  int32
	AutoMigrate bool

	AWSRegion       string
	AWSEndpoint     string // LocalStack/MiniStack endpoint; empty for real AWS
	AWSAccessKeyID  string
	AWSSecretKey    string
	WagerQueueName  string
	WagerDLQName    string
	EventsQueueName string

	OIDCIssuer   string // expected "iss" claim
	OIDCJWKSURL  string // where to fetch signing keys (may be an internal hostname)
	OIDCAudience string // expected "aud" claim

	ConsumerEnabled           bool
	ConsumerName              string
	ConsumerWaitSeconds       int32
	ConsumerVisibilityTimeout int32
	ConsumerMaxMessages       int32
	ConsumerProcessTimeout    time.Duration
	ConsumerRetryBase         time.Duration
	ConsumerRetryMax          time.Duration

	PublisherEnabled      bool
	PublisherPollInterval time.Duration
	PublisherBatchSize    int
	PublisherLease        time.Duration
	PublisherRetryBase    time.Duration
	PublisherRetryMax     time.Duration

	ReferenceWorkerEnabled bool
	ReferencePollInterval  time.Duration
	ReferenceBatchSize     int
	ReferenceRetryBase     time.Duration
	ReferenceRetryMax      time.Duration
	ReferenceMaxAttempts   int
	ReferenceTTL           time.Duration
}

// Load reads the environment. Defaults target the local Docker Compose stack.
func Load() (Config, error) {
	r := reader{}
	host, _ := os.Hostname()
	c := Config{
		InstanceID:      r.str("INSTANCE_ID", host),
		LogLevel:        r.str("LOG_LEVEL", "info"),
		HTTPAddr:        r.str("HTTP_ADDR", ":8080"),
		ShutdownTimeout: r.dur("SHUTDOWN_TIMEOUT", 20*time.Second),

		DatabaseURL: r.str("DATABASE_URL", ""),
		DBMaxConns:  int32(r.int("DB_MAX_CONNS", 20)),
		AutoMigrate: r.bool("AUTO_MIGRATE", false),

		AWSRegion:       r.str("AWS_REGION", "us-east-1"),
		AWSEndpoint:     r.str("AWS_ENDPOINT_URL", ""),
		AWSAccessKeyID:  r.str("AWS_ACCESS_KEY_ID", ""),
		AWSSecretKey:    r.str("AWS_SECRET_ACCESS_KEY", ""),
		WagerQueueName:  r.str("SQS_WAGER_QUEUE", "wager-transactions.fifo"),
		WagerDLQName:    r.str("SQS_WAGER_DLQ", "wager-transactions-dlq.fifo"),
		EventsQueueName: r.str("SQS_EVENTS_QUEUE", "wallet-events"),

		OIDCIssuer:   r.str("OIDC_ISSUER", ""),
		OIDCJWKSURL:  r.str("OIDC_JWKS_URL", ""),
		OIDCAudience: r.str("OIDC_AUDIENCE", "wagering-api"),

		ConsumerEnabled:           r.bool("CONSUMER_ENABLED", true),
		ConsumerName:              r.str("CONSUMER_NAME", "wager-transactions-consumer"),
		ConsumerWaitSeconds:       int32(r.int("CONSUMER_WAIT_SECONDS", 10)),
		ConsumerVisibilityTimeout: int32(r.int("CONSUMER_VISIBILITY_TIMEOUT_SECONDS", 30)),
		ConsumerMaxMessages:       int32(r.int("CONSUMER_MAX_MESSAGES", 10)),
		ConsumerProcessTimeout:    r.dur("CONSUMER_PROCESS_TIMEOUT", 15*time.Second),
		ConsumerRetryBase:         r.dur("CONSUMER_RETRY_BASE", 2*time.Second),
		ConsumerRetryMax:          r.dur("CONSUMER_RETRY_MAX", 60*time.Second),

		PublisherEnabled:      r.bool("PUBLISHER_ENABLED", true),
		PublisherPollInterval: r.dur("PUBLISHER_POLL_INTERVAL", 500*time.Millisecond),
		PublisherBatchSize:    r.int("PUBLISHER_BATCH_SIZE", 50),
		PublisherLease:        r.dur("PUBLISHER_LEASE", 30*time.Second),
		PublisherRetryBase:    r.dur("PUBLISHER_RETRY_BASE", time.Second),
		PublisherRetryMax:     r.dur("PUBLISHER_RETRY_MAX", time.Minute),

		ReferenceWorkerEnabled: r.bool("REFERENCE_WORKER_ENABLED", true),
		ReferencePollInterval:  r.dur("REFERENCE_POLL_INTERVAL", time.Second),
		ReferenceBatchSize:     r.int("REFERENCE_BATCH_SIZE", 50),
		ReferenceRetryBase:     r.dur("REFERENCE_RETRY_BASE", 2*time.Second),
		ReferenceRetryMax:      r.dur("REFERENCE_RETRY_MAX", 5*time.Minute),
		ReferenceMaxAttempts:   r.int("REFERENCE_MAX_ATTEMPTS", 10),
		ReferenceTTL:           r.dur("REFERENCE_TTL", time.Hour),
	}
	if err := r.err(); err != nil {
		return Config{}, err
	}
	return c, c.Validate()
}

// Validate fails fast on missing or inconsistent configuration.
func (c Config) Validate() error {
	var errs []error
	req := map[string]string{
		"DATABASE_URL": c.DatabaseURL, "HTTP_ADDR": c.HTTPAddr, "AWS_REGION": c.AWSRegion,
		"SQS_WAGER_QUEUE": c.WagerQueueName, "SQS_WAGER_DLQ": c.WagerDLQName, "SQS_EVENTS_QUEUE": c.EventsQueueName,
		"OIDC_ISSUER": c.OIDCIssuer, "OIDC_JWKS_URL": c.OIDCJWKSURL, "OIDC_AUDIENCE": c.OIDCAudience,
		"CONSUMER_NAME": c.ConsumerName, "INSTANCE_ID": c.InstanceID,
	}
	for k, v := range req {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", k))
		}
	}
	if !strings.HasSuffix(c.WagerQueueName, ".fifo") || !strings.HasSuffix(c.WagerDLQName, ".fifo") {
		errs = append(errs, errors.New("wager queue and DLQ must be FIFO (.fifo)"))
	}
	if c.ReferenceMaxAttempts < 2 {
		errs = append(errs, errors.New("REFERENCE_MAX_ATTEMPTS must be >= 2"))
	}
	if c.ConsumerVisibilityTimeout <= 0 || c.ConsumerWaitSeconds < 0 || c.ConsumerWaitSeconds > 20 ||
		c.ConsumerMaxMessages < 1 || c.ConsumerMaxMessages > 10 {
		errs = append(errs, errors.New("invalid consumer settings (wait 0-20s, max messages 1-10, visibility > 0)"))
	}
	if c.ConsumerProcessTimeout >= time.Duration(c.ConsumerVisibilityTimeout)*time.Second {
		errs = append(errs, errors.New("CONSUMER_PROCESS_TIMEOUT must be shorter than the visibility timeout"))
	}
	for name, d := range map[string]time.Duration{
		"SHUTDOWN_TIMEOUT": c.ShutdownTimeout, "PUBLISHER_POLL_INTERVAL": c.PublisherPollInterval,
		"PUBLISHER_LEASE": c.PublisherLease, "PUBLISHER_RETRY_BASE": c.PublisherRetryBase,
		"REFERENCE_POLL_INTERVAL": c.ReferencePollInterval, "REFERENCE_RETRY_BASE": c.ReferenceRetryBase,
		"REFERENCE_TTL": c.ReferenceTTL, "CONSUMER_RETRY_BASE": c.ConsumerRetryBase,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	if c.PublisherBatchSize < 1 || c.ReferenceBatchSize < 1 || c.DBMaxConns < 1 {
		errs = append(errs, errors.New("batch sizes and DB_MAX_CONNS must be >= 1"))
	}
	return errors.Join(errs...)
}

type reader struct{ errs []error }

func (r *reader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func (r *reader) int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return n
}

func (r *reader) bool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return b
}

func (r *reader) dur(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return d
}

func (r *reader) err() error { return errors.Join(r.errs...) }
