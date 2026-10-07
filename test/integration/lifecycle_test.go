//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"wagering/internal/fxapp"
)

// TestFxStartStopReleasesResources starts the full app with every worker,
// checks readiness, stops it and verifies the listener and the pool are released.
func TestFxStartStopReleasesResources(t *testing.T) {
	cfg := baseConfig(t, createQueues(t, 5))
	cfg.ConsumerEnabled, cfg.PublisherEnabled, cfg.ReferenceWorkerEnabled = true, true, true
	var pool *pgxpool.Pool
	a := fxapp.New(cfg, fx.Populate(&pool))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	must(t, a.Start(ctx))

	r := call(t, "GET", "http://"+cfg.HTTPAddr+"/health/ready", "", nil, nil)
	if r.Status != 200 || r.Body["checks"].(map[string]any)["postgres"] != "up" || r.Body["checks"].(map[string]any)["sqs"] != "up" {
		t.Fatalf("ready: %d %s", r.Status, r.Raw)
	}

	stopStart := time.Now()
	must(t, a.Stop(ctx))
	if time.Since(stopStart) > cfg.ShutdownTimeout {
		t.Fatal("stop exceeded the shutdown timeout")
	}
	if _, err := net.DialTimeout("tcp", cfg.HTTPAddr, time.Second); err == nil {
		t.Fatal("HTTP listener still accepting connections after stop")
	}
	if err := pool.Ping(context.Background()); err == nil {
		t.Fatal("pool must be closed after stop")
	}
}

// TestFxStartFailsOnBadDependency: invalid IdP keys URL aborts startup.
func TestFxStartFailsOnBadDependency(t *testing.T) {
	cfg := baseConfig(t, createQueues(t, 5))
	cfg.OIDCJWKSURL = "http://127.0.0.1:1/certs"
	a := fxapp.New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx); err == nil {
		_ = a.Stop(ctx)
		t.Fatal("start must fail when the IdP is unreachable")
	}

	cfg = baseConfig(t, createQueues(t, 5))
	cfg.WagerQueueName = "does-not-exist-" + newID()[:8] + ".fifo"
	a = fxapp.New(cfg)
	if err := a.Start(ctx); err == nil {
		_ = a.Stop(ctx)
		t.Fatal("start must fail when a queue is missing")
	}
}
