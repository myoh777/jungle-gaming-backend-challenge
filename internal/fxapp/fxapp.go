// Package fxapp composes the service with Uber Fx.
//
// Lifecycle order: Fx runs OnStart hooks in registration order and OnStop
// hooks in reverse. Resources (PostgreSQL pool, SQS queues, IdP keys) are
// constructed first because everything depends on them, so they start first
// and stop last. HTTP and workers are registered afterwards and therefore stop
// accepting work before the pool is closed.
package fxapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"wagering/internal/app"
	"wagering/internal/auth"
	"wagering/internal/config"
	"wagering/internal/domain/wagering"
	"wagering/internal/httpapi"
	"wagering/internal/observability"
	"wagering/internal/outbox"
	"wagering/internal/pendingref"
	"wagering/internal/postgres"
	"wagering/internal/sqsconsumer"
	"wagering/internal/sqsx"
	"wagering/migrations"
)

// New builds the application for the given configuration. extra options are
// appended (tests use them to populate handles such as the pool).
func New(cfg config.Config, extra ...fx.Option) *fx.App {
	opts := []fx.Option{
		fx.Supply(cfg),
		fx.StopTimeout(cfg.ShutdownTimeout),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			l := &fxevent.SlogLogger{Logger: log.With("component", "fx")}
			l.UseLogLevel(slog.LevelDebug)
			return l
		}),
		ObservabilityModule,
		PostgresModule,
		SQSModule,
		AuthModule,
		AppModule,
		HTTPModule,
		WorkersModule,
	}
	return fx.New(append(opts, extra...)...)
}

var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config) *slog.Logger {
			return observability.NewLogger(cfg.LogLevel).With("instanceId", cfg.InstanceID)
		},
		observability.NewMetrics,
	),
)

var PostgresModule = fx.Module("postgres",
	fx.Provide(
		NewPool,
		fx.Annotate(postgres.NewStore, fx.As(new(app.Store))),
		postgres.NewOutboxRepo,
	),
)

// NewPool creates the pool, pings it (and optionally migrates) on start and
// closes it on stop.
func NewPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pcfg.MaxConns = cfg.DBMaxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), pcfg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := pool.Ping(ctx); err != nil {
				return fmt.Errorf("postgres ping: %w", err)
			}
			if cfg.AutoMigrate {
				applied, err := postgres.MigrateUp(ctx, pool, migrations.FS)
				if err != nil {
					return fmt.Errorf("migrate: %w", err)
				}
				log.Info("migrations applied", "versions", applied)
			}
			log.Info("postgres ready")
			return nil
		},
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("postgres pool closed")
			return nil
		},
	})
	return pool, nil
}

// Queues is filled when the app starts (queue URLs resolved against SQS).
type Queues struct{ sqsx.Queues }

var SQSModule = fx.Module("sqs",
	fx.Provide(
		func(cfg config.Config) (*sqs.Client, error) {
			return sqsx.NewClient(context.Background(), cfg)
		},
		func(lc fx.Lifecycle, client *sqs.Client, cfg config.Config, log *slog.Logger) *Queues {
			q := &Queues{}
			lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
				resolved, err := sqsx.Resolve(ctx, client, cfg)
				if err != nil {
					return err
				}
				q.Queues = resolved
				log.Info("sqs queues resolved")
				return nil
			}})
			return q
		},
	),
)

var AuthModule = fx.Module("auth",
	fx.Provide(func(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) *auth.Verifier {
		v := auth.NewVerifier(auth.Config{Issuer: cfg.OIDCIssuer, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience})
		lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
			if err := v.CheckJWKS(ctx); err != nil {
				return fmt.Errorf("identity provider: %w", err)
			}
			log.Info("identity provider keys reachable")
			return nil
		}})
		return v
	}),
)

var AppModule = fx.Module("app",
	fx.Provide(
		func(cfg config.Config) app.WagerConfig {
			return app.WagerConfig{Retry: wagering.RetryPolicy{
				BaseDelay: cfg.ReferenceRetryBase, MaxDelay: cfg.ReferenceRetryMax,
				MaxAttempts: cfg.ReferenceMaxAttempts, TTL: cfg.ReferenceTTL,
			}}
		},
		app.NewWagerService,
		app.NewWalletService,
		app.NewReferenceResolver,
	),
)

var HTTPModule = fx.Module("http",
	fx.Provide(func(w *app.WagerService, wl *app.WalletService, v *auth.Verifier, pool *pgxpool.Pool,
		client *sqs.Client, q *Queues, m *observability.Metrics, log *slog.Logger) *httpapi.Handler {
		checks := map[string]httpapi.ReadinessCheck{
			"postgres": pool.Ping,
			"sqs":      func(ctx context.Context) error { return sqsx.Ping(ctx, client, q.WagerURL) },
		}
		return httpapi.NewHandler(w, wl, v, checks, m, log)
	}),
	fx.Invoke(RegisterHTTPServer),
)

// RegisterHTTPServer binds the listener on start (failing fast if the port is
// taken) and drains it on stop: readiness turns 503, then Shutdown stops
// accepting connections and waits for in-flight requests until the deadline.
func RegisterHTTPServer(lc fx.Lifecycle, cfg config.Config, h *httpapi.Handler, log *slog.Logger) {
	srv := &http.Server{
		Addr: cfg.HTTPAddr, Handler: h.Routes(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := net.Listen("tcp", cfg.HTTPAddr)
			if err != nil {
				return err
			}
			go func() {
				if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
					log.Error("http server failed", "error", err.Error())
				}
			}()
			log.Info("http server listening", "addr", ln.Addr().String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			h.SetDraining()
			err := srv.Shutdown(ctx)
			log.Info("http server stopped")
			return err
		},
	})
}

var WorkersModule = fx.Module("workers",
	fx.Provide(
		func(client *sqs.Client, w *app.WagerService, q *Queues, cfg config.Config, m *observability.Metrics, log *slog.Logger) *sqsconsumer.Consumer {
			return sqsconsumer.New(client, w, sqsconsumer.Config{
				ConsumerName: cfg.ConsumerName,
				QueueURL:     func() string { return q.WagerURL },
				DLQURL:       func() string { return q.DLQURL },
				WaitSeconds:  cfg.ConsumerWaitSeconds, VisibilityTimeout: cfg.ConsumerVisibilityTimeout,
				MaxMessages: cfg.ConsumerMaxMessages, ProcessTimeout: cfg.ConsumerProcessTimeout,
				RetryBase: cfg.ConsumerRetryBase, RetryMax: cfg.ConsumerRetryMax,
			}, m, log)
		},
		func(repo *postgres.OutboxRepo, client *sqs.Client, q *Queues, cfg config.Config, m *observability.Metrics, log *slog.Logger) *outbox.Publisher {
			sender := sqsx.NewEventSender(client, func() string { return q.EventsURL })
			return outbox.NewPublisher(repo, sender, outbox.Config{
				Owner: cfg.InstanceID + "/" + app.NewID(), PollInterval: cfg.PublisherPollInterval,
				BatchSize: cfg.PublisherBatchSize, Lease: cfg.PublisherLease,
				RetryBase: cfg.PublisherRetryBase, RetryMax: cfg.PublisherRetryMax,
			}, m, log)
		},
		func(r *app.ReferenceResolver, cfg config.Config, log *slog.Logger) *pendingref.Worker {
			return pendingref.NewWorker(r, cfg.ReferencePollInterval, cfg.ReferenceBatchSize, log)
		},
	),
	fx.Invoke(RegisterWorkers),
)

type worker interface {
	Start()
	Stop(ctx context.Context) error
}

// RegisterWorkers starts the enabled workers after HTTP and stops them before it.
func RegisterWorkers(lc fx.Lifecycle, cfg config.Config, c *sqsconsumer.Consumer, p *outbox.Publisher, r *pendingref.Worker) {
	for _, w := range []struct {
		enabled bool
		w       worker
	}{
		{cfg.ConsumerEnabled, c},
		{cfg.PublisherEnabled, p},
		{cfg.ReferenceWorkerEnabled, r},
	} {
		if !w.enabled {
			continue
		}
		w := w.w
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error { w.Start(); return nil },
			OnStop:  w.Stop,
		})
	}
}
