package fxapp

import (
	"testing"

	"go.uber.org/fx"

	"wagering/internal/config"
)

// TestGraphIsValid checks that every dependency can be resolved without
// running constructors or touching infrastructure. Start/stop against real
// PostgreSQL, SQS and Keycloak is covered by test/integration.
func TestGraphIsValid(t *testing.T) {
	cfg := config.Config{ShutdownTimeout: 1}
	err := fx.ValidateApp(
		fx.Supply(cfg),
		ObservabilityModule, PostgresModule, SQSModule, AuthModule, AppModule, HTTPModule, WorkersModule,
	)
	if err != nil {
		t.Fatal(err)
	}
}
