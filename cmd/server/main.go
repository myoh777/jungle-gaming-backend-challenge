// Command server runs the wagering service (HTTP API + SQS consumer + outbox
// publisher + pending reference worker). It stops gracefully on SIGINT/SIGTERM.
package main

import (
	"fmt"
	"os"

	"wagering/internal/config"
	"wagering/internal/fxapp"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(2)
	}
	app := fxapp.New(cfg)
	if err := app.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "cannot build application:", err)
		os.Exit(1)
	}
	// Run starts the app, blocks until a signal arrives and then stops it
	// within cfg.ShutdownTimeout; it exits non-zero if start or stop fails.
	app.Run()
}
