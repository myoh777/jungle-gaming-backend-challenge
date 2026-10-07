// Command migrate applies or reverts the versioned SQL migrations.
//
//	migrate up           apply all pending migrations
//	migrate down [N]     revert the last N applied migrations (default 1)
//
// It reads DATABASE_URL.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"wagering/internal/postgres"
	"wagering/migrations"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: migrate up | down [N]")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()

	switch args[0] {
	case "up":
		applied, err := postgres.MigrateUp(ctx, pool, migrations.FS)
		if err != nil {
			return err
		}
		fmt.Println("applied:", applied)
	case "down":
		steps := 1
		if len(args) > 1 {
			if steps, err = strconv.Atoi(args[1]); err != nil || steps < 1 {
				return fmt.Errorf("invalid step count %q", args[1])
			}
		}
		reverted, err := postgres.MigrateDown(ctx, pool, migrations.FS, steps)
		if err != nil {
			return err
		}
		fmt.Println("reverted:", reverted)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	return nil
}
