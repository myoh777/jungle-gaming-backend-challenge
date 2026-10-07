package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockID is the pg_advisory_lock key that serializes concurrent
// migration runs (several instances starting at once).
const migrationLockID int64 = 727274001

type migration struct {
	version int
	name    string
	up      string
	down    string
}

func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	byVersion := map[int]*migration{}
	for _, e := range entries {
		name := e.Name()
		var direction string
		switch {
		case strings.HasSuffix(name, ".up.sql"):
			direction = "up"
		case strings.HasSuffix(name, ".down.sql"):
			direction = "down"
		default:
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: expected NNNN_name.(up|down).sql", name)
		}
		v, err := strconv.Atoi(prefix)
		if err != nil {
			return nil, fmt.Errorf("migration %q: bad version: %w", name, err)
		}
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, err
		}
		m := byVersion[v]
		if m == nil {
			m = &migration{version: v, name: strings.TrimSuffix(strings.TrimSuffix(name, ".up.sql"), ".down.sql")}
			byVersion[v] = m
		}
		if direction == "up" {
			m.up = string(body)
		} else {
			m.down = string(body)
		}
	}
	var out []migration
	for _, m := range byVersion {
		if m.up == "" || m.down == "" {
			return nil, fmt.Errorf("migration %d: both up and down files are required", m.version)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// MigrateUp applies every pending migration, each in its own transaction,
// holding an advisory lock so concurrent runs are safe. Returns applied versions.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) ([]int, error) {
	migs, err := loadMigrations(fsys)
	if err != nil {
		return nil, err
	}
	var applied []int
	err = withMigrationLock(ctx, pool, func(conn *pgxpool.Conn) error {
		done, err := appliedVersions(ctx, conn)
		if err != nil {
			return err
		}
		for _, m := range migs {
			if done[m.version] {
				continue
			}
			err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, m.up); err != nil {
					return fmt.Errorf("apply %s: %w", m.name, err)
				}
				_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, now())`, m.version, m.name)
				return err
			})
			if err != nil {
				return err
			}
			applied = append(applied, m.version)
		}
		return nil
	})
	return applied, err
}

// MigrateDown reverts the last `steps` applied migrations, newest first.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, steps int) ([]int, error) {
	migs, err := loadMigrations(fsys)
	if err != nil {
		return nil, err
	}
	var reverted []int
	err = withMigrationLock(ctx, pool, func(conn *pgxpool.Conn) error {
		done, err := appliedVersions(ctx, conn)
		if err != nil {
			return err
		}
		for i := len(migs) - 1; i >= 0 && len(reverted) < steps; i-- {
			m := migs[i]
			if !done[m.version] {
				continue
			}
			err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, m.down); err != nil {
					return fmt.Errorf("revert %s: %w", m.name, err)
				}
				_, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, m.version)
				return err
			})
			if err != nil {
				return err
			}
			reverted = append(reverted, m.version)
		}
		return nil
	})
	return reverted, err
}

func withMigrationLock(ctx context.Context, pool *pgxpool.Pool, fn func(*pgxpool.Conn) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID) //nolint:errcheck
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer PRIMARY KEY,
		name       text NOT NULL,
		applied_at timestamptz NOT NULL)`); err != nil {
		return err
	}
	return fn(conn)
}

func appliedVersions(ctx context.Context, conn *pgxpool.Conn) (map[int]bool, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return nil, err
	}
	out := map[int]bool{}
	for _, v := range versions {
		out[v] = true
	}
	return out, nil
}
