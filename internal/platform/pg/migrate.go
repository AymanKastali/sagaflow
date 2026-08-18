package pg

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/tern/v2/migrate"
)

// Migrate applies every migration in fsys that has not yet run.
//
// tern takes a *pgx.Conn rather than a pool because it holds a session-level
// advisory lock for the duration, which makes concurrent migrations from two
// starting replicas safe: the second waits, then finds nothing to do.
//
// Migration files must be named NNN_description.sql and use the separator
// "---- create above / drop below ----" between the up and down halves.
func Migrate(ctx context.Context, dsn string, fsys fs.FS) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	m, err := migrate.NewMigrator(ctx, conn, "schema_version")
	if err != nil {
		return fmt.Errorf("new migrator: %w", err)
	}
	if err := m.LoadMigrations(fsys); err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	if err := m.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}
