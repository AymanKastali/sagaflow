package eventstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	migrations "github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/platform/pgtest"
)

// One container for the package, per spec §12.4. Isolation between tests comes
// from the database name newDB is given, not from a fresh container.
func TestMain(m *testing.M) {
	stop, err := pgtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}

func newDB(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.Shared(t).DSN(t, name)
	if err := pg.Migrate(ctx, dsn, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestSchemaRejectsDuplicateStreamVersion(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "eventstore_schema")

	const ins = `INSERT INTO events (stream_id, version, type, data, meta)
	             VALUES ($1, $2, 'T', '{}'::jsonb, '{}'::jsonb)`

	if _, err := pool.Exec(ctx, ins, "s-1", 1); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err := pool.Exec(ctx, ins, "s-1", 1)
	if err == nil {
		t.Fatal("want unique violation on (stream_id, version), got nil")
	}
}

func TestSchemaHasNoRedundantIndex(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "eventstore_index")

	rows, err := pool.Query(ctx,
		`SELECT indexdef FROM pg_indexes WHERE tablename = 'events'`)
	if err != nil {
		t.Fatalf("query indexes: %v", err)
	}
	defer rows.Close()

	var n int
	for rows.Next() {
		var def string
		if err := rows.Scan(&def); err != nil {
			t.Fatalf("scan: %v", err)
		}
		n++
		t.Logf("index: %s", def)
	}
	// Exactly two: the global_seq primary key and the (stream_id, version)
	// unique constraint. A third means someone re-added the redundant index.
	if n != 2 {
		t.Fatalf("want exactly 2 indexes on events, got %d", n)
	}
}
