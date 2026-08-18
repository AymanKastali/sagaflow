package pg_test

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"testing/fstest"

	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/testsupport/pgtest"
	"github.com/jackc/pgx/v5"
)

// One container for the whole package, started once and shared by every test,
// rather than one per test — starting a container per test would make the
// suite too slow to run routinely. Every integration test package in this
// repository has a TestMain of exactly this shape.
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

var schema = fstest.MapFS{
	"001_widgets.sql": &fstest.MapFile{Data: []byte(
		"CREATE TABLE widgets (id INT PRIMARY KEY);\n" +
			"---- create above / drop below ----\n" +
			"DROP TABLE widgets;\n")},
}

func TestMigrateCreatesSchemaAndIsIdempotent(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.Shared(t).DSN(t, "migrate_test")

	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("second migrate must be a no-op, got: %v", err)
	}

	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("widgets table missing after migrate: %v", err)
	}
}

func TestWithTxRollsBackOnError(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.Shared(t).DSN(t, "withtx_test")
	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	sentinel := errors.New("handler failed")
	err = pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO widgets (id) VALUES (1)"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel error returned to caller, got %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 rows after rollback, got %d", n)
	}
}

func TestWithTxCommitsOnSuccess(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.Shared(t).DSN(t, "withtx_commit_test")
	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO widgets (id) VALUES (2)")
		return err
	}); err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM widgets WHERE id = 2").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 committed row, got %d", n)
	}
}
