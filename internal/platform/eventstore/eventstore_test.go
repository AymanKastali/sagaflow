package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	migrations "github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
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

func ev(t string) eventstore.Event {
	return eventstore.Event{
		Type: t,
		Data: []byte(`{"k":"v"}`),
		Meta: eventstore.Meta{TraceID: "trace-1", CorrelationID: "corr-1"},
	}
}

func TestAppendAssignsSequentialVersions(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "append_versions")

	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14A", 0,
			[]eventstore.Event{ev("A"), ev("B"), ev("C")})
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	rows, err := pool.Query(ctx,
		`SELECT version, type FROM events WHERE stream_id = $1 ORDER BY version`, "seat-14A")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got []string
	want := []string{"1:A", "2:B", "3:C"}
	for rows.Next() {
		var v int
		var typ string
		if err := rows.Scan(&v, &typ); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, fmt.Sprintf("%d:%s", v, typ))
	}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

func TestAppendStaleExpectedVersionReturnsErrVersionConflict(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "append_stale")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{ev("A")})
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	// A caller that folded from version 0 and did not notice the write above.
	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{ev("B")})
	})
	if !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}
}

func TestAppendConcurrentWritersExactlyOneWins(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "append_race")

	// This is the test the whole optimistic-concurrency design exists for, and
	// the reason the seat hold in phase 5 needs no locks: two racers folding
	// from the same version both try to write version 1.
	const racers = 8
	var wg sync.WaitGroup
	results := make([]error, racers)
	start := make(chan struct{})

	for i := range racers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
				return eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{ev("Held")})
			})
		}(i)
	}
	close(start)
	wg.Wait()

	var won, conflicted int
	for i, err := range results {
		switch {
		case err == nil:
			won++
		case errors.Is(err, eventstore.ErrVersionConflict):
			conflicted++
		default:
			t.Fatalf("racer %d got unexpected error: %v", i, err)
		}
	}
	if won != 1 {
		t.Fatalf("want exactly 1 winner, got %d (conflicts: %d)", won, conflicted)
	}
	if conflicted != racers-1 {
		t.Fatalf("want %d conflicts, got %d", racers-1, conflicted)
	}

	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM events WHERE stream_id = $1`, "seat-14A").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 event stored, got %d", n)
	}
}

func TestAppendEmptySliceIsANoOp(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "append_empty")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14A", 0, nil)
	}); err != nil {
		t.Fatalf("append nil: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 events, got %d", n)
	}
}
