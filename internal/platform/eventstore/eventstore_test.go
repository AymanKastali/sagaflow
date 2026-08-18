package eventstore_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	migrations "github.com/AymanKastali/sagaflow/internal/inventory/migrations"
	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/testsupport/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// One container for the package. Isolation between tests comes from the
// database name newDB is given, not from a fresh container.
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
	return pgtest.Shared(t).Migrated(t, name, migrations.FS)
}

func TestSchemaRejectsDuplicateStreamVersion(t *testing.T) {
	ctx := t.Context()
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
	ctx := t.Context()
	pool := newDB(t, "eventstore_index")

	rows, err := pool.Query(ctx,
		`SELECT indexdef FROM pg_indexes WHERE tablename = 'events'`)
	if err != nil {
		t.Fatalf("query indexes: %v", err)
	}
	defs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read indexes: %v", err)
	}
	// Exactly two: the global_seq primary key and the (stream_id, version)
	// unique constraint. A third means someone re-added the redundant index.
	if len(defs) != 2 {
		t.Fatalf("want exactly 2 indexes on events, got %d:\n%s", len(defs), strings.Join(defs, "\n"))
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
	ctx := t.Context()
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
	got, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (string, error) {
		var version int
		var typ string
		err := r.Scan(&version, &typ)
		return fmt.Sprintf("%d:%s", version, typ), err
	})
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if want := []string{"1:A", "2:B", "3:C"}; !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestAppendStaleExpectedVersionReturnsErrVersionConflict(t *testing.T) {
	ctx := t.Context()
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
	ctx := t.Context()
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
	ctx := t.Context()
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

func TestLoadReturnsEventsInVersionOrder(t *testing.T) {
	ctx := t.Context()
	pool := newDB(t, "load_order")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14A", 0,
			[]eventstore.Event{ev("A"), ev("B")})
	}); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14A", 2, []eventstore.Event{ev("C")})
	}); err != nil {
		t.Fatalf("append 2: %v", err)
	}

	var got []eventstore.Recorded
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		got, err = eventstore.Load(ctx, tx, "seat-14A")
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("want 3 events, got %d", len(got))
	}
	for i, want := range []string{"A", "B", "C"} {
		if got[i].Type != want {
			t.Fatalf("event %d: want type %s, got %s", i, want, got[i].Type)
		}
		if got[i].Version != i+1 {
			t.Fatalf("event %d: want version %d, got %d", i, i+1, got[i].Version)
		}
	}
	if got[0].Meta.TraceID != "trace-1" {
		t.Fatalf("meta did not round-trip: %+v", got[0].Meta)
	}
	// Semantic comparison, not byte comparison. JSONB is not a byte-preserving
	// store: Postgres reparses and re-serialises, so {"k":"v"} comes back as
	// {"k": "v"} and key order is not promised either — which is why the
	// stored form must never be hashed or used as a cache key.
	var data map[string]string
	if err := json.Unmarshal(got[0].Data, &data); err != nil {
		t.Fatalf("stored data is not valid JSON: %s: %v", got[0].Data, err)
	}
	if data["k"] != "v" {
		t.Fatalf("data did not round-trip: %s", got[0].Data)
	}
	if got[0].RecordedAt.IsZero() {
		t.Fatal("recorded_at was not populated")
	}
}

func TestLoadUnknownStreamIsEmptyNotError(t *testing.T) {
	ctx := t.Context()
	pool := newDB(t, "load_missing")

	var got []eventstore.Recorded
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		got, err = eventstore.Load(ctx, tx, "seat-nobody-held")
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 events for unknown stream, got %d", len(got))
	}
}

func TestLoadIsolatesStreams(t *testing.T) {
	ctx := t.Context()
	pool := newDB(t, "load_isolation")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if err := eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{ev("A")}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("append 14A: %v", err)
	}
	// A second stream at the same version — legal, and must not bleed across.
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14B", 0, []eventstore.Event{ev("B")})
	}); err != nil {
		t.Fatalf("append 14B: %v", err)
	}

	var got []eventstore.Recorded
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		got, err = eventstore.Load(ctx, tx, "seat-14B")
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 1 || got[0].Type != "B" {
		t.Fatalf("want only seat-14B's single event, got %+v", got)
	}
}
