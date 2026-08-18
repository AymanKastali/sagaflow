# SagaFlow Phase 2 — Event Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the append-only event log with optimistic concurrency, and prove that two writers folding from the same version produce exactly one winner.

**Architecture:** One `events` table per service database, with `UNIQUE (stream_id, version)` as the entire concurrency-control mechanism — no `SELECT … FOR UPDATE`, no table locks, no counters. `Append` is a single statement whose unique violation is translated into `ErrVersionConflict`; `Load` folds nothing and returns rows in version order, leaving state reconstruction to each service. The package knows only "a type name and some JSON", so payload encoding stays out of it.

**Tech Stack:** Go 1.26.6, Postgres 18.6, pgx v5.10.0, tern v2.4.2.

**Spec:** [docs/superpowers/specs/2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) — §6 (event store), §13 phase 2

**Plan sequence:** this is plan 2 of 6. See [README.md](README.md). **Depends on Phase 1** for `pg.Open`, `pg.WithTx`, `pg.Migrate` and `pgtest`. Phases 3 onward depend on this one.

**Deliverable that ends this phase:** `TestAppendConcurrentWritersExactlyOneWins` passes with eight racing goroutines — one commit, seven `ErrVersionConflict`, one row in the table.

## Global Constraints

Copied verbatim from spec §5 and §3. Every task's requirements implicitly include this section.

- **Go 1.26.6.** `go.mod` declares `go 1.26.6`; with `GOTOOLCHAIN=auto` the go command fetches that toolchain itself, so the installed go may be older and no machine-level upgrade is required.
- **Module path:** `github.com/kptac/sagaflow`. One module at the repository root.
- **Pinned images, never `latest`:** `apache/kafka:4.3.1`, `postgres:18.6`, `apicurio/apicurio-registry:3.3.1`, `cr.jaegertracing.io/jaegertracing/jaeger:2.20.0`.
- **Pinned Go dependencies** (spec §5): franz-go v1.21.6, `franz-go/pkg/sr` v1.8.0, `franz-go/pkg/kadm` v1.18.0, pgx/v5 v5.10.0, tern/v2 v2.4.2, google/uuid v1.6.0, protobuf v1.36.12, otel + otel/sdk v1.45.0, testcontainers-go v0.44.0. **Add no dependency not listed in §5.**
- **The OTel modules move as a set.** `otel`, `otel/sdk` and the trace exporter are released together; mixing versions fails at runtime, not build time.
- **Never `testcontainers-go/modules/kafka`** (spec D16). It only runs Confluent images. Kafka test containers come from `platform/kafkatest`.
- **One transaction writes exactly one stream** (spec §7.2), plus its outbox rows and its inbox row. Never two streams.
- **`global_seq` is diagnostic only** (spec §6.4). No component may read events by a monotonic local cursor.
- **`expires_at` and `fire_at` are application-supplied, never `DEFAULT now()`** (spec §10.5). Only diagnostic columns such as `recorded_at` use the database clock.
- **The outbox is at-least-once, never exactly-once** (spec §10.1). Exactly-once application comes from the inbox.
- **Services never auto-register schemas** (spec D14). Registration is an explicit `make` target.
- **Postgres error code `23505`** is a unique violation; it is a control-flow signal in this codebase, not a crash.

### Two deliberate refinements to the spec

Both are noted here so a reviewer does not read them as drift:

1. **`Append` takes a `context.Context` first parameter.** Spec §6.2 writes `Append(tx pgx.Tx, streamID string, expectedVersion int, evts []Event) error`. Every pgx call requires a context, so the real signature is `Append(ctx, tx, streamID, expectedVersion, evts)`. The spec's point — that the caller supplies the transaction and the expected version — is unchanged.
2. **Proto files live at `proto/sagaflow/<service>/v1/`, not `proto/<service>/v1/`.** Spec §7's tree shows the shorter path, but §8.1 requires `ce_type` values like `sagaflow.inventory.v1.SeatHeld`, which means the proto package is `sagaflow.inventory.v1`, and buf's `STANDARD` lint rule `PACKAGE_DIRECTORY_MATCH` requires the directory to match the package. §8.1's names are load-bearing at runtime — they are the `ce_type` header and the `protoregistry` lookup key — so the directory yields.

---


## File Structure

| File | Responsibility |
|---|---|
| `internal/inventory/migrations/001_events.sql` | Inventory's `events` table |
| `internal/inventory/migrations/migrations.go` | `embed.FS` for the above |
| `internal/booking/migrations/001_events.sql` | Booking's `events` table — byte-identical, separate database |
| `internal/booking/migrations/migrations.go` | `embed.FS` for the above |
| `internal/platform/eventstore/eventstore.go` | `Event`, `Recorded`, `Meta`, `Append`, `Load` |
| `internal/platform/eventstore/errors.go` | `ErrVersionConflict` |
| `internal/platform/eventstore/eventstore_test.go` | Schema assertions, append semantics, the concurrency race |

The two migration files are duplicated on purpose. Each service owns its schema, and the day one needs a column the other does not, there is nothing to disentangle. Do not factor them into a shared package.

---

## Phase 2 Tasks

### Task 1: Service migrations and the events table

**Files:**
- Create: `internal/inventory/migrations/001_events.sql`, `internal/inventory/migrations/migrations.go`
- Create: `internal/booking/migrations/001_events.sql`, `internal/booking/migrations/migrations.go`
- Create: `internal/platform/eventstore/eventstore_test.go` (schema assertions only in this task)

**Interfaces:**
- Consumes: `pg.Migrate`, `pg.Open`, `pg.WithTx` and `pgtest` from Phase 1.
- Produces: `inventorymigrations.FS` and `bookingmigrations.FS`, both `embed.FS`, each applying an identical `events` table to its own database.

The two files are byte-identical and that is intentional: each service owns its schema, and the day one of them needs a column the other does not, there is nothing to disentangle. Do not factor them into a shared file.

- [ ] **Step 1: Write `internal/inventory/migrations/001_events.sql`**

```sql
CREATE TABLE events (
    global_seq  BIGSERIAL   PRIMARY KEY,
    stream_id   TEXT        NOT NULL,
    version     INT         NOT NULL,
    type        TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    meta        JSONB       NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, version)
);

COMMENT ON COLUMN events.global_seq IS
    'Diagnostic and replay tooling only. BIGSERIAL commits out of order, so no consumer may track a cursor over this column.';

---- create above / drop below ----

DROP TABLE events;
```

There is deliberately no `CREATE INDEX` on `(stream_id, version)`: the `UNIQUE` constraint already builds that exact btree, and a second copy would double write cost on the hottest table for no read benefit (spec §6.1).

- [ ] **Step 2: Write the embed shim**

Create `internal/inventory/migrations/migrations.go`:

```go
// Package migrations holds inventory's own schema. Each service migrates its
// own database; the SQL being identical across services today is a coincidence
// of timing, not a shared contract.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
```

- [ ] **Step 3: Copy both files to booking**

```bash
mkdir -p internal/booking/migrations
cp internal/inventory/migrations/001_events.sql internal/booking/migrations/001_events.sql
sed 's/inventory/booking/' internal/inventory/migrations/migrations.go > internal/booking/migrations/migrations.go
```

Verify the doc comment now reads "booking's own schema":

```bash
head -3 internal/booking/migrations/migrations.go
```

- [ ] **Step 4: Write the failing test that the schema applies and enforces its constraint**

Create `internal/platform/eventstore/eventstore_test.go`:

```go
package eventstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/platform/pgtest"
	migrations "github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
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
```

- [ ] **Step 5: Run it**

Run: `go test ./internal/platform/eventstore/ -v`
Expected: both PASS. If `TestSchemaHasNoRedundantIndex` reports 3, remove the extra `CREATE INDEX` from the migration.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory/migrations internal/booking/migrations internal/platform/eventstore
git commit -m "feat(eventstore): events table migrations for inventory and booking"
```

---

### Task 2: `Append` with optimistic concurrency

**Files:**
- Create: `internal/platform/eventstore/eventstore.go`, `internal/platform/eventstore/errors.go`
- Modify: `internal/platform/eventstore/eventstore_test.go` (add append tests)

**Interfaces:**
- Consumes: `pg.WithTx`, `pgtest`, `migrations.FS`.
- Produces:
  - `type Meta struct { TraceID, CorrelationID, CausationID string }`
  - `type Event struct { Type string; Data []byte; Meta Meta }` — `Data` is `protojson` bytes
  - `type Recorded struct { Event; StreamID string; Version int; GlobalSeq int64; RecordedAt time.Time }`
  - `var ErrVersionConflict = errors.New(...)`
  - `func Append(ctx context.Context, tx pgx.Tx, streamID string, expectedVersion int, evts []Event) error`

- [ ] **Step 1: Write the failing tests**

Append to `internal/platform/eventstore/eventstore_test.go`:

```go
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
```

Add these imports to the test file's import block: `encoding/json`, `errors`, `fmt`, `sync`, `github.com/jackc/pgx/v5`, `github.com/kptac/sagaflow/internal/platform/eventstore`.

An empty append must be a no-op rather than an error because a pure `Decide` returning no events is the normal, expected outcome for a duplicate or late message — spec §10.5 relies on it.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/platform/eventstore/ -run TestAppend -v`
Expected: FAIL to build — `undefined: eventstore.Append`, `undefined: eventstore.Event`, `undefined: eventstore.ErrVersionConflict`.

- [ ] **Step 3: Implement `errors.go`**

```go
package eventstore

import "errors"

// ErrVersionConflict means another writer appended to this stream after the
// caller folded its state. The caller reloads and retries; it is not a failure
// condition, it is how concurrency is resolved (spec §6.2).
var ErrVersionConflict = errors.New("eventstore: version conflict")
```

- [ ] **Step 4: Implement `eventstore.go`**

```go
// Package eventstore is the append-only event log every service owns a copy of.
//
// It knows nothing about event payloads beyond "a type name and some JSON".
// Encoding lives in platform/codec; folding events into state lives in each
// service. This package's whole job is the version invariant.
package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Meta travels with every event so a stored row can be traced back to the
// request that caused it (spec §11).
type Meta struct {
	TraceID       string `json:"trace_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
}

// Event is an event about to be appended. Data is protojson bytes: readable in
// psql and independent of the schema registry (spec §8.4).
type Event struct {
	Type string
	Data []byte
	Meta Meta
}

// Recorded is an event read back from the log.
type Recorded struct {
	Event
	StreamID   string
	Version    int
	GlobalSeq  int64
	RecordedAt time.Time
}

const appendSQL = `
INSERT INTO events (stream_id, version, type, data, meta)
SELECT $1, $2 + t.ord, t.type, t.data, t.meta
FROM unnest($3::text[], $4::jsonb[], $5::jsonb[])
     WITH ORDINALITY AS t(type, data, meta, ord)`

// Append writes evts to streamID at versions expectedVersion+1 … +n.
//
// expectedVersion is the version the caller's in-memory state was folded from.
// A unique violation on (stream_id, version) means someone else got there
// first, and is translated to ErrVersionConflict.
//
// One statement, one round trip, one error to interpret: WITH ORDINALITY
// numbers the unnested rows from 1, so version arithmetic happens in the
// database and there is no loop that could skip a number.
func Append(ctx context.Context, tx pgx.Tx, streamID string, expectedVersion int, evts []Event) error {
	if len(evts) == 0 {
		return nil
	}
	types := make([]string, len(evts))
	data := make([]string, len(evts))
	meta := make([]string, len(evts))
	for i, e := range evts {
		if e.Type == "" {
			return fmt.Errorf("eventstore: event %d has no type", i)
		}
		if len(e.Data) == 0 {
			return fmt.Errorf("eventstore: event %d (%s) has no data", i, e.Type)
		}
		m, err := json.Marshal(e.Meta)
		if err != nil {
			return fmt.Errorf("eventstore: marshal meta for %s: %w", e.Type, err)
		}
		types[i] = e.Type
		data[i] = string(e.Data)
		meta[i] = string(m)
	}

	_, err := tx.Exec(ctx, appendSQL, streamID, expectedVersion, types, data, meta)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrVersionConflict
		}
		return fmt.Errorf("eventstore: append to %s at %d: %w", streamID, expectedVersion, err)
	}
	return nil
}
```

- [ ] **Step 5: Run the tests**

Run: `go test ./internal/platform/eventstore/ -race -v`
Expected: all PASS, including `TestAppendConcurrentWritersExactlyOneWins`.

If that test reports more than one winner, the `UNIQUE` constraint is missing from the migration. If it reports zero winners, all eight transactions deadlocked — check that `WithTx` is not holding a lock it should not.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/eventstore
git commit -m "feat(eventstore): Append with optimistic concurrency via UNIQUE(stream_id, version)"
```

---

### Task 3: `Load`

**Files:**
- Modify: `internal/platform/eventstore/eventstore.go`
- Modify: `internal/platform/eventstore/eventstore_test.go`

**Interfaces:**
- Consumes: everything from Task 2.
- Produces: `func Load(ctx context.Context, tx pgx.Tx, streamID string) ([]Recorded, error)`. Returns events in ascending version order; an empty slice and no error for a stream that does not exist.

There is no separate `Replay`: replaying a stream is `Load`, and replaying a whole database is a projection reading from Kafka (spec §6.4), so a table-scanning replay API would be an attractive nuisance — the one thing spec §6.4 forbids.

- [ ] **Step 1: Write the failing tests**

Append to `internal/platform/eventstore/eventstore_test.go`:

```go
func TestLoadReturnsEventsInVersionOrder(t *testing.T) {
	ctx := context.Background()
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
	// {"k": "v"} and key order is not promised either. Spec §8.4 forbids
	// comparing protojson byte-wise for precisely this reason — it is why the
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
	ctx := context.Background()
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
	ctx := context.Background()
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
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/platform/eventstore/ -run TestLoad -v`
Expected: FAIL to build — `undefined: eventstore.Load`.

- [ ] **Step 3: Implement `Load`**

Append to `internal/platform/eventstore/eventstore.go`:

```go
const loadSQL = `
SELECT global_seq, version, type, data, meta, recorded_at
FROM events
WHERE stream_id = $1
ORDER BY version`

// Load reads a stream in version order.
//
// It reads inside the caller's transaction, so the version of the last event
// returned is a safe expectedVersion for a subsequent Append in that same
// transaction — nothing can interleave.
func Load(ctx context.Context, tx pgx.Tx, streamID string) ([]Recorded, error) {
	rows, err := tx.Query(ctx, loadSQL, streamID)
	if err != nil {
		return nil, fmt.Errorf("eventstore: load %s: %w", streamID, err)
	}
	defer rows.Close()

	var out []Recorded
	for rows.Next() {
		var (
			r        Recorded
			metaJSON []byte
		)
		if err := rows.Scan(&r.GlobalSeq, &r.Version, &r.Type, &r.Data, &metaJSON, &r.RecordedAt); err != nil {
			return nil, fmt.Errorf("eventstore: scan %s: %w", streamID, err)
		}
		if err := json.Unmarshal(metaJSON, &r.Meta); err != nil {
			return nil, fmt.Errorf("eventstore: unmarshal meta for %s v%d: %w", streamID, r.Version, err)
		}
		r.StreamID = streamID
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterate %s: %w", streamID, err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run the whole event store suite**

Run: `go test ./internal/platform/eventstore/ -race -v`
Expected: every test PASSes.

- [ ] **Step 5: Commit**

```bash
git add internal/platform/eventstore
git commit -m "feat(eventstore): Load a stream in version order"
```

---
