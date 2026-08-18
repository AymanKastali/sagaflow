# Phase 5c — Availability Projection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A queryable seat-availability view, derived entirely from the seat streams, that can be thrown away and rebuilt to the identical result — and that cannot cause an oversell no matter how stale it gets.

**Architecture:** A `seat_availability` table holding one row per seat that has a stream. The projector does not apply the event it was handed; it re-reads the seat's stream and writes the fold. Re-derivation is idempotent by construction, so the projection needs no inbox row and no ordering guarantee, and a version column stops a re-derivation that read an older stream from overwriting a newer row. Rebuilding is the same function run for every stream inside one transaction.

**Tech Stack:** Go 1.26.6, pgx v5, `internal/platform/{eventstore,pg}`, `internal/testsupport/pgtest`.

**Spec:** [2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) §6.3 (availability browsing is a projection and deliberately stale), §6.4 (why no consumer may track a cursor over `global_seq`), §7.1 (`projection.go` inside the service), §10.2 (the inbox key), §12.2 (fold, drop, rebuild: identical result), §12.6 (eventual consistency is proven by the rebuild and by stale-availability → 409).

## Global Constraints

- Go 1.26.6; module `github.com/AymanKastali/sagaflow`; contracts are the second module `github.com/AymanKastali/sagaflow/contracts`.
- A single transaction writes **exactly one stream**, plus its outbox rows, its inbox row, and any timer it scheduled (spec §7.2). The projector writes no stream at all: it only reads them.
- Projections take their live feed from Kafka, never from a cursor over `events` (spec §6.4).
- Every package under `internal/` gets a `doc.go` with the six chapter headings ([docs/conventions.md](../../conventions.md)); `README.md` links every package under `internal/platform/` (D5). `internal/docs` fails the suite otherwise.
- `§` appears in `doc.go` and nowhere else in Go source (D4).
- Chapters are 60–120 lines (C7). `internal/inventory/doc.go` is already at the top of that range, so Task 5 rewrites it rather than appending to it.
- Functions stay under about 40 lines; comments inside a function body are single lines (R4, R5).
- `make test` starts no container; one container per package, started in `TestMain` (spec §12.4).
- No `time.Sleep` in assertions (spec §12.4).
- Every task's last step is its commit.

## Scope

**In:** `eventstore.Streams`, `005_projections.sql`, `internal/inventory/projection.go`, its tests including the two properties below, and the documentation that follows from them.

**Out, and where each lands:**

| Deferred | Where |
|---|---|
| `consumers.go` — the Kafka handler that calls `Project` for each `inventory.events` record | Phase 5d |
| `wire.go`, `cmd/inventory/main.go` — inventory as something you can start | Phase 5d |
| `bookings_view` and the HTTP read endpoint | Phase 8 |
| A seat map (which seats exist at all) | Never, as things stand. See below. |

## The one invariant this phase introduces

**A derived answer may be old, but it may never be authoritative.**

The seat availability view exists to be read cheaply and often — a customer browsing a flight should not fold three hundred streams to see a seat map. It is therefore allowed to lag: the events it is built from arrive over Kafka, and Kafka delivers when it delivers.

What makes that safe is that no decision is ever taken from it. A hold is decided from the seat's own stream inside the transaction that appends to it, so the worst a stale view can do is offer a seat that has just gone — and the answer to that is the refusal phase 5a already produces. Staleness costs a customer one click. It cannot cost the airline a seat.

Two consequences shape the code:

- **The projector re-derives rather than applies.** Handed "seat 14A changed", it re-reads 14A's stream and writes the fold, instead of applying the event's payload to the row. Doing the same work twice produces the same row, so a redelivered message needs no inbox row to absorb it, and two messages arriving out of order need no sequence number to sort them. This is phase 5b's corollary again, one layer out: *the message is a hint about when to look, never the source of truth about what is true.*
- **The view only knows seats that have events.** A seat nobody has ever held has no stream, so it has no row. The view therefore answers "which seats are taken", and "which seats exist" is reference data this system does not have. Inventing a seat map to make the view look complete would be inventing a source of truth that no event backs.

---

### Task 1: Amend the spec

The two clauses below are where this phase departs from what the spec currently says, so they are settled in the authority before any code argues from them.

**Files:**
- Modify: `docs/superpowers/specs/2026-08-17-sagaflow-design.md` (§6.4, §10.2)

- [ ] **Step 1: Amend §6.4 — a rebuild is not a cursor**

Find this bullet under "The design removes the need for a fix:"

```markdown
- **Projections consume from Kafka** using committed consumer-group offsets, never by scanning `events`.
```

Append a paragraph immediately after the bullet list that ends with it (before the line beginning "No component reads events by a monotonic local cursor"):

```markdown
That prohibition is about the live feed, not about a rebuild. Rebuilding a projection from the event store
is safe as long as it enumerates *streams* and folds each one whole: it reads every version of a stream or
it fails, so there is no cursor for a late-committing row to slip behind. What is forbidden is the
incremental form — remembering a high-water `global_seq` and asking for everything above it.
```

- [ ] **Step 2: Amend §10.2 — an inbox row is for effects that cannot be repeated**

Find this paragraph:

```markdown
`consumer` is in the key because several consumers in one service read the same message — the saga and
the projection both see `SeatHeld` and must deduplicate independently. Rows are pruned past Kafka's
retention window.
```

Insert a paragraph immediately after it:

```markdown
A projection is not automatically one of those consumers. `inventory`'s availability view re-derives a
seat from its stream rather than applying the event it was handed, so a redelivery recomputes the same
row and writing it twice is indistinguishable from writing it once. It takes no inbox row, because there
is nothing an inbox row would prevent. The saga is the opposite case and keeps its own: applying
`SeatHeld` twice would advance a state machine twice. The rule is therefore about the handler's effect
rather than about the number of consumers — deduplicate what cannot be repeated, and let what can be
repeated be repeated.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-08-17-sagaflow-design.md
git commit -m "docs(spec): a rebuild is not a cursor, and an inbox row is for effects that cannot repeat"
```

---

### Task 2: `eventstore.Streams`

A rebuild has to know which streams exist. This is the one thing the event store cannot already answer.

**Files:**
- Modify: `internal/platform/eventstore/eventstore.go`
- Modify: `internal/platform/eventstore/doc.go` (the reading-order line for `eventstore.go`)
- Test: `internal/platform/eventstore/eventstore_test.go`

**Interfaces:**
- Produces: `func Streams(ctx context.Context, tx pgx.Tx) ([]string, error)` — every distinct `stream_id` in the database, in ascending order.

- [ ] **Step 1: Write the failing test**

Append to `internal/platform/eventstore/eventstore_test.go`:

```go
func TestStreamsNamesEveryStreamOnce(t *testing.T) {
	// A rebuild folds one stream at a time, so what it needs from the log is the
	// list of streams — not the events, and above all not a cursor over them.
	ctx := t.Context()
	pool := db(t, "eventstore_streams")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		for _, stream := range []string{"seat-b", "seat-a", "seat-b"} {
			existing, err := eventstore.Load(ctx, tx, stream)
			if err != nil {
				return err
			}
			if err := eventstore.Append(ctx, tx, stream, len(existing), []eventstore.Event{
				{Type: "T", Data: []byte(`{}`)},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got []string
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		got, err = eventstore.Streams(ctx, tx)
		return err
	}); err != nil {
		t.Fatalf("streams: %v", err)
	}

	want := []string{"seat-a", "seat-b"}
	if !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v — seat-b has two events and must still be named once", want, got)
	}
}
```

Add `"slices"` to the test file's imports if it is not already there. Reuse the file's existing `db` helper; if the file names it differently, use whatever it already uses rather than adding a second one.

- [ ] **Step 2: Run it and watch it fail**

```bash
go test ./internal/platform/eventstore/ -run TestStreamsNamesEveryStreamOnce
```

Expected: a compile failure — `undefined: eventstore.Streams`.

- [ ] **Step 3: Implement `Streams`**

Add to `internal/platform/eventstore/eventstore.go`, after `Load`:

```go
const streamsSQL = `SELECT DISTINCT stream_id FROM events ORDER BY stream_id`

// Streams names every stream in the database, so a projection can be rebuilt by
// folding each one from scratch.
//
// Enumerating streams is what makes a rebuild safe, and the alternative is the
// trap this table is shaped to avoid. Row ids are handed out at insert but become
// visible at commit, so a rebuild that remembered "I have read up to id 42" would
// step straight over a row that took id 41 and committed later — losing an event
// silently, only under load, only in production. Folding a whole stream reads
// every version of it or fails; there is no cursor for anything to slip behind.
//
// It reads inside the caller's transaction, like Load, so a rebuild sees one
// consistent snapshot of the log: the streams it enumerates and the events it
// then folds are the same log, not two reads of a moving one.
func Streams(ctx context.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, streamsSQL)
	if err != nil {
		return nil, fmt.Errorf("eventstore: list streams: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("eventstore: scan stream id: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterate stream ids: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run it and watch it pass**

```bash
go test ./internal/platform/eventstore/ -run TestStreamsNamesEveryStreamOnce
```

Expected: PASS.

- [ ] **Step 5: Update the chapter's reading order**

In `internal/platform/eventstore/doc.go`, change:

```go
//	eventstore.go  Append and Load. Start with Append's SQL.
```

to:

```go
//	eventstore.go  Append, Load and Streams. Start with Append's SQL.
```

- [ ] **Step 6: Run the package's suite and the docs enforcement**

```bash
go test ./internal/platform/eventstore/ ./internal/docs/
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/platform/eventstore/
git commit -m "feat(eventstore): name every stream, so a projection can be rebuilt without a cursor"
```

---

### Task 3: The view and the projector

**Files:**
- Create: `internal/inventory/migrations/005_projections.sql`
- Create: `internal/inventory/projection.go`
- Test: `internal/inventory/projection_test.go`

**Interfaces:**
- Consumes: `inventory.LoadSeat`, `inventory.Status`, `eventstore.Streams`, `pg.WithTx`.
- Produces:
  - `type Availability struct { SeatID string; Status Status; HoldID, BookingID string; ExpiresAt time.Time; Version int }`
  - `func NewProjector(pool *pgxpool.Pool) *Projector`
  - `func (p *Projector) Project(ctx context.Context, seatID string) error`
  - `func (p *Projector) Rebuild(ctx context.Context) error`
  - `func LoadAvailability(ctx context.Context, pool *pgxpool.Pool, seatID string) (Availability, bool, error)`
  - `func HeldSeats(ctx context.Context, pool *pgxpool.Pool, flightPrefix string) ([]string, error)`

- [ ] **Step 1: Write the migration**

Create `internal/inventory/migrations/005_projections.sql`:

```sql
CREATE TABLE seat_availability (
    seat_id    TEXT        PRIMARY KEY,
    status     TEXT        NOT NULL,
    hold_id    TEXT        NOT NULL,
    booking_id TEXT        NOT NULL,
    expires_at TIMESTAMPTZ,
    version    INT         NOT NULL
);

CREATE INDEX seat_availability_browse ON seat_availability (status, seat_id);

COMMENT ON TABLE seat_availability IS
    'Derived from the seat streams and safe to drop: every row can be folded again from events. Deliberately stale. A hold decided from this table instead of from the seat stream would be an oversell.';

COMMENT ON COLUMN seat_availability.status IS
    'Stored as text rather than an integer so the table reads in psql and so renumbering a Go constant cannot silently rewrite what a row says.';

COMMENT ON COLUMN seat_availability.version IS
    'The stream version this row was folded to. Not a lock: it is what stops a re-derivation that read an older stream from overwriting a newer row.';

---- create above / drop below ----

DROP TABLE seat_availability;
```

- [ ] **Step 2: Write the failing tests**

Create `internal/inventory/projection_test.go`:

```go
package inventory_test

import (
	"context"
	"testing"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
	"google.golang.org/protobuf/proto"
)

// The package-wide seat is on BA117; this one is on another flight, so a browse
// query has something it must not return.
const otherFlightSeat = "seat-BA999-2026-09-01-01A"

func holdSeatOn(seatID, holdID string) *inventoryv1.HoldSeat {
	return &inventoryv1.HoldSeat{
		HoldId: holdID, BookingId: booking, SeatId: seatID, ExpiresAt: expires,
	}
}

func releaseSeatOn(seatID, holdID string) *inventoryv1.ReleaseSeatHold {
	return &inventoryv1.ReleaseSeatHold{
		HoldId: holdID, BookingId: booking, SeatId: seatID, Reason: "compensating",
	}
}

// apply drives one command through the real handler, so the events a projection
// test folds are the events the service actually writes.
func apply(t *testing.T, ctx context.Context, h *inventory.Handler, seatID string, cmd proto.Message) {
	t.Helper()
	env := command(cmd)
	env.Subject = seatID
	if err := h.Handle(ctx, env, cmd); err != nil {
		t.Fatalf("handle %T on %s: %v", cmd, seatID, err)
	}
}

func TestAHeldSeatShowsAsHeldInTheView(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_projection_held")
	h := inventory.NewHandler(pool, jsonEncoder{})
	p := inventory.NewProjector(pool)

	apply(t, ctx, h, seat, holdSeatOn(seat, hold))
	if err := p.Project(ctx, seat); err != nil {
		t.Fatalf("project: %v", err)
	}

	got, ok, err := inventory.LoadAvailability(ctx, pool, seat)
	if err != nil {
		t.Fatalf("load availability: %v", err)
	}
	if !ok {
		t.Fatal("the seat has a stream, so the view must have a row for it")
	}
	if got.Status != inventory.StatusHeld {
		t.Fatalf("want held, got %v", got.Status)
	}
	if got.HoldID != hold || got.BookingID != booking {
		t.Fatalf("view lost the hold's identity: %+v", got)
	}
	if got.Version != 1 {
		t.Fatalf("one event in the stream, so the row is folded to version 1, got %d", got.Version)
	}
	if !got.ExpiresAt.Equal(expires.AsTime()) {
		t.Fatalf("want deadline %v, got %v", expires.AsTime(), got.ExpiresAt)
	}
}

func TestProjectingTheSameSeatTwiceChangesNothing(t *testing.T) {
	// Re-deriving is the reason this projection needs no inbox row: a redelivered
	// message makes it do the same work again, and the same work again is a no-op.
	ctx := t.Context()
	pool := db(t, "inventory_projection_twice")
	h := inventory.NewHandler(pool, jsonEncoder{})
	p := inventory.NewProjector(pool)

	apply(t, ctx, h, seat, holdSeatOn(seat, hold))
	for range 2 {
		if err := p.Project(ctx, seat); err != nil {
			t.Fatalf("project: %v", err)
		}
	}

	got, _, err := inventory.LoadAvailability(ctx, pool, seat)
	if err != nil {
		t.Fatalf("load availability: %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("projecting twice must not advance anything, got version %d", got.Version)
	}
}

func TestAReleasedSeatGoesBackToFree(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_projection_released")
	h := inventory.NewHandler(pool, jsonEncoder{})
	p := inventory.NewProjector(pool)

	apply(t, ctx, h, seat, holdSeatOn(seat, hold))
	apply(t, ctx, h, seat, releaseSeatOn(seat, hold))
	if err := p.Project(ctx, seat); err != nil {
		t.Fatalf("project: %v", err)
	}

	got, _, err := inventory.LoadAvailability(ctx, pool, seat)
	if err != nil {
		t.Fatalf("load availability: %v", err)
	}
	if got.Status != inventory.StatusFree {
		t.Fatalf("want free, got %v", got.Status)
	}
	if got.HoldID != "" || got.BookingID != "" || !got.ExpiresAt.IsZero() {
		t.Fatalf("a free seat carries no hold, no booking and no deadline: %+v", got)
	}
}

func TestBrowsingOneFlightDoesNotReturnAnother(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_projection_browse")
	h := inventory.NewHandler(pool, jsonEncoder{})
	p := inventory.NewProjector(pool)

	apply(t, ctx, h, seat, holdSeatOn(seat, hold))
	apply(t, ctx, h, otherFlightSeat, holdSeatOn(otherFlightSeat, "hold-2"))
	for _, id := range []string{seat, otherFlightSeat} {
		if err := p.Project(ctx, id); err != nil {
			t.Fatalf("project %s: %v", id, err)
		}
	}

	held, err := inventory.HeldSeats(ctx, pool, "seat-BA117-")
	if err != nil {
		t.Fatalf("held seats: %v", err)
	}
	if len(held) != 1 || held[0] != seat {
		t.Fatalf("want only %s for this flight, got %v", seat, held)
	}
}

func TestASeatWithNoStreamHasNoRow(t *testing.T) {
	// The view is derived from events, so it knows the seats that have happened,
	// not the seats that exist. Which seats exist is reference data nothing here
	// owns, and inventing it would mean inventing a truth no event backs.
	ctx := t.Context()
	pool := db(t, "inventory_projection_absent")
	p := inventory.NewProjector(pool)

	if err := p.Project(ctx, seat); err != nil {
		t.Fatalf("project: %v", err)
	}
	if _, ok, err := inventory.LoadAvailability(ctx, pool, seat); err != nil || ok {
		t.Fatalf("want no row and no error, got ok=%v err=%v", ok, err)
	}
}
```

- [ ] **Step 3: Run them and watch them fail**

```bash
go test ./internal/inventory/ -run 'TestAHeldSeat|TestProjectingTheSame|TestAReleasedSeat|TestBrowsingOneFlight|TestASeatWithNoStream'
```

Expected: a compile failure — `undefined: inventory.NewProjector`.

- [ ] **Step 4: Write `projection.go`**

Create `internal/inventory/projection.go`:

```go
package inventory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Availability is one seat as the browsing view knows it: the folded seat, plus
// the stream version it was folded from.
//
// It is a copy of SeatState with the seat's own id attached, rather than
// SeatState itself, because the two answer different questions. SeatState is what
// a decision is taken from and is always current; this is what a customer is
// shown and is allowed to be old.
type Availability struct {
	SeatID    string
	Status    Status
	HoldID    string
	BookingID string
	ExpiresAt time.Time
	Version   int
}

// Projector keeps seat_availability in step with the seat streams.
//
// It re-derives rather than applies: handed "this seat changed", it re-reads that
// seat's stream and writes the fold, instead of applying the event's payload to
// the row it already has. Doing that twice produces the same row, which is what
// lets this projection take no inbox row — there is no duplicate for one to
// absorb — and take no ordering guarantee either, because two notifications
// arriving in the wrong order both end at the current state.
//
// The price is that it only works where the projection and the stream share a
// database. A view living somewhere else could not re-read anything and would
// have to apply what the message carried, sequence numbers and all.
type Projector struct {
	pool *pgxpool.Pool
}

func NewProjector(pool *pgxpool.Pool) *Projector { return &Projector{pool: pool} }

// Project brings one seat's row up to date with its stream.
func (p *Projector) Project(ctx context.Context, seatID string) error {
	return pg.WithTx(ctx, p.pool, func(tx pgx.Tx) error {
		return project(ctx, tx, seatID)
	})
}

// Rebuild throws the whole view away and folds it again from the streams.
//
// All of it in one transaction, so a reader either sees the old view or the new
// one and never an empty table. That also makes the rebuild deterministic: the
// streams it enumerates and the events it folds come from a single snapshot of
// the log rather than from a log still being written to.
func (p *Projector) Rebuild(ctx context.Context) error {
	return pg.WithTx(ctx, p.pool, func(tx pgx.Tx) error {
		ids, err := eventstore.Streams(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM seat_availability`); err != nil {
			return fmt.Errorf("inventory: clear seat_availability: %w", err)
		}
		for _, id := range ids {
			if err := project(ctx, tx, id); err != nil {
				return err
			}
		}
		return nil
	})
}

// upsertAvailabilitySQL writes a folded seat, unless the row is already at or
// past the version that fold came from.
//
// The guard is what makes a re-derivation safe to run at any moment from any
// caller. Two of them can overlap — a rebuild and a live notification, two
// notifications for the same seat — and the older one simply does nothing rather
// than dragging the row backwards to a state that is no longer true.
const upsertAvailabilitySQL = `
INSERT INTO seat_availability (seat_id, status, hold_id, booking_id, expires_at, version)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (seat_id) DO UPDATE SET
    status     = excluded.status,
    hold_id    = excluded.hold_id,
    booking_id = excluded.booking_id,
    expires_at = excluded.expires_at,
    version    = excluded.version
WHERE seat_availability.version < excluded.version`

// project folds one seat inside the caller's transaction and writes the result.
func project(ctx context.Context, tx pgx.Tx, seatID string) error {
	state, _, err := LoadSeat(ctx, tx, seatID)
	if err != nil {
		return err
	}
	if state.Version == 0 {
		return nil // no stream: nothing has happened to this seat, so there is nothing to derive
	}
	if _, err := tx.Exec(ctx, upsertAvailabilitySQL, seatID, state.Status.String(),
		state.HoldID, state.BookingID, deadline(state.ExpiresAt), state.Version); err != nil {
		return fmt.Errorf("inventory: project %s: %w", seatID, err)
	}
	return nil
}

// deadline maps the zero time to NULL. A free seat has no deadline, and writing
// year one into the column would make it look like one that passed long ago.
func deadline(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

const availabilitySQL = `
SELECT seat_id, status, hold_id, booking_id, expires_at, version
FROM seat_availability
WHERE seat_id = $1`

// LoadAvailability reads one seat from the view, reporting whether it had a row.
//
// It takes a pool rather than a transaction, and that is the point of the whole
// table: this read joins nothing, folds nothing and locks nothing, so it can be
// served while the seat it describes is being written to. What it returns may
// already be out of date by the time it is rendered, and nothing may decide
// anything from it — a hold is decided from the stream, in the transaction that
// appends to it.
func LoadAvailability(ctx context.Context, pool *pgxpool.Pool, seatID string) (Availability, bool, error) {
	var (
		a      Availability
		status string
		until  *time.Time
	)
	err := pool.QueryRow(ctx, availabilitySQL, seatID).Scan(
		&a.SeatID, &status, &a.HoldID, &a.BookingID, &until, &a.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return Availability{}, false, nil
	}
	if err != nil {
		return Availability{}, false, fmt.Errorf("inventory: read availability for %s: %w", seatID, err)
	}
	if a.Status, err = parseStatus(status); err != nil {
		return Availability{}, false, err
	}
	if until != nil {
		a.ExpiresAt = *until
	}
	return a, true, nil
}

const heldSeatsSQL = `
SELECT seat_id
FROM seat_availability
WHERE status = $1 AND seat_id LIKE $2 || '%'
ORDER BY seat_id`

// HeldSeats lists the seats on one flight that the view believes are taken.
//
// Taken rather than free, because a seat nobody has ever held has no stream and
// therefore no row here. The view answers what has happened; which seats exist is
// reference data no event in this system carries.
//
// flightPrefix is matched against the start of the stream id — seat ids begin
// with the flight and date they belong to, so the prefix is the flight.
func HeldSeats(ctx context.Context, pool *pgxpool.Pool, flightPrefix string) ([]string, error) {
	rows, err := pool.Query(ctx, heldSeatsSQL, StatusHeld.String(), flightPrefix)
	if err != nil {
		return nil, fmt.Errorf("inventory: browse %s: %w", flightPrefix, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("inventory: scan browsed seat: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inventory: iterate browsed seats: %w", err)
	}
	return out, nil
}

// parseStatus turns the stored text back into a Status, refusing anything it does
// not recognise rather than defaulting.
//
// Defaulting an unknown status to free would turn a schema mistake into an
// oversell-shaped answer, which is the one direction this table must never fail
// in.
func parseStatus(s string) (Status, error) {
	switch s {
	case StatusFree.String():
		return StatusFree, nil
	case StatusHeld.String():
		return StatusHeld, nil
	}
	return 0, fmt.Errorf("inventory: unknown status %q in seat_availability", s)
}
```

- [ ] **Step 5: Run the tests and watch them pass**

```bash
go test ./internal/inventory/ -run 'TestAHeldSeat|TestProjectingTheSame|TestAReleasedSeat|TestBrowsingOneFlight|TestASeatWithNoStream'
```

Expected: PASS.

- [ ] **Step 6: Run the whole package**

```bash
go test ./internal/inventory/
```

Expected: PASS. The new migration runs for every test in the package, so a mistake in `005_projections.sql` surfaces here rather than in one test.

- [ ] **Step 7: Commit**

```bash
git add internal/inventory/migrations/005_projections.sql internal/inventory/projection.go internal/inventory/projection_test.go
git commit -m "feat(inventory): a seat availability view, re-derived rather than applied"
```

---

### Task 4: The two properties

One property is that the view is genuinely derived — drop it, build it again, get the same thing. The other is that being wrong about the world cannot hurt: a stale view offers a seat that has gone, and the hold still refuses.

**Files:**
- Modify: `internal/inventory/projection_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/inventory/projection_test.go`:

```go
// snapshot reads the whole view as text, so two builds of it can be compared
// without asserting field by field what "the same" means.
func snapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT seat_id || ' ' || status || ' ' || hold_id || ' ' || booking_id || ' ' ||
		        coalesce(expires_at::text, '-') || ' v' || version
		 FROM seat_availability ORDER BY seat_id`)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan snapshot row: %v", err)
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate snapshot: %v", err)
	}
	return out
}

func TestDroppingTheViewAndBuildingItAgainGivesTheSameRows(t *testing.T) {
	// This is what "derived" means, stated as a test: nothing in the view is
	// remembered anywhere but the streams, so throwing it away costs nothing.
	ctx := t.Context()
	pool := db(t, "inventory_projection_rebuild")
	h := inventory.NewHandler(pool, jsonEncoder{})
	p := inventory.NewProjector(pool)

	apply(t, ctx, h, seat, holdSeatOn(seat, hold))
	apply(t, ctx, h, seat, releaseSeatOn(seat, hold))
	apply(t, ctx, h, seat, holdSeatOn(seat, "hold-3"))
	apply(t, ctx, h, otherFlightSeat, holdSeatOn(otherFlightSeat, "hold-2"))
	for _, id := range []string{seat, otherFlightSeat} {
		if err := p.Project(ctx, id); err != nil {
			t.Fatalf("project %s: %v", id, err)
		}
	}
	before := snapshot(t, ctx, pool)
	if len(before) != 2 {
		t.Fatalf("two seats have streams, so the view has two rows, got %d: %v", len(before), before)
	}

	if err := p.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	after := snapshot(t, ctx, pool)
	if !slices.Equal(before, after) {
		t.Fatalf("rebuild changed the view.\nbefore: %v\nafter:  %v", before, after)
	}
}

func TestAStaleViewStillCannotOversellASeat(t *testing.T) {
	// The whole reason the view is allowed to lag. It says free, the stream says
	// held, and the hold is decided from the stream — so the second customer is
	// refused rather than sold a seat that is gone.
	ctx := t.Context()
	pool := db(t, "inventory_projection_stale")
	h := inventory.NewHandler(pool, jsonEncoder{})

	apply(t, ctx, h, seat, holdSeatOn(seat, hold))
	// Deliberately no Project: this is the window between the hold committing and
	// the notification arriving, which is exactly where a real browse lands.
	if _, ok, err := inventory.LoadAvailability(ctx, pool, seat); err != nil || ok {
		t.Fatalf("the view has not caught up yet, so it must have no row: ok=%v err=%v", ok, err)
	}

	second := holdSeatOn(seat, "hold-late")
	apply(t, ctx, h, seat, second)

	rows := outboxRows(t, ctx, pool)
	last := rows[len(rows)-1]
	const unavailable = "sagaflow.inventory.v1.SeatUnavailable"
	if last.ceTyp != unavailable {
		t.Fatalf("the second hold read a stale view but must still be refused: want %s, got %s",
			unavailable, last.ceTyp)
	}
}

func TestAnOlderDerivationDoesNotDragTheRowBackwards(t *testing.T) {
	// Two re-derivations can overlap — a rebuild and a live notification, or two
	// notifications for one seat. The one that read the older stream must lose.
	ctx := t.Context()
	pool := db(t, "inventory_projection_backwards")
	h := inventory.NewHandler(pool, jsonEncoder{})
	p := inventory.NewProjector(pool)

	apply(t, ctx, h, seat, holdSeatOn(seat, hold))

	// A row already ahead of the stream, as a slow re-derivation would find on
	// waking up to discover someone else got there first.
	if _, err := pool.Exec(ctx,
		`INSERT INTO seat_availability (seat_id, status, hold_id, booking_id, expires_at, version)
		 VALUES ($1, 'free', '', '', NULL, 9)`, seat); err != nil {
		t.Fatalf("seed newer row: %v", err)
	}

	if err := p.Project(ctx, seat); err != nil {
		t.Fatalf("project: %v", err)
	}

	got, _, err := inventory.LoadAvailability(ctx, pool, seat)
	if err != nil {
		t.Fatalf("load availability: %v", err)
	}
	if got.Version != 9 || got.Status != inventory.StatusFree {
		t.Fatalf("the newer row must survive an older fold, got %+v", got)
	}
}
```

Add `"slices"` and `"github.com/jackc/pgx/v5/pgxpool"` to the file's imports.

- [ ] **Step 2: Run them and watch them fail**

```bash
go test ./internal/inventory/ -run 'TestDroppingTheView|TestAStaleView|TestAnOlderDerivation'
```

Expected: a compile failure on the first run (missing imports), then genuine results. If `TestDroppingTheView` or `TestAnOlderDerivation` fails on behaviour, the version guard in `upsertAvailabilitySQL` is wrong — fix the SQL, not the test.

- [ ] **Step 3: Make them pass**

No new production code should be needed: Task 3 already wrote the guard and the rebuild. If a test fails, the defect is in Task 3's code and belongs there.

- [ ] **Step 4: Run the whole package**

```bash
go test ./internal/inventory/
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/inventory/projection_test.go
git commit -m "test(inventory): the view rebuilds identically, and a stale one still refuses"
```

---

### Task 5: The chapters

**Files:**
- Modify: `internal/inventory/doc.go`
- Modify: `README.md`
- Modify: `docs/glossary.md`
- Modify: `docs/architecture.md`

- [ ] **Step 1: Rewrite `internal/inventory/doc.go`**

The current chapter is at the top of the 60–120 line budget, so the projection goes in by replacing prose rather than by adding to it. Write the file as:

```go
// Package inventory decides who gets a seat when two requests for it arrive
// at the same instant, by treating each seat as an append-only stream rather
// than a row that can be overwritten.
//
// # The problem
//
// Two customers browsing the same flight both tap "hold" on seat 14A close
// enough together that neither request can see the other's effect before
// committing to its own. Exactly one of them must get the seat; the other
// must be told no, not eventually and not after a support ticket, but as the
// direct answer to the request it just made. And no number of retries by the
// loser may ever produce a second hold on 14A — a retried decision that has
// been overtaken by events has to come back a refusal, not a duplicate.
//
// # Why the obvious fixes do not work
//
// A seats table with a held_by column, read then write: read whether the
// column is set, and if not, write your own id into it. Between the read and
// the write is a window with no lock in it, and two requests that both read
// "empty" both proceed to write — the second write simply overwrites the
// first, with no error and no trace that a collision happened.
//
// Wrap that same read-then-write in SELECT … FOR UPDATE: the window closes,
// but the lock is held for as long as the transaction takes, not as long as
// the write takes. Every other request touching that row — including a
// second customer who only wants to check whether 14A is still free — queues
// behind whatever the lock holder's application code is doing, for exactly
// as long as that code takes to decide.
//
// An application-level mutex around the hold logic: this works, right up
// until there are two instances of the service, which is the normal
// deployment shape for anything that needs to survive one instance dying.
// Two processes, two independent mutexes, guarding nothing against each
// other.
//
// # What this package does
//
// A seat is a stream, so the conflict is caught by UNIQUE(stream_id,
// version) at the instant of the append — no lock, no window, because
// nothing is checked and then acted on separately. Two holds for 14A both
// fold from version 0 and both attempt to append version 1; Postgres accepts
// one and rejects the other with ErrVersionConflict. The loser does not retry
// its old decision — it reloads the stream, sees the SeatHeld that beat it,
// and re-decides. Re-deciding is what turns the loss into a refusal: replaying
// the original decision would append a second hold onto a seat that is no
// longer free.
//
// Every command gets a reply; only a change to the seat gets an event.
// SeatUnavailable answers a losing racer but is never appended, because
// nothing happened to the seat. Nor may a decision answer with silence: a saga
// step that hears nothing back re-dispatches the same command forever, so even
// a no-op release still produces a reply.
//
// The decision functions have no clock. A hold is live until an event ends
// it — a release, an expiry — never until a clock says the TTL has passed.
// That absence is what stops a new hold racing an expiry: if "held" meant
// "held, unless now() has passed expires_at", two goroutines evaluating now()
// a millisecond apart could disagree about whether the seat was free. Expiry
// is itself an event, SeatHoldExpired, appended by a timer this package owns.
//
// A hold therefore ends in one of three ways, and all three are events on the
// seat's own stream: the saga releases it, the saga confirms it, or its
// deadline passes and inventory expires it. The third exists because the first
// two need the booking service alive, and 14A has to come back even when it
// is not. Expiry is also the one decision here that may answer with nothing
// at all — a deadline that finds its hold already gone has nobody to answer.
//
// Alongside the streams is one derived table, seat_availability, so that
// showing a customer a seat map does not mean folding three hundred streams.
// It is allowed to be out of date, because nothing decides anything from it:
// the worst a stale row can do is offer a seat that has just gone, and the
// answer to that is the refusal above. It is kept up to date by re-reading the
// seat whose stream changed rather than by applying what the message said, so
// the same notification twice, or two of them out of order, both land on the
// current state.
//
// # What it deliberately does not do
//
// It does not treat a passed deadline as making a hold false. The hold is
// still held until SeatHoldExpired is appended, which is why a new hold can
// never race an expiry and why nothing here needs a now().
//
// It does not cancel a timer when a hold is released early. The release and
// the deadline can arrive in either order, so a late fire has to be harmless
// regardless — and once it is harmless, cancelling it is dead code that can
// itself fail.
//
// It does not know which seats exist. A seat nobody has ever held has no
// stream and so no row in the view: what is derived from events can only
// describe what has happened. A seat map is reference data, and no event in
// this system carries it.
//
// This package does not know Kafka exists. Its dependency list ends at the
// outbox row it writes in the same transaction as the seat's events —
// confirmed by `go list -deps ./internal/inventory`. Turning that row into a
// Kafka message is the outbox poller's job; the boundary is what lets this
// package be tested against nothing but Postgres.
//
// # Reading order
//
//	seat.go        SeatState, Fold, Decide, Hold, Release, Expire — the pure
//	               decision functions. No context, no database, no clock.
//	store.go       LoadSeat and AppendSeat — the same fold and append, now
//	               wrapped around a real transaction.
//	errors.go      ErrUnknownEvent and ErrUnknownCommand.
//	commands.go    Handler.Handle — the one-stream-per-transaction glue that
//	               calls both. It only makes sense once seat.go's decisions and
//	               store.go's transaction boundary are already understood.
//	expiry.go      Expirer.Fire — the same shape as commands.go with one thing
//	               missing. Read them side by side: the absent inbox row is the
//	               whole lesson.
//	projection.go  Projector and the read side. Last, because it is the only
//	               file here that is allowed to be wrong for a moment.
//
// # Where this comes from
//
// Design spec §6.3 (a seat is one stream, why that is safe, and why browsing
// availability is a deliberately stale projection), §6.4 (why a rebuild
// enumerates streams instead of scanning by a cursor), §7.2 (one transaction
// writes exactly one stream, plus its outbox rows, its inbox row, and any
// deadline it scheduled), §9.3 (a compensation such as ReleaseSeatHold must
// never dead-letter, so it always gets a reply), §10.2 (an inbox row is for an
// effect that cannot be repeated, which is why the projection has none),
// §10.3 (three immediate reload-retries before a conflict is given up on),
// §10.5 (why the seat's own timer owns expiry), §12.2 (fold a projection, drop
// it, rebuild it: identical result).
package inventory
```

- [ ] **Step 2: Update `README.md`**

In the Status table, replace the `5c` row with these two:

```markdown
| 5c | Availability projection — a derived seat map that may lag and still cannot oversell | `internal/inventory/projection.go` | **built** |
| 5d | `wire.go` and `cmd/inventory` — inventory as something you can start | — | not built |
```

In the Reading order, append after item 10:

```markdown
11. **`internal/inventory/projection.go`** — the read side: the same events
    folded a second time into a table you can query, why it is allowed to be
    stale, and why being stale cannot cost anyone a seat.
```

- [ ] **Step 3: Update `docs/glossary.md`**

In the **Projection** entry, replace:

```markdown
`bookings_view` and the seat availability view are projections.
**(not built yet — phases 5b and 8)**
```

with:

```markdown
`seat_availability` is a projection: a seat map derived from the seat streams,
rebuilt by folding them again. `bookings_view` will be another.
**(`bookings_view` not built yet — phase 8)**
```

- [ ] **Step 4: Add a section to `docs/architecture.md`**

Insert a new section immediately before `## The saga`:

```markdown
## The answer that is allowed to be wrong

Everything above is about being exactly right: one hold per seat, one apply per
message, no state change without the message that announces it. The read side is
the opposite. A customer opening a seat map wants three hundred seats at once,
and folding three hundred streams to draw one picture would make browsing the
most expensive thing the system does.

So there is a derived table, `seat_availability`, holding one row per seat that
has a stream. It is written by consuming `inventory.events`, and it lags by
however long that takes.

```mermaid
sequenceDiagram
    participant C as customer
    participant V as seat_availability
    participant S as seat stream
    C->>V: which seats are taken?
    V-->>C: 14A is free
    Note over S: 14A was held 40 ms ago;<br/>the view has not heard yet
    C->>S: hold 14A
    S-->>C: SeatUnavailable
```

The stale answer costs one click. It cannot cost a seat, because no decision is
ever taken from the view: a hold is decided from the seat's own stream, inside
the transaction that appends to it, where `UNIQUE(stream_id, version)` is
waiting. Cheap stale reads with strict writes is the standard split, and this is
what makes it safe here.

The projector does not apply the event it was handed. It re-reads the seat whose
stream changed and writes the fold — so the same notification twice produces the
same row, and two notifications in the wrong order both end at the current state.
That is why this consumer keeps no inbox row: there is no duplicate for one to
absorb. The same function run for every stream is a full rebuild, which is how
the view can be dropped whenever its shape needs to change.
```

- [ ] **Step 5: Run the enforcement suite and the linters**

```bash
make lint && go test ./internal/docs/ ./internal/inventory/
```

Expected: PASS. If `internal/docs` reports a missing chapter heading, the rewrite in Step 1 dropped one.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory/doc.go README.md docs/glossary.md docs/architecture.md
git commit -m "docs(inventory): the read side, and why it is allowed to be wrong"
```

---

## Done when

- `make lint` and `make test` are green.
- `go test ./internal/inventory/ ./internal/platform/eventstore/` is green, including the rebuild and stale-view properties.
- `go doc ./internal/inventory` renders the six headings with the projection described under each of them that it belongs to.
- The Status table names 5c built and 5d not built.
