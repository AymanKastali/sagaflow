package inventory_test

import (
	"context"
	"slices"
	"testing"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
	"github.com/jackc/pgx/v5/pgxpool"
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

	if after := snapshot(t, ctx, pool); !slices.Equal(before, after) {
		t.Fatalf("rebuild changed the view.\nbefore: %v\nafter:  %v", before, after)
	}
}

func TestAStaleViewStillCannotOversellASeat(t *testing.T) {
	// The whole reason the view is allowed to lag. It says nothing is held, the
	// stream says 14A is, and the hold is decided from the stream — so the second
	// customer is refused rather than sold a seat that is already gone.
	ctx := t.Context()
	pool := db(t, "inventory_projection_stale")
	h := inventory.NewHandler(pool, jsonEncoder{})

	apply(t, ctx, h, seat, holdSeatOn(seat, hold))
	// Deliberately no Project: this is the window between the hold committing and
	// the notification arriving, which is exactly where a real browse lands.
	if _, ok, err := inventory.LoadAvailability(ctx, pool, seat); err != nil || ok {
		t.Fatalf("the view has not caught up yet, so it must have no row: ok=%v err=%v", ok, err)
	}

	apply(t, ctx, h, seat, holdSeatOn(seat, "hold-late"))

	rows := outboxRows(t, ctx, pool)
	const unavailable = "sagaflow.inventory.v1.SeatUnavailable"
	if last := rows[len(rows)-1]; last.ceTyp != unavailable {
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
