package inventory_test

import (
	"context"
	"testing"
	"time"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/platform/timers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// tick runs one scheduler pass and returns how many timers fired.
func tick(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	n, err := timers.NewScheduler(pool, inventory.NewExpirer(jsonEncoder{})).Tick(t.Context())
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	return n
}

// seatState folds the seat's stream as it stands now.
func seatState(t *testing.T, pool *pgxpool.Pool, seatID string) inventory.SeatState {
	t.Helper()
	var s inventory.SeatState
	if err := pg.WithTx(t.Context(), pool, func(tx pgx.Tx) error {
		var err error
		s, _, err = inventory.LoadSeat(t.Context(), tx, seatID)
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	return s
}

// expiredHold is a HoldSeat whose deadline has already passed, so the very next
// scheduler pass finds it due. Controlling the deadline with a value rather than
// by waiting is what keeps these tests free of sleeps.
func expiredHold(holdID string) *inventoryv1.HoldSeat {
	cmd := holdSeat(holdID)
	cmd.ExpiresAt = timestamppb.New(time.Now().Add(-time.Second))
	return cmd
}

func TestADeadlineThatPassesFreesTheSeat(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_expiry")
	h := inventory.NewHandler(pool, jsonEncoder{})

	cmd := expiredHold(hold)
	if err := h.Handle(ctx, command(cmd), cmd); err != nil {
		t.Fatalf("hold: %v", err)
	}

	if n := tick(t, pool); n != 1 {
		t.Fatalf("one deadline has passed, so one timer fires: got %d", n)
	}

	got := seatState(t, pool, seat)
	if got.Status != inventory.StatusFree {
		t.Fatalf("the expired hold left the seat held: %+v", got)
	}
	if got.Version != 2 {
		t.Fatalf("SeatHeld then SeatHoldExpired is two events: got version %d", got.Version)
	}

	rows := outboxRows(t, ctx, pool)
	if len(rows) != 2 {
		t.Fatalf("the hold and the expiry are both announced: want 2 outbox rows, got %+v", rows)
	}
	if rows[1].ceTyp != "sagaflow.inventory.v1.SeatHoldExpired" {
		t.Fatalf("want ce_type SeatHoldExpired, got %q", rows[1].ceTyp)
	}
	if rows[1].key != seat {
		t.Fatalf("the expiry must be keyed by the same stream as the hold it undoes: %q", rows[1].key)
	}
}

func TestAnExpiryCarriesTheSagaThatTookTheHold(t *testing.T) {
	// Nothing sent this expiry, so there is no incoming envelope to copy a
	// correlation id from. The seat's own stream recorded which saga took the
	// hold, and that is the flow the expiry has to reach.
	ctx := t.Context()
	pool := db(t, "inventory_expiry_correlation")
	h := inventory.NewHandler(pool, jsonEncoder{})

	cmd := expiredHold(hold)
	incoming := command(cmd)
	if err := h.Handle(ctx, incoming, cmd); err != nil {
		t.Fatalf("hold: %v", err)
	}
	tick(t, pool)

	var correlation, causation *string
	if err := pool.QueryRow(ctx, `
		SELECT headers->>'ce_correlationid', headers->>'ce_causationid'
		FROM outbox
		WHERE headers->>'ce_type' = $1`,
		"sagaflow.inventory.v1.SeatHoldExpired").Scan(&correlation, &causation); err != nil {
		t.Fatalf("read the expiry's headers: %v", err)
	}
	if correlation == nil || *correlation != incoming.CorrelationID {
		t.Fatalf("the expiry must reach the saga that is waiting on the seat: want %q, got %v",
			incoming.CorrelationID, correlation)
	}
	if causation != nil {
		t.Fatalf("no message caused this, so the chain ends here, got %q", *causation)
	}
}

func TestAStaleTimerLeavesTheNewHoldAlone(t *testing.T) {
	// hold-1's deadline passes after hold-1 was released and the seat retaken.
	// Without the token fence this frees a seat someone is actively holding.
	ctx := t.Context()
	pool := db(t, "inventory_expiry_stale")
	h := inventory.NewHandler(pool, jsonEncoder{})

	first := expiredHold("hold-1")
	if err := h.Handle(ctx, command(first), first); err != nil {
		t.Fatalf("first hold: %v", err)
	}
	release := &inventoryv1.ReleaseSeatHold{
		HoldId: "hold-1", BookingId: booking, SeatId: seat, Reason: "compensating",
	}
	if err := h.Handle(ctx, command(release), release); err != nil {
		t.Fatalf("release: %v", err)
	}
	second := holdSeat("hold-2") // deadline far in the future
	if err := h.Handle(ctx, command(second), second); err != nil {
		t.Fatalf("second hold: %v", err)
	}

	if n := tick(t, pool); n != 1 {
		t.Fatalf("the stale timer is due and is claimed: got %d", n)
	}

	got := seatState(t, pool, seat)
	if got.Status != inventory.StatusHeld || got.HoldID != "hold-2" {
		t.Fatalf("a stale timer disturbed the live hold: %+v", got)
	}
	if got.Version != 3 {
		t.Fatalf("held, released, held again — and the stale fire appended nothing: got version %d",
			got.Version)
	}
}

func TestATimerThatFiredIsNotDueAgain(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_expiry_once")
	h := inventory.NewHandler(pool, jsonEncoder{})

	cmd := expiredHold(hold)
	if err := h.Handle(ctx, command(cmd), cmd); err != nil {
		t.Fatalf("hold: %v", err)
	}

	if n := tick(t, pool); n != 1 {
		t.Fatalf("first pass fires the timer: got %d", n)
	}
	if n := tick(t, pool); n != 0 {
		t.Fatalf("a fired timer is done: second pass fired %d", n)
	}

	if got := seatState(t, pool, seat); got.Version != 2 {
		t.Fatalf("the second pass appended a second expiry: version %d", got.Version)
	}
}

func TestASeatFreesItselfWhileNothingElseIsRunning(t *testing.T) {
	// The claim of this phase, stated as a test. A hold is taken and then
	// nothing further happens — no release arrives, no consumer runs, no saga
	// exists. The scheduler is the only thing still moving, and the seat comes
	// back on its own.
	ctx := t.Context()
	pool := db(t, "inventory_self_healing")

	cmd := holdSeat(hold)
	cmd.ExpiresAt = timestamppb.New(time.Now().Add(150 * time.Millisecond))
	if err := inventory.NewHandler(pool, jsonEncoder{}).
		Handle(ctx, command(cmd), cmd); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if got := seatState(t, pool, seat); got.Status != inventory.StatusHeld {
		t.Fatalf("setup: the seat should be held, got %+v", got)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	sched := timers.NewScheduler(pool, inventory.NewExpirer(jsonEncoder{}))
	go func() { _ = sched.Run(runCtx) }()

	// Poll the effect rather than wait a duration: the assertion is bounded by
	// t.Context's deadline, so a slow machine is slow rather than flaky.
	for {
		if seatState(t, pool, seat).Status == inventory.StatusFree {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("the deadline passed and the seat is still held")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
