package inventory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AymanKastali/sagaflow/internal/inventory"
	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// jsonEncoder stands in for platform/schema.Serde. Registry framing is that
// package's property and is proven in its tests; what matters here is that the
// handler puts the encoder's bytes in the outbox row.
type jsonEncoder struct{}

func (jsonEncoder) Encode(m proto.Message) ([]byte, error) { return protojson.Marshal(m) }

// outboxRow is one message waiting to be published, read back from the table.
type outboxRow struct {
	topic string
	key   string
	ceTyp string
}

func outboxRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []outboxRow {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT topic, key, headers->>'ce_type' FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()

	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.topic, &r.key, &r.ceTyp); err != nil {
			t.Fatalf("scan outbox row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox: %v", err)
	}
	return out
}

func command(cmd proto.Message) envelope.Envelope {
	return envelope.Envelope{
		ID:            envelope.NewID(),
		Source:        "/sagaflow/booking",
		Type:          string(cmd.ProtoReflect().Descriptor().FullName()),
		Subject:       seat,
		CorrelationID: "saga-1",
	}
}

func TestHoldAppendsTheEventAndEnqueuesItInOneTransaction(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_hold")
	h := inventory.NewHandler(pool, jsonEncoder{})

	cmd := holdSeat(hold)
	if err := h.Handle(ctx, command(cmd), cmd); err != nil {
		t.Fatalf("handle: %v", err)
	}

	rows := outboxRows(t, ctx, pool)
	if len(rows) != 1 {
		t.Fatalf("want 1 outbox row, got %d: %+v", len(rows), rows)
	}
	if rows[0].topic != inventory.EventsTopic {
		t.Fatalf("want topic %q, got %q", inventory.EventsTopic, rows[0].topic)
	}
	if rows[0].key != seat {
		t.Fatalf("the key must be the stream id, which is what keeps a seat's "+
			"events in one partition; want %q, got %q", seat, rows[0].key)
	}
	if rows[0].ceTyp != "sagaflow.inventory.v1.SeatHeld" {
		t.Fatalf("want ce_type SeatHeld, got %q", rows[0].ceTyp)
	}
}

func TestARedeliveredCommandIsAppliedOnce(t *testing.T) {
	// Handle the same ce_id twice and state must advance only once: the second
	// delivery is exactly what the inbox exists to absorb.
	ctx := t.Context()
	pool := db(t, "inventory_handler_dedupe")
	h := inventory.NewHandler(pool, jsonEncoder{})

	cmd := holdSeat(hold)
	env := command(cmd)
	for range 2 {
		if err := h.Handle(ctx, env, cmd); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}

	var events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("the inbox must absorb the redelivery; got %d events", events)
	}
	if rows := outboxRows(t, ctx, pool); len(rows) != 1 {
		t.Fatalf("a deduplicated command must publish nothing the second time; got %d rows", len(rows))
	}
}

func TestARefusedHoldWritesNoEventAndStillReplies(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_refused")
	h := inventory.NewHandler(pool, jsonEncoder{})

	first := holdSeat(hold)
	if err := h.Handle(ctx, command(first), first); err != nil {
		t.Fatalf("first hold: %v", err)
	}
	second := holdSeat("hold-2")
	if err := h.Handle(ctx, command(second), second); err != nil {
		t.Fatalf("second hold: %v", err)
	}

	var events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("a refusal appends nothing, so the seat has 1 event; got %d", events)
	}

	rows := outboxRows(t, ctx, pool)
	if len(rows) != 2 {
		t.Fatalf("want SeatHeld then SeatUnavailable, got %d rows: %+v", len(rows), rows)
	}
	if rows[1].ceTyp != "sagaflow.inventory.v1.SeatUnavailable" {
		t.Fatalf("the refusal must reach the saga; got ce_type %q", rows[1].ceTyp)
	}
}

// TestTwoConcurrentHoldsProduceOneHoldAndOneRefusal is this package's central
// guarantee: two racing holds on the same seat produce one SeatHeld and one
// SeatUnavailable, never two holds. An HTTP layer would render the refusal as
// a 409, but there is no HTTP yet, so the assertion is on the refusal itself.
//
// Both goroutines fold from version 0 and both attempt version 1. The loser gets
// ErrVersionConflict, reloads, sees SeatHeld, and re-decides to a refusal — so
// this proves the retry re-decides rather than merely re-appending.
func TestTwoConcurrentHoldsProduceOneHoldAndOneRefusal(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_race")
	h := inventory.NewHandler(pool, jsonEncoder{})

	var (
		wg    sync.WaitGroup
		errs  [2]error
		start = make(chan struct{})
	)
	for i, holdID := range [2]string{"hold-a", "hold-b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := holdSeat(holdID)
			env := command(cmd)
			<-start // release both at once, so they really do race
			errs[i] = h.Handle(ctx, env, cmd)
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("hold %d failed instead of being decided: %v", i, err)
		}
	}

	var events int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM events WHERE stream_id = $1`, seat).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("a seat holds at most one live hold, so exactly one event may be "+
			"appended; got %d", events)
	}

	rows := outboxRows(t, ctx, pool)
	var held, unavailable int
	for _, r := range rows {
		switch r.ceTyp {
		case "sagaflow.inventory.v1.SeatHeld":
			held++
		case "sagaflow.inventory.v1.SeatUnavailable":
			unavailable++
		}
	}
	if held != 1 || unavailable != 1 {
		t.Fatalf("want exactly one SeatHeld and one SeatUnavailable, got %d and %d: %+v",
			held, unavailable, rows)
	}
}

func TestAnUnknownCommandIsRejected(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_unknown")
	h := inventory.NewHandler(pool, jsonEncoder{})

	// A SeatHeld is an event, not a command. Redelivery cannot change that.
	ev := heldEvent(hold)
	if err := h.Handle(ctx, command(ev), ev); !errors.Is(err, inventory.ErrUnknownCommand) {
		t.Fatalf("want ErrUnknownCommand, got %v", err)
	}
}

// TestAVersionConflictReloadsAndReDecides proves the retry deterministically,
// which the concurrent test above cannot: two goroutines might simply not
// overlap, and the loser would then refuse without ever seeing a conflict.
//
// An uncommitted SeatHeld at version 1 makes the handler's own insert block on
// the unique index until this transaction commits, so the conflict is arranged
// rather than hoped for.
func TestAVersionConflictReloadsAndReDecides(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_conflict")
	h := inventory.NewHandler(pool, jsonEncoder{})

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := inventory.AppendSeat(ctx, tx, seat, 0,
		[]proto.Message{heldEvent("hold-a")}, eventstore.Meta{}); err != nil {
		t.Fatalf("append the winner: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		cmd := holdSeat("hold-b")
		done <- h.Handle(ctx, command(cmd), cmd)
	}()

	waitForLock(t, ctx, pool) // the handler has reached its insert and is blocked
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit the winner: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("the loser must be decided, not failed: %v", err)
	}

	rows := outboxRows(t, ctx, pool)
	if len(rows) != 1 || rows[0].ceTyp != "sagaflow.inventory.v1.SeatUnavailable" {
		t.Fatalf("the loser must reload, re-decide, and refuse; got %+v", rows)
	}

	var events int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM events WHERE stream_id = $1`, seat).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("the retry must re-decide, not re-append: want 1 event, got %d", events)
	}
}

// waitForLock blocks until a backend in this database is waiting on a lock,
// which is how the test knows the handler reached its insert. A polled condition
// with a deadline as the failure mode, not a sleep before an assertion.
func waitForLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatalf("poll pg_stat_activity: %v", err)
		}
		if n > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("the handler never blocked on the uncommitted row")
		case <-tick.C:
		}
	}
}
