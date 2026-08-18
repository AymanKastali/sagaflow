package inventory_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kptac/sagaflow/internal/inventory"
	"github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/testsupport/pgtest"
	"google.golang.org/protobuf/proto"
)

func db(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	return pgtest.Shared(t).Migrated(t, name, migrations.FS)
}

func TestSeatStreamRoundTripsThroughPostgres(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_store")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return inventory.AppendSeat(ctx, tx, seat, 0,
			[]proto.Message{heldEvent(hold)}, eventstore.Meta{CorrelationID: "saga-1"})
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	var got inventory.SeatState
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		got, err = inventory.LoadSeat(ctx, tx, seat)
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}

	if got.Status != inventory.StatusHeld || got.HoldID != hold {
		t.Fatalf("state did not survive the round trip: %+v", got)
	}
	if got.Version != 1 {
		t.Fatalf("one event stored, so the expected version is 1, got %d", got.Version)
	}
	if !got.ExpiresAt.Equal(expires.AsTime()) {
		t.Fatalf("expires_at lost precision or zone: want %v, got %v",
			expires.AsTime(), got.ExpiresAt)
	}
}

func TestAppendAtAStaleVersionConflicts(t *testing.T) {
	// The atomic hold *is* this constraint (spec §6.3): two writers folding from
	// version 0 both try version 1 and Postgres rejects one. Asserted here so
	// Task 4's retry has something proven to retry on.
	ctx := t.Context()
	pool := db(t, "inventory_store_conflict")

	appendFrom := func(version int) error {
		return pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
			return inventory.AppendSeat(ctx, tx, seat, version,
				[]proto.Message{heldEvent(hold)}, eventstore.Meta{})
		})
	}

	if err := appendFrom(0); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := appendFrom(0); !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict on a stale expected version, got %v", err)
	}
}

func TestLoadOfAnEmptyStreamIsAFreeSeatAtVersionZero(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_store_empty")

	var got inventory.SeatState
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		got, err = inventory.LoadSeat(ctx, tx, "seat-never-touched")
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Status != inventory.StatusFree || got.Version != 0 {
		t.Fatalf("an unwritten seat is free at version 0, got %+v", got)
	}
}

func TestStoredEventIsReadableProtoJSON(t *testing.T) {
	// Spec §8.4: storage is protojson so a replay survives a registry outage and
	// psql shows something readable during an incident. Asserted against the
	// column rather than through the codec, which would prove only symmetry.
	ctx := t.Context()
	pool := db(t, "inventory_store_json")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return inventory.AppendSeat(ctx, tx, seat, 0,
			[]proto.Message{heldEvent(hold)}, eventstore.Meta{})
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	var typ, holdID string
	if err := pool.QueryRow(ctx,
		`SELECT type, data->>'hold_id' FROM events WHERE stream_id = $1`, seat,
	).Scan(&typ, &holdID); err != nil {
		t.Fatalf("read the row back: %v", err)
	}
	if typ != "sagaflow.inventory.v1.SeatHeld" {
		t.Fatalf("events.type must be the fully qualified name, got %q", typ)
	}
	if holdID != hold {
		t.Fatalf("data must use proto field names, so data->>'hold_id' is %q, got %q", hold, holdID)
	}
}
