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
// It is a copy of SeatState with the seat's own id attached rather than SeatState
// itself, because the two answer different questions. SeatState is what a
// decision is taken from and is always current; this is what a customer is shown
// and is allowed to be old.
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
// absorb — and no ordering guarantee either, because two notifications arriving
// in the wrong order both end at the current state.
//
// The price is that it only works where the view and the streams share a
// database. A projection living somewhere else could not re-read anything and
// would have to apply what the message carried, sequence numbers and all.
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
// All of it in one transaction, so a reader sees either the old view or the new
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
// caller. Two of them can overlap — a rebuild and a live notification, or two
// notifications for one seat — and the one that read the older stream does
// nothing rather than dragging the row back to a state that is no longer true.
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
// flightPrefix is matched against the start of the stream id — a seat id begins
// with the flight and date it belongs to, so the prefix is the flight.
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
